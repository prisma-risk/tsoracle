//
//  ░▀█▀░█▀▀░█▀█░█▀▄░█▀█░█▀▀░█░░░█▀▀
//  ░░█░░▀▀█░█░█░█▀▄░█▀█░█░░░█░░░█▀▀
//  ░░▀░░▀▀▀░▀▀▀░▀░▀░▀░▀░▀▀▀░▀▀▀░▀▀▀
//
//  tsoracle — Distributed Timestamp Oracle
//
//  Copyright (c) 2026 Prisma Risk
//  Licensed under the Apache License, Version 2.0
//  https://github.com/prisma-risk/tsoracle
//

//! OmniPaxos lifecycle: tick task, outbound drain, leader-event emission.

pub mod events;
pub mod state;

pub use events::{LeaderEventSender, LeaderEventStream, SendError, leader_event_channel};
pub use state::{LeadershipState, Peer};

use std::sync::Arc;
use std::time::Duration;

use omnipaxos::OmniPaxos;
use omnipaxos::messages::Message;
use omnipaxos::storage::{Entry, Storage};
use parking_lot::Mutex;
use tokio::sync::{Notify, oneshot};
use tokio::task::JoinHandle;
use tokio::time::interval;
use tracing::{debug, error, warn};
use tsoracle_core::Epoch;

/// Outbound message dispatch contract supplied by the caller.
///
/// The toolkit owns the OmniPaxos tick + outbound drain but knows nothing
/// about wire transport; the embedding application (examples, the future
/// driver crate) implements this trait to route messages to peers over
/// whatever transport it has chosen (typically tonic / gRPC).
#[async_trait::async_trait]
pub trait MessageSink<T: Entry>: Send + Sync + 'static {
    async fn send(&self, message: Message<T>);
}

/// Owner of the OmniPaxos tick task.
///
/// On `start`, spawns a tokio task that periodically calls
/// `OmniPaxos::tick`, drains outbound messages through the supplied
/// [`MessageSink`], observes leadership, emits transitions through
/// the leader-event channel, and notifies a shared [`Notify`] so an
/// external apply task can drain decided entries without polling.
pub struct PaxosRunner<T, S>
where
    T: Entry + Send + 'static,
    S: Storage<T> + Send + 'static,
{
    omnipaxos: Arc<Mutex<OmniPaxos<T, S>>>,
    my_node_id: u64,
    peers: Vec<Peer>,
    tick_interval: Duration,
    leader_sender: LeaderEventSender,
    leader_stream: Option<LeaderEventStream>,
    apply_notify: Arc<Notify>,
    handle: Option<JoinHandle<()>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl<T, S> PaxosRunner<T, S>
where
    T: Entry + Send + 'static,
    S: Storage<T> + Send + 'static,
{
    /// Build a runner around a pre-constructed `OmniPaxos` handle.
    ///
    /// `peers` is the topology hint used to resolve follower-redirect
    /// endpoints when leadership lands on another node. `tick_interval`
    /// controls how often `OmniPaxos::tick` is invoked.
    pub fn new(
        omnipaxos: Arc<Mutex<OmniPaxos<T, S>>>,
        my_node_id: u64,
        peers: Vec<Peer>,
        tick_interval: Duration,
    ) -> Self {
        let (leader_sender, leader_stream) = leader_event_channel();
        Self {
            omnipaxos,
            my_node_id,
            peers,
            tick_interval,
            leader_sender,
            leader_stream: Some(leader_stream),
            apply_notify: Arc::new(Notify::new()),
            handle: None,
            shutdown_tx: None,
        }
    }

    /// Take ownership of the leader-event stream. Returns `None` if already taken.
    #[must_use]
    pub fn take_leader_stream(&mut self) -> Option<LeaderEventStream> {
        self.leader_stream.take()
    }

    /// Notification fired once per tick, after outbound messages have been
    /// drained. External apply tasks await this so they can drain decided
    /// entries opportunistically rather than polling.
    ///
    /// Semantics (matches `tokio::sync::Notify::notify_waiters`):
    /// - **Edge-triggered:** a waiter that is not parked at the `Notify` at
    ///   the moment the tick task fires will miss that tick's notification
    ///   and catch the next one.
    /// - **All waiters wake:** every task currently parked on this `Notify`
    ///   wakes simultaneously. There is no permit accumulation; a wake that
    ///   has no waiters is dropped on the floor.
    /// - **Consequence:** apply tasks should loop and always re-park, never
    ///   assume one wake corresponds to one decided entry.
    #[must_use]
    pub fn apply_notify(&self) -> Arc<Notify> {
        self.apply_notify.clone()
    }

    /// Borrow the underlying `OmniPaxos` handle for direct interaction
    /// (e.g., to `append` an entry from outside the tick loop).
    #[must_use]
    pub fn omnipaxos(&self) -> Arc<Mutex<OmniPaxos<T, S>>> {
        self.omnipaxos.clone()
    }

    /// Spawn the tick task with `sink` as the outbound transport.
    ///
    /// # Preconditions
    ///
    /// Must not be called while the runner is already running. Call
    /// [`Self::stop`] first to restart. Debug builds assert this; release
    /// builds would leave the previous task orphaned (it exits cleanly
    /// once its shutdown channel is dropped, but two tick tasks briefly
    /// race during the overlap).
    pub fn start<Sink: MessageSink<T>>(&mut self, sink: Arc<Sink>)
    where
        <T as Entry>::Snapshot: Send,
    {
        debug_assert!(
            self.handle.is_none(),
            "PaxosRunner::start called while already running; call stop() first",
        );

        let omnipaxos = self.omnipaxos.clone();
        let my_node_id = self.my_node_id;
        let peers = self.peers.clone();
        let tick_interval = self.tick_interval;
        let leader_sender = self.leader_sender.clone();
        let apply_notify = self.apply_notify.clone();
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        self.shutdown_tx = Some(shutdown_tx);

        let handle = tokio::spawn(async move {
            let mut ticker = interval(tick_interval);
            // Locally-tracked leader observation + monotonic counter for the
            // epoch placeholder (see the runner module's doc).
            let mut last_observed_leader: Option<u64> = None;
            let mut leader_change_counter: u64 = 0;

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        // 1. Tick + drain in a short critical section, then
                        //    drop the guard before any await.
                        let outgoing = {
                            let mut op = omnipaxos.lock();
                            op.tick();
                            op.outgoing_messages()
                        };

                        // 2. Send outbound messages with the lock released.
                        for message in outgoing {
                            sink.send(message).await;
                        }

                        // 3. Observe leadership.
                        //
                        //    KNOWN LIMITATION: the counter-derived epoch
                        //    does NOT match the spec's fencing strategy in
                        //    persist_high_water(at_least, epoch), which
                        //    compares epoch == encode_epoch(promise). A
                        //    leader that passes its own epoch to persist
                        //    would fail the fence check. The follow-up
                        //    driver crate replaces this stream with one
                        //    that derives epoch from
                        //    omnipaxos.get_promise() (read via the local
                        //    storage handle), so the value matches what
                        //    the fence expects.
                        let leader_pid: Option<u64> = {
                            let op = omnipaxos.lock();
                            op.get_current_leader()
                        };
                        if leader_pid != last_observed_leader {
                            last_observed_leader = leader_pid;
                            if leader_pid.is_some() {
                                leader_change_counter = leader_change_counter.wrapping_add(1);
                            }
                        }
                        let epoch = leader_pid.map(|_| Epoch(leader_change_counter));
                        let state = LeadershipState::from_omnipaxos(
                            my_node_id, leader_pid, epoch, &peers,
                        );
                        if let Err(err) = leader_sender.send(state.to_consensus()) {
                            warn!(error = %err, "leader event channel closed");
                            break;
                        }

                        // 4. Wake the apply task in case decided_idx advanced.
                        apply_notify.notify_waiters();
                    }
                    _ = &mut shutdown_rx => {
                        debug!("paxos runner received shutdown");
                        break;
                    }
                }
            }
        });
        self.handle = Some(handle);
    }

    /// Signal shutdown and await the tick task.
    ///
    /// Surfaces a `tracing::error!` if the task terminated abnormally
    /// (panic or cancellation). Otherwise silent.
    pub async fn stop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.handle.take() {
            if let Err(err) = handle.await {
                error!(error = ?err, "paxos runner task terminated abnormally");
            }
        }
    }
}

impl<T, S> Drop for PaxosRunner<T, S>
where
    T: Entry + Send + 'static,
    S: Storage<T> + Send + 'static,
{
    /// Best-effort shutdown signal on drop.
    ///
    /// Sends the shutdown one-shot if present, but does NOT await the
    /// task — that would require an async context. The detached task
    /// observes the dropped receiver and exits cleanly. Callers that
    /// need synchronous completion should invoke `stop().await` first.
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use omnipaxos::ballot_leader_election::Ballot;
    use omnipaxos::storage::{Snapshot, StopSign, StorageResult};
    use omnipaxos::{ClusterConfig, OmniPaxosConfig, ServerConfig};
    use tokio::time::sleep;
    use tsoracle_consensus::LeaderState;

    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    struct TestEntry;

    impl Entry for TestEntry {
        type Snapshot = TestSnapshot;
    }

    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    struct TestSnapshot;

    impl Snapshot<TestEntry> for TestSnapshot {
        fn create(_: &[TestEntry]) -> Self {
            Self
        }
        fn merge(&mut self, _: Self) {}
        fn use_snapshots() -> bool {
            false
        }
    }

    /// Minimal in-memory `Storage<TestEntry>` for runner-loop coverage tests.
    ///
    /// We do not depend on `omnipaxos_storage::MemoryStorage` because it
    /// pulls `sled` transitively, which brings unmaintained `bincode`,
    /// `fxhash`, and `instant` advisories that `cargo deny` rejects.
    /// A richer `MemStorage` lands in a follow-up sub-issue under
    /// `test_fakes/`; this stub is only enough for OmniPaxos to call
    /// the tick path without panicking on an empty log.
    #[derive(Default)]
    struct StubStorage {
        promise: Option<Ballot>,
        accepted_round: Option<Ballot>,
        decided_idx: u64,
        compacted_idx: u64,
        snapshot: Option<TestSnapshot>,
        stopsign: Option<StopSign>,
    }

    impl Storage<TestEntry> for StubStorage {
        fn append_entry(&mut self, _: TestEntry) -> StorageResult<u64> {
            Ok(0)
        }
        fn append_entries(&mut self, _: Vec<TestEntry>) -> StorageResult<u64> {
            Ok(0)
        }
        fn append_on_prefix(&mut self, _: u64, _: Vec<TestEntry>) -> StorageResult<u64> {
            Ok(0)
        }
        fn get_entries(&self, _: u64, _: u64) -> StorageResult<Vec<TestEntry>> {
            Ok(Vec::new())
        }
        fn get_log_len(&self) -> StorageResult<u64> {
            Ok(0)
        }
        fn get_suffix(&self, _: u64) -> StorageResult<Vec<TestEntry>> {
            Ok(Vec::new())
        }
        fn set_promise(&mut self, ballot: Ballot) -> StorageResult<()> {
            self.promise = Some(ballot);
            Ok(())
        }
        fn get_promise(&self) -> StorageResult<Option<Ballot>> {
            Ok(self.promise)
        }
        fn set_accepted_round(&mut self, ballot: Ballot) -> StorageResult<()> {
            self.accepted_round = Some(ballot);
            Ok(())
        }
        fn get_accepted_round(&self) -> StorageResult<Option<Ballot>> {
            Ok(self.accepted_round)
        }
        fn set_decided_idx(&mut self, idx: u64) -> StorageResult<()> {
            self.decided_idx = idx;
            Ok(())
        }
        fn get_decided_idx(&self) -> StorageResult<u64> {
            Ok(self.decided_idx)
        }
        fn trim(&mut self, _: u64) -> StorageResult<()> {
            Ok(())
        }
        fn set_compacted_idx(&mut self, idx: u64) -> StorageResult<()> {
            self.compacted_idx = idx;
            Ok(())
        }
        fn get_compacted_idx(&self) -> StorageResult<u64> {
            Ok(self.compacted_idx)
        }
        fn set_snapshot(&mut self, snapshot: Option<TestSnapshot>) -> StorageResult<()> {
            self.snapshot = snapshot;
            Ok(())
        }
        fn get_snapshot(&self) -> StorageResult<Option<TestSnapshot>> {
            Ok(self.snapshot.clone())
        }
        fn set_stopsign(&mut self, stopsign: Option<StopSign>) -> StorageResult<()> {
            self.stopsign = stopsign;
            Ok(())
        }
        fn get_stopsign(&self) -> StorageResult<Option<StopSign>> {
            Ok(self.stopsign.clone())
        }
    }

    struct NoopSink;

    #[async_trait::async_trait]
    impl MessageSink<TestEntry> for NoopSink {
        async fn send(&self, _message: Message<TestEntry>) {}
    }

    fn build_omnipaxos(node_id: u64) -> Arc<Mutex<OmniPaxos<TestEntry, StubStorage>>> {
        // OmniPaxos 0.2 rejects single-node ClusterConfigs, so build a 3-node
        // configuration even when only one runner will exist.
        let cluster_config = ClusterConfig {
            configuration_id: 1,
            nodes: vec![1, 2, 3],
            flexible_quorum: None,
        };
        let server_config = ServerConfig {
            pid: node_id,
            ..Default::default()
        };
        let op_config = OmniPaxosConfig {
            cluster_config,
            server_config,
        };
        let op = op_config
            .build(StubStorage::default())
            .expect("build omnipaxos");
        Arc::new(Mutex::new(op))
    }

    fn build_runner(node_id: u64) -> PaxosRunner<TestEntry, StubStorage> {
        PaxosRunner::new(
            build_omnipaxos(node_id),
            node_id,
            vec![],
            Duration::from_millis(5),
        )
    }

    #[tokio::test]
    async fn take_leader_stream_is_once_only() {
        let mut runner = build_runner(1);
        assert!(runner.take_leader_stream().is_some());
        assert!(runner.take_leader_stream().is_none());
    }

    #[tokio::test]
    async fn omnipaxos_handle_is_shared() {
        let omnipaxos = build_omnipaxos(1);
        let runner = PaxosRunner::new(omnipaxos.clone(), 1, vec![], Duration::from_millis(5));
        assert!(Arc::ptr_eq(&omnipaxos, &runner.omnipaxos()));
    }

    #[tokio::test]
    async fn apply_notify_handle_is_shared() {
        let runner = build_runner(1);
        let first = runner.apply_notify();
        let second = runner.apply_notify();
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[tokio::test]
    async fn stop_without_start_is_noop() {
        let mut runner = build_runner(1);
        runner.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn runner_ticks_emit_unknown_state_under_dead_network() {
        // Boot a runner with a no-op MessageSink. Without inbound messages
        // from the (imaginary) peer nodes, no consensus is reached and
        // get_current_leader() returns None. The tick task still runs:
        // it ticks, drains outbound (the messages go nowhere via NoopSink),
        // observes leader = None, emits LeaderState::Unknown via the
        // leader-event channel, and calls notify_waiters().
        let mut runner = build_runner(1);
        let mut stream = runner.take_leader_stream().expect("stream").into_pin();
        runner.start(Arc::new(NoopSink));

        // First yielded value is the initial Unknown.
        assert_eq!(stream.next().await, Some(LeaderState::Unknown));

        // Let several ticks fire. They all emit Unknown (same as initial),
        // which the debounce arm absorbs. We don't assert on a second
        // stream value because debounce intentionally suppresses repeats.
        sleep(Duration::from_millis(30)).await;

        // Stop the tick task. The runner struct (and its leader_sender)
        // outlives this call — they drop at the end of the function scope.
        // We do NOT drain the stream here because `stream.next().await`
        // would block until the sender drops.
        runner.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn apply_notify_fires_on_each_tick() {
        // Park a waiter on apply_notify before the runner starts ticking;
        // the next tick should wake it. This covers the notify_waiters()
        // call site in the tick task body.
        let mut runner = build_runner(1);
        let notify = runner.apply_notify();
        runner.start(Arc::new(NoopSink));

        let woke = tokio::time::timeout(Duration::from_millis(50), notify.notified()).await;
        assert!(
            woke.is_ok(),
            "apply_notify should fire within 50ms of starting"
        );

        runner.stop().await;
    }
}
