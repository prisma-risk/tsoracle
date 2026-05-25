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

pub use events::{
    LeaderEventSender, LeaderEventStream, LeaderEventSubscriber, SendError, leader_event_channel,
};
pub use state::LeadershipState;
pub use tsoracle_core::TsoPeer;

use std::sync::Arc;
use std::time::Duration;

use omnipaxos::OmniPaxos;
use omnipaxos::messages::Message;
use omnipaxos::storage::{Entry, Storage};
use parking_lot::Mutex;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::interval;
use tracing::{debug, error, warn};
use tsoracle_core::Epoch;

/// Bound on the outbound message queue feeding the dedicated sender task.
///
/// Outbound delivery is fire-and-forget by OmniPaxos's design — every tick
/// regenerates the messages a node still needs to send, so a message dropped
/// here is retransmitted on the next tick. The queue exists only to decouple
/// tick-loop progress from per-send latency; when it fills (a slow or
/// blackholed peer), the tick loop drops rather than blocks. The capacity is
/// therefore a memory/throughput knob, not a correctness one.
const OUTBOUND_QUEUE_CAPACITY: usize = 1024;

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
    peers: Vec<TsoPeer>,
    tick_interval: Duration,
    leader_sender: LeaderEventSender,
    leader_subscriber: Option<LeaderEventSubscriber>,
    apply_notify: Arc<Notify>,
    handle: Option<JoinHandle<()>>,
    sender_handle: Option<JoinHandle<()>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    /// Leader-observation state for the synchronous [`Self::tick_once`] stepping
    /// path: `(last_observed_leader, leader_change_counter)`. The async
    /// [`Self::start`] loop keeps its own task-local copy; a runner is driven by
    /// exactly one of the two paths, so they never interleave.
    step_leader: Mutex<(Option<u64>, u64)>,
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
        peers: Vec<TsoPeer>,
        tick_interval: Duration,
    ) -> Self {
        let (leader_sender, leader_subscriber) = leader_event_channel();
        Self {
            omnipaxos,
            my_node_id,
            peers,
            tick_interval,
            leader_sender,
            leader_subscriber: Some(leader_subscriber),
            apply_notify: Arc::new(Notify::new()),
            handle: None,
            sender_handle: None,
            shutdown_tx: None,
            step_leader: Mutex::new((None, 0)),
        }
    }

    /// Take ownership of the leader-event subscriber. Returns `None` if already
    /// taken.
    ///
    /// The runner keeps only the [`LeaderEventSender`] after this hand-off, not
    /// a receiver, so the consumer that holds the returned subscriber is the
    /// sole owner of the channel's receiver side. Dropping it (and every stream
    /// it minted) closes the channel, which is the runner's drop-based shutdown
    /// signal. The subscriber itself is re-subscribable, so a consumer can mint
    /// fresh streams without coming back to the runner.
    #[must_use]
    pub fn take_leader_subscriber(&mut self) -> Option<LeaderEventSubscriber> {
        self.leader_subscriber.take()
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

        // Dedicated outbound sender: owns the sink and drains the queue
        // serially. Isolating per-send latency here is what keeps the tick
        // loop's cadence — and thus leadership observation, leader-event
        // emission, apply notification, and shutdown — independent of how
        // long any individual send takes. A send that never resolves wedges
        // only this task; `stop` aborts it as the backstop.
        let (outbound_tx, mut outbound_rx) = mpsc::channel::<Message<T>>(OUTBOUND_QUEUE_CAPACITY);
        let sender_handle = tokio::spawn(async move {
            while let Some(message) = outbound_rx.recv().await {
                sink.send(message).await;
            }
        });
        self.sender_handle = Some(sender_handle);

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

                        // 2. Hand outbound messages to the sender task without
                        //    awaiting delivery. `try_send` never blocks, so a
                        //    slow or wedged sink cannot stall this loop. A full
                        //    queue means the sink is behind; we drop the message
                        //    because OmniPaxos regenerates it next tick.
                        for message in outgoing {
                            match outbound_tx.try_send(message) {
                                Ok(()) => {}
                                Err(mpsc::error::TrySendError::Full(_)) => {
                                    debug!(
                                        "paxos outbound queue full; dropping message \
                                         (resent next tick)"
                                    );
                                }
                                Err(mpsc::error::TrySendError::Closed(_)) => {
                                    warn!("paxos outbound sender task gone; stopping tick loop");
                                    break;
                                }
                            }
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
                        let epoch = leader_pid.map(|_| Epoch(u128::from(leader_change_counter)));
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

    /// Synchronous single tick for deterministic test stepping.
    ///
    /// Ticks OmniPaxos once, emits the resulting leader transition on the
    /// leader-event stream, and returns the outbound messages for the caller to
    /// route (rather than the async sink path that [`Self::start`] uses).
    /// Mutually exclusive with [`Self::start`] — drive a runner by exactly one
    /// of the two. Intended for in-process deterministic-simulation tests.
    pub fn tick_once(&self) -> Vec<Message<T>> {
        let outgoing = {
            let mut op = self.omnipaxos.lock();
            op.tick();
            op.outgoing_messages()
        };
        let leader_pid: Option<u64> = self.omnipaxos.lock().get_current_leader();
        let mut observed = self.step_leader.lock();
        if leader_pid != observed.0 {
            observed.0 = leader_pid;
            if leader_pid.is_some() {
                observed.1 = observed.1.wrapping_add(1);
            }
        }
        let epoch = leader_pid.map(|_| Epoch(u128::from(observed.1)));
        let state =
            LeadershipState::from_omnipaxos(self.my_node_id, leader_pid, epoch, &self.peers);
        let _ = self.leader_sender.send(state.to_consensus());
        outgoing
    }

    /// Deliver an inbound message synchronously (deterministic test stepping).
    /// Mutually exclusive with [`Self::start`]'s pump path.
    pub fn handle_incoming(&self, message: Message<T>) {
        self.omnipaxos.lock().handle_incoming(message);
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
        // The tick task drops `outbound_tx` as it exits, closing the queue so
        // the sender task finishes once it has drained — unless it is wedged
        // on a send that never resolves. A hung send has no cancellation point
        // of its own, so awaiting the sender unconditionally would reintroduce
        // the very deadlock this design removes; abort is the backstop.
        if let Some(sender) = self.sender_handle.take() {
            sender.abort();
            let _ = sender.await;
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
        // Cannot await in Drop, so abort the sender task outright rather than
        // wait for the closed queue to unwind it — guards against leaking a
        // task wedged on a never-resolving send.
        if let Some(sender) = self.sender_handle.take() {
            sender.abort();
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

    /// A sink whose `send` future never resolves, modelling a blackholed
    /// peer (firewall rule, dropped keepalives) reached over a transport
    /// with no per-request timeout. `entered` records how many sends were
    /// started so tests can confirm the hang path was actually exercised.
    #[derive(Default)]
    struct BlockingSink {
        entered: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl MessageSink<TestEntry> for BlockingSink {
        async fn send(&self, _message: Message<TestEntry>) {
            self.entered
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            std::future::pending::<()>().await;
        }
    }

    /// Poll `cond` until it returns true or the deadline elapses.
    async fn wait_until(deadline: Duration, cond: impl Fn() -> bool) -> bool {
        let start = std::time::Instant::now();
        while start.elapsed() < deadline {
            if cond() {
                return true;
            }
            sleep(Duration::from_millis(2)).await;
        }
        cond()
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
    async fn take_leader_subscriber_is_once_only() {
        let mut runner = build_runner(1);
        assert!(runner.take_leader_subscriber().is_some());
        assert!(runner.take_leader_subscriber().is_none());
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
        let mut stream = runner
            .take_leader_subscriber()
            .expect("subscriber")
            .subscribe()
            .into_pin();
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn hung_sink_does_not_starve_apply_notify() {
        // A sink that never resolves must not prevent the tick loop from
        // making progress: ticks, leadership observation, and apply_notify
        // are all downstream of the outbound drain, so coupling them to
        // per-send completion starves the apply task on a blackholed peer.
        let mut runner = build_runner(1);
        let sink = Arc::new(BlockingSink::default());
        let entered = sink.entered.clone();
        let notify = runner.apply_notify();
        runner.start(sink);

        // Confirm the hang path is actually exercised (BLE emits heartbeats
        // to peers 2 and 3 every tick, so a send is attempted promptly).
        assert!(
            wait_until(Duration::from_millis(200), || entered
                .load(std::sync::atomic::Ordering::SeqCst)
                >= 1)
            .await,
            "expected at least one send to be attempted",
        );

        let woke = tokio::time::timeout(Duration::from_millis(200), notify.notified()).await;
        assert!(
            woke.is_ok(),
            "apply_notify must fire even while every send is wedged",
        );

        runner.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn hung_sink_does_not_deadlock_stop() {
        // SIGTERM -> host.stop().await must return even when an outbound
        // send is wedged forever. The original serial-await-in-select shape
        // could not observe the shutdown branch while suspended mid-send, so
        // stop() blocked on the JoinHandle indefinitely.
        let mut runner = build_runner(1);
        let sink = Arc::new(BlockingSink::default());
        let entered = sink.entered.clone();
        runner.start(sink);

        assert!(
            wait_until(Duration::from_millis(200), || entered
                .load(std::sync::atomic::Ordering::SeqCst)
                >= 1)
            .await,
            "expected at least one send to be attempted before stop",
        );

        let stopped = tokio::time::timeout(Duration::from_secs(1), runner.stop()).await;
        assert!(
            stopped.is_ok(),
            "stop() must complete even when a send is wedged",
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tick_loop_exits_when_leader_subscriber_dropped() {
        // The tick loop emits every leadership transition through the
        // leader-event channel. Once every receiver is gone,
        // `LeaderEventSender::send` returns `SendError::Closed`, and the loop
        // must exit on its own rather than spin forever emitting into a closed
        // channel. Take the subscriber and drop it before starting (it has
        // minted no stream, so it is the sole receiver), so the first tick's
        // leader-event send observes the closed channel and breaks — without
        // anyone firing the shutdown signal. This is the only path that ends
        // the loop here, so the task finishing before we call `stop()` is proof
        // the closed-channel arm ran.
        let mut runner = build_runner(1);
        let subscriber = runner.take_leader_subscriber().expect("subscriber");
        drop(subscriber);
        runner.start(Arc::new(NoopSink));

        let exited = wait_until(Duration::from_secs(1), || {
            runner
                .handle
                .as_ref()
                .map(JoinHandle::is_finished)
                .unwrap_or(false)
        })
        .await;
        assert!(
            exited,
            "tick loop must exit once the leader-event subscriber is dropped",
        );

        // `stop()` remains safe to call after the task has already exited.
        runner.stop().await;
    }
}
