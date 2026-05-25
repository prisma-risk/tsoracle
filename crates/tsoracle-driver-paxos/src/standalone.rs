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

//! Standalone host: owns an OmniPaxos cluster handle, the toolkit's
//! `PaxosRunner`, the spawned apply task, and the in-memory high-water
//! state. Implements [`PaxosHighWaterHost`] so the driver can drive it
//! directly.
//!
//! Use this host when the embedding service does not already run an
//! OmniPaxos cluster for other state. Services that do should implement
//! `PaxosHighWaterHost` directly against their existing cluster instead.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use omnipaxos::OmniPaxos;
use omnipaxos::messages::Message;
use omnipaxos::storage::Storage;
use parking_lot::Mutex;
use tsoracle_consensus::{AdvancePayload, ConsensusError};
use tsoracle_paxos_toolkit::lifecycle::{LeaderEventSubscriber, MessageSink, PaxosRunner, TsoPeer};

use crate::apply::{ApplyEngine, ApplyTask};
use crate::host::PaxosHighWaterHost;
use crate::log_entry::HighWaterCommand;
use crate::snapshot_policy::SnapshotPolicy;

/// Standalone host owning its own paxos cluster + apply pipeline.
pub struct StandaloneHost<S>
where
    S: Storage<HighWaterCommand> + Send + 'static,
{
    omnipaxos: Arc<Mutex<OmniPaxos<HighWaterCommand, S>>>,
    my_node_id: u64,
    barrier_seq: AtomicU64,
    runner: PaxosRunner<HighWaterCommand, S>,
    leader_subscriber: Mutex<Option<LeaderEventSubscriber>>,
    /// Apply state + snapshot policy + the drain/snapshot step. Drives the
    /// synchronous stepping path and the barrier-linearized reads directly,
    /// and the async apply path via a clone moved into the spawned task.
    engine: ApplyEngine,
    /// The running apply task, or `None` when no task is live. `start`
    /// installs one; `stop` takes and consumes it. A single `Option` makes
    /// "running" structural — the apply task cannot be half-installed — and
    /// needs no `Mutex` because `start`/`stop` take `&mut self`. `None`
    /// covers stop-before-start, double-stop, and stop-after-exit.
    task: Option<ApplyTask>,
    /// Decided-log cursor for the apply path, seeded once in [`Self::new`] past
    /// any recovered suffix so the first drain after construction does not
    /// re-read the entries the constructor already folded.
    ///
    /// Shared (cloned `Arc`) by both drive paths: the synchronous
    /// [`Self::step`] / [`Self::apply_once`] stepping path locks it directly,
    /// and [`Self::start`]'s spawned apply task ([`ApplyEngine::spawn`]) clones
    /// it and locks it per drain. A host is driven by exactly one path, so the
    /// lock is uncontended; making the seed a single shared value is what keeps
    /// the async task from re-draining the whole decided log from index 0 on
    /// every startup (the two paths can no longer disagree on where recovery
    /// left off).
    apply_cursor: Arc<Mutex<u64>>,
    /// Upper bound on how long a barrier-linearized read/advance
    /// ([`Self::current_high_water`], [`Self::submit_advance`]) waits for its
    /// barrier to be folded before giving up with a retryable
    /// [`ConsensusError::TransientDriver`]. A backstop against an
    /// indefinite park when the barrier never decides (quorum loss, lost
    /// leadership) — not a tight SLA. Apply-task death is surfaced faster than
    /// this via the engine's liveness signal.
    barrier_timeout: Duration,
}

/// Default barrier-wait deadline. Generous relative to the tick cadence
/// (consensus normally folds a barrier in a handful of ticks), so it fires
/// only when the barrier genuinely cannot make progress.
pub const DEFAULT_BARRIER_TIMEOUT: Duration = Duration::from_secs(5);

impl<S> StandaloneHost<S>
where
    S: Storage<HighWaterCommand> + Send + 'static,
    <HighWaterCommand as omnipaxos::storage::Entry>::Snapshot: Send,
{
    /// Build a host from a pre-constructed OmniPaxos handle.
    ///
    /// Prefer [`StandaloneHost::builder`] for new code; this constructor
    /// is the lower-level entry point. Caller owns the OmniPaxos handle
    /// and the storage instance.
    pub fn new(
        omnipaxos: Arc<Mutex<OmniPaxos<HighWaterCommand, S>>>,
        my_node_id: u64,
        peers: Vec<TsoPeer>,
        tick_interval: Duration,
        policy: SnapshotPolicy,
        barrier_timeout: Duration,
    ) -> Self {
        let mut runner = PaxosRunner::new(omnipaxos.clone(), my_node_id, peers, tick_interval);
        let leader_subscriber = runner.take_leader_subscriber();

        // Resume the barrier-nonce counter above any seq this node already used
        // in a prior process lifetime. `barrier_seq` is process-local and
        // resets to 0 on restart; `current_high_water` waits for
        // `applied_barrier_seq(self) >= minted_seq`, so minting from 0 would
        // hand back a seq a recovered `(self, old_seq)` entry already satisfies,
        // letting the read return before its own barrier is applied and seeding
        // the new leader's allocator below the prior ceiling.
        //
        // The seed is the highest `(self, seq)` recoverable from the *durable
        // log* — see [`max_logged_barrier_seq`]. It deliberately does NOT come
        // from the recovered decided fold (`applied_barrier_seq` after
        // `recover`): `set_decided_idx` is a non-synced write, so a crash can
        // recover a `decided_idx` below a barrier that is still durably logged.
        // A decided-only seed would miss that barrier, yet the apply task will
        // fold it once the decision is re-confirmed — reopening the exact
        // false-satisfy this counter exists to prevent. Scanning the accepted
        // suffix instead bounds the nonce above every barrier the log can still
        // surface, lost `decided_idx` or not.
        //
        // The recovery fold still runs, for the other recovered state it is the
        // right source for: the in-memory high-water, the `applied_barriers`
        // ledger, and the recovered decided index that seeds `apply_cursor` so
        // whichever drive path runs (sync stepping or the spawned apply task)
        // resumes from where the fold left off rather than re-draining the
        // decided log from 0. The fold is idempotent (max over advances and
        // per-node seqs), so the worst the old cursor-from-0 behaviour did was
        // redundant work; seeding the cursor here removes that O(decided-log)
        // startup cost on long-lived nodes.
        let engine = ApplyEngine::new(policy);
        let mut recovery_cursor = 0u64;
        engine.recover(&omnipaxos, &mut recovery_cursor);
        let recovered_seq = crate::state_machine::max_logged_barrier_seq(&omnipaxos, my_node_id);

        Self {
            omnipaxos,
            my_node_id,
            barrier_seq: AtomicU64::new(recovered_seq),
            runner,
            leader_subscriber: Mutex::new(leader_subscriber),
            engine,
            task: None,
            apply_cursor: Arc::new(Mutex::new(recovery_cursor)),
            barrier_timeout,
        }
    }

    /// Take ownership of the leader-event subscriber. Returns `None` if
    /// already taken. The driver consumes this at construction and re-derives a
    /// fresh stream from it on every `leadership_events` call. The host retains
    /// no receiver after this hand-off, so the runner's drop-based shutdown
    /// still fires once the driver (and any stream it minted) is dropped.
    pub fn take_leader_subscriber(&self) -> Option<LeaderEventSubscriber> {
        self.leader_subscriber.lock().take()
    }

    /// Borrow the OmniPaxos handle for direct interaction.
    pub fn omnipaxos_handle(&self) -> Arc<Mutex<OmniPaxos<HighWaterCommand, S>>> {
        self.omnipaxos.clone()
    }

    /// Spawn the runner's tick task and the apply task.
    ///
    /// `sink` carries outbound paxos messages to peers. The apply task
    /// awaits the runner's `apply_notify` and drains decided entries
    /// into the in-memory high-water on each wake.
    ///
    /// # Errors
    ///
    /// Returns [`AlreadyRunning`] if the host is already running (a
    /// `start` with no intervening [`Self::stop`]). The guard runs before
    /// either task is spawned, so a rejected call spawns nothing and leaves
    /// the live apply/tick tasks untouched — it never orphans them. `stop`
    /// `take()`s the task handle, so the host is startable again afterwards.
    pub fn start<Sink: MessageSink<HighWaterCommand>>(
        &mut self,
        sink: Arc<Sink>,
    ) -> Result<(), AlreadyRunning> {
        if self.task.is_some() {
            return Err(AlreadyRunning);
        }

        // Start the runner's tick task first. The runner enforces the same
        // not-already-running invariant; mapping its rejection onto this
        // host's `AlreadyRunning` keeps the guards in lockstep, and starting
        // it before spawning the apply task means a rejection cannot orphan a
        // freshly-spawned apply task. The host guard above makes this branch
        // unreachable in practice (task and tick handle move together), so the
        // map is belt-and-suspenders.
        self.runner
            .start(sink)
            .map_err(|_runner_already_running| AlreadyRunning)?;

        // Hand the apply task the shared cursor (seeded in `new` past the
        // recovered suffix) so it resumes there instead of re-draining the
        // decided log from 0 on its first wake.
        self.task = Some(self.engine.spawn(
            self.runner.apply_notify(),
            self.omnipaxos.clone(),
            self.apply_cursor.clone(),
        ));
        Ok(())
    }

    /// Signal shutdown and await both the runner tick task and the
    /// apply task. Surfaces a `tracing::warn!` if the apply task
    /// terminated abnormally.
    pub async fn stop(&mut self) {
        // Taking the Option means a stop() with no running task does nothing,
        // so no shutdown permit can survive to poison a later start(). The
        // notify_one / mid-drain reasoning lives on ApplyTask::stop.
        if let Some(task) = self.task.take() {
            task.stop().await;
        }
        self.runner.stop().await;
    }

    /// Current in-memory high-water value (no consensus round-trip).
    /// Used internally by `current_high_water` after a barrier decides.
    pub fn current_value(&self) -> u64 {
        self.engine.high_water()
    }

    /// Synchronous single step for deterministic test stepping: tick the runner
    /// once and apply any newly-decided entries, returning the runner's outbound
    /// messages for the caller to route. Mutually exclusive with [`Self::start`]
    /// — a host is driven by exactly one of the two paths.
    pub fn step(&self) -> Vec<Message<HighWaterCommand>> {
        let outgoing = self.runner.tick_once();
        self.apply_once();
        outgoing
    }

    /// Tick the runner once *without* applying decided entries, returning its
    /// outbound messages. Lets a deterministic test hold a node's apply "parked"
    /// (decided_idx advances via consensus, but the high-water / barrier ledger
    /// does not fold) — the synchronous analogue of pausing the async apply task
    /// at its yield point. Pair with an explicit [`Self::apply_once`] to release.
    pub fn tick_only(&self) -> Vec<Message<HighWaterCommand>> {
        self.runner.tick_once()
    }

    /// Apply newly-decided entries into the high-water state and snapshot per
    /// policy, advancing the shared apply cursor. The synchronous sibling of
    /// the async apply task; idempotent (max over advances and per-node barrier
    /// seqs).
    pub fn apply_once(&self) {
        let mut cursor = self.apply_cursor.lock();
        self.engine.apply_step(&self.omnipaxos, &mut cursor);
    }

    /// Deliver an inbound message synchronously (deterministic test stepping).
    /// Mutually exclusive with [`Self::start`]'s pump path.
    pub fn deliver(&self, message: Message<HighWaterCommand>) {
        self.runner.handle_incoming(message);
    }

    /// Wait for this node's barrier nonce `seq` to be folded by the apply path,
    /// then return the resulting high-water. `floor`, when set, additionally
    /// requires `high_water >= floor` (the `submit_advance` postcondition; a
    /// read passes `None`).
    ///
    /// Bounded three ways so the wait can never park forever (#354):
    /// - the barrier is folded and the floor (if any) is met — success;
    /// - the apply task that would fold the barrier has died — fail fast with a
    ///   non-retryable [`ConsensusError::PermanentDriver`];
    /// - `barrier_timeout` elapses without progress (quorum loss, lost
    ///   leadership) — give up with a retryable
    ///   [`ConsensusError::TransientDriver`] so the caller can react.
    async fn await_barrier(&self, seq: u64, floor: Option<u64>) -> Result<u64, ConsensusError> {
        let notifier = self.engine.apply_notifier();
        let wait = async {
            loop {
                // Register as waiter before checking state; otherwise a
                // notify_waiters that fires between this check and the next
                // notified().await is lost.
                let notified = notifier.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();

                let folded = self.engine.applied_barrier_seq(self.my_node_id) >= seq;
                let floor_met = match floor {
                    Some(at_least) => self.engine.high_water() >= at_least,
                    None => true,
                };
                if folded && floor_met {
                    return Ok(self.engine.high_water());
                }
                // The apply task that folds barriers is gone, so no further
                // progress is possible — fail fast instead of waiting out the
                // whole deadline. A host driven by the synchronous stepping
                // path never spawns one, so "never spawned" is not death.
                if self.engine.apply_task_died() {
                    return Err(ConsensusError::PermanentDriver(Box::new(ApplyTaskGone)));
                }
                notified.await;
            }
        };

        match tokio::time::timeout(self.barrier_timeout, wait).await {
            Ok(result) => result,
            Err(_elapsed) => Err(ConsensusError::TransientDriver(Box::new(
                BarrierWaitTimeout(self.barrier_timeout),
            ))),
        }
    }
}

/// Builder for [`StandaloneHost`].
pub struct StandaloneHostBuilder<S>
where
    S: Storage<HighWaterCommand> + Send + 'static,
{
    omnipaxos: Option<Arc<Mutex<OmniPaxos<HighWaterCommand, S>>>>,
    my_node_id: Option<u64>,
    peers: Vec<TsoPeer>,
    tick_interval: Duration,
    policy: SnapshotPolicy,
    barrier_timeout: Duration,
}

impl<S> Default for StandaloneHostBuilder<S>
where
    S: Storage<HighWaterCommand> + Send + 'static,
{
    fn default() -> Self {
        Self {
            omnipaxos: None,
            my_node_id: None,
            peers: Vec::new(),
            tick_interval: Duration::from_millis(20),
            policy: SnapshotPolicy::disabled(),
            barrier_timeout: DEFAULT_BARRIER_TIMEOUT,
        }
    }
}

impl<S> StandaloneHostBuilder<S>
where
    S: Storage<HighWaterCommand> + Send + 'static,
    <HighWaterCommand as omnipaxos::storage::Entry>::Snapshot: Send,
{
    pub fn omnipaxos(mut self, omnipaxos: Arc<Mutex<OmniPaxos<HighWaterCommand, S>>>) -> Self {
        self.omnipaxos = Some(omnipaxos);
        self
    }

    pub fn my_node_id(mut self, node_id: u64) -> Self {
        self.my_node_id = Some(node_id);
        self
    }

    pub fn peers(mut self, peers: Vec<TsoPeer>) -> Self {
        self.peers = peers;
        self
    }

    pub fn tick_interval(mut self, tick_interval: Duration) -> Self {
        self.tick_interval = tick_interval;
        self
    }

    pub fn snapshot_policy(mut self, policy: SnapshotPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Override the barrier-wait deadline (default [`DEFAULT_BARRIER_TIMEOUT`]).
    /// See [`StandaloneHost::current_high_water`] / [`StandaloneHost::submit_advance`].
    pub fn barrier_timeout(mut self, barrier_timeout: Duration) -> Self {
        self.barrier_timeout = barrier_timeout;
        self
    }

    pub fn build(self) -> Result<StandaloneHost<S>, BuilderError> {
        let omnipaxos = self.omnipaxos.ok_or(BuilderError::MissingOmnipaxos)?;
        let my_node_id = self.my_node_id.ok_or(BuilderError::MissingNodeId)?;
        Ok(StandaloneHost::new(
            omnipaxos,
            my_node_id,
            self.peers,
            self.tick_interval,
            self.policy,
            self.barrier_timeout,
        ))
    }
}

impl<S> StandaloneHost<S>
where
    S: Storage<HighWaterCommand> + Send + 'static,
    <HighWaterCommand as omnipaxos::storage::Entry>::Snapshot: Send,
{
    #[must_use]
    pub fn builder() -> StandaloneHostBuilder<S> {
        StandaloneHostBuilder::default()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BuilderError {
    #[error("omnipaxos handle is required")]
    MissingOmnipaxos,
    #[error("my_node_id is required")]
    MissingNodeId,
}

/// [`StandaloneHost::start`] was called while the host was already running.
/// The call is rejected before either the apply task or the runner tick task
/// is spawned, so nothing is orphaned; call [`StandaloneHost::stop`] before
/// starting again.
#[derive(Debug, thiserror::Error)]
#[error("StandaloneHost::start called while already running")]
pub struct AlreadyRunning;

#[async_trait]
impl<S> PaxosHighWaterHost for StandaloneHost<S>
where
    S: Storage<HighWaterCommand> + Send + 'static,
    <HighWaterCommand as omnipaxos::storage::Entry>::Snapshot: Send,
{
    type Entry = HighWaterCommand;
    type Storage = S;

    fn omnipaxos(&self) -> Arc<Mutex<OmniPaxos<HighWaterCommand, S>>> {
        self.omnipaxos.clone()
    }

    async fn current_high_water(&self) -> Result<u64, ConsensusError> {
        // Mint a (my_node_id, seq) nonce, append a Barrier carrying it,
        // and wait until the apply path folds *this specific* barrier
        // into the ledger. Tracking by appending-node lets two nodes'
        // independent counters coexist without trampling each other.
        let seq = self.barrier_seq.fetch_add(1, Ordering::SeqCst) + 1;
        self.omnipaxos
            .lock()
            .append(HighWaterCommand::Barrier {
                node: self.my_node_id,
                seq,
            })
            .map_err(|err| {
                ConsensusError::TransientDriver(Box::new(BarrierAppendError(format!("{err:?}"))))
            })?;
        tsoracle_yieldpoint::yieldpoint!(
            "standalone_host::current_high_water::after_append_before_await"
        );
        // A read has no floor — any folded value attributable to this barrier
        // is correct.
        self.await_barrier(seq, None).await
    }

    async fn submit_advance(&self, at_least: u64) -> Result<u64, ConsensusError> {
        // Append the Advance, then a (my_node_id, seq) barrier nonce, and
        // wait until the apply path folds *this specific* barrier — exactly
        // as `current_high_water` does. The barrier is appended immediately
        // after the Advance, so a folded `Barrier { node: self, seq }`
        // proves every earlier decided entry (this call's own Advance among
        // them) has already been folded: the returned high-water is
        // provably attributable to this call.
        //
        // A bare `new_decided > snapshot_decided && high_water() >= at_least`
        // threshold could not make that guarantee — both halves can be
        // satisfied by a *racing* caller's Advance before this call's own
        // entry is applied, so the value returned would not be provably
        // this call's. The trailing `high_water() >= at_least` guard keeps
        // the floor postcondition (unique to `submit_advance`; a read has
        // no floor) even in the corner where a mid-call leadership change
        // drops this Advance while the barrier still decides under the new
        // leader — there the floor is unmet, so we wait rather than return a
        // sub-floor value. The outer epoch fence is the safety net that
        // surfaces the leadership change to the caller.
        self.omnipaxos
            .lock()
            .append(HighWaterCommand::Advance(AdvancePayload { at_least }))
            .map_err(|err| {
                ConsensusError::TransientDriver(Box::new(AdvanceAppendError(format!("{err:?}"))))
            })?;
        let seq = self.barrier_seq.fetch_add(1, Ordering::SeqCst) + 1;
        self.omnipaxos
            .lock()
            .append(HighWaterCommand::Barrier {
                node: self.my_node_id,
                seq,
            })
            .map_err(|err| {
                ConsensusError::TransientDriver(Box::new(BarrierAppendError(format!("{err:?}"))))
            })?;
        tsoracle_yieldpoint::yieldpoint!(
            "standalone_host::submit_advance::after_append_before_await"
        );
        // Keep the floor postcondition (unique to submit_advance) even in the
        // corner where a mid-call leadership change drops this Advance while
        // the barrier still decides under the new leader: there the floor is
        // unmet, so we keep waiting rather than return a sub-floor value.
        self.await_barrier(seq, Some(at_least)).await
    }
}

#[derive(Debug, thiserror::Error)]
#[error("barrier append failed: {0}")]
struct BarrierAppendError(String);

/// The barrier did not fold within `barrier_timeout`. Retryable: the most
/// likely cause is transient (quorum loss, a leadership change in flight), and
/// the caller's epoch fence surfaces a genuine leadership loss separately.
#[derive(Debug, thiserror::Error)]
#[error("barrier wait timed out after {0:?}")]
struct BarrierWaitTimeout(Duration);

/// The apply task that folds barriers has died, so the barrier can never be
/// folded. Non-retryable: a panicked/stopped apply task does not recover by
/// retrying the same call.
#[derive(Debug, thiserror::Error)]
#[error("apply task is gone; barrier can never be folded")]
struct ApplyTaskGone;

#[derive(Debug, thiserror::Error)]
#[error("advance append failed: {0}")]
struct AdvanceAppendError(String);

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(dead_code)]
    fn assert_builder_api_compiles<S>()
    where
        S: Storage<HighWaterCommand> + Send + 'static,
        <HighWaterCommand as omnipaxos::storage::Entry>::Snapshot: Send,
    {
        let _ = StandaloneHost::<S>::builder();
    }

    /// A `StandaloneHost` rebuilt over a decided log (the post-restart shape)
    /// must seed its apply cursor at the recovered decided index, not 0. The
    /// recovery fold in `new` is idempotent, so a cursor of 0 is *correct* but
    /// re-drains the entire decided log on the apply task's first wake —
    /// O(decided-log) redundant work on every long-lived node's startup. The
    /// recovered seed is the single shared `apply_cursor` both drive paths
    /// consume (the spawned apply task clones this exact `Arc`), so asserting
    /// it here pins the value the apply task begins from.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn apply_cursor_is_seeded_from_recovered_decided_suffix() {
        use omnipaxos::{ClusterConfig, OmniPaxosConfig, ServerConfig};
        use std::time::Duration;
        use tokio::sync::mpsc;
        use tsoracle_paxos_toolkit::test_fakes::mem_network::MemNetwork;
        use tsoracle_paxos_toolkit::test_fakes::mem_storage::MemStorage;

        type Handle = Arc<Mutex<OmniPaxos<HighWaterCommand, MemStorage<HighWaterCommand>>>>;

        let network: Arc<MemNetwork<HighWaterCommand>> = Arc::new(MemNetwork::new());
        let node_ids = vec![1u64, 2, 3];
        let cluster_config = ClusterConfig {
            configuration_id: 1,
            nodes: node_ids.clone(),
            flexible_quorum: None,
        };
        let mut handles: Vec<(u64, Handle)> = Vec::new();
        let mut inboxes: Vec<(u64, mpsc::Receiver<Message<HighWaterCommand>>)> = Vec::new();
        for &node_id in &node_ids {
            let server_config = ServerConfig {
                pid: node_id,
                election_tick_timeout: 5,
                resend_message_tick_timeout: 5,
                ..Default::default()
            };
            let config = OmniPaxosConfig {
                cluster_config: cluster_config.clone(),
                server_config,
            };
            let omnipaxos = config
                .build(MemStorage::<HighWaterCommand>::new())
                .expect("build omnipaxos");
            inboxes.push((node_id, network.register(node_id)));
            handles.push((node_id, Arc::new(Mutex::new(omnipaxos))));
        }

        // Drive ticks + message routing until the cluster decides our appended
        // entries. Returns once `predicate` holds; panics after `max_ticks`.
        let mut drive_until = |predicate: &dyn Fn() -> bool, max_ticks: usize| {
            for _ in 0..max_ticks {
                let mut outgoing = Vec::new();
                for (_, handle) in &handles {
                    let mut omnipaxos = handle.lock();
                    omnipaxos.tick();
                    outgoing.extend(omnipaxos.outgoing_messages());
                }
                for message in outgoing {
                    network.deliver_now(message);
                }
                for (node_id, inbox) in &mut inboxes {
                    while let Ok(message) = inbox.try_recv() {
                        let handle = &handles
                            .iter()
                            .find(|(id, _)| id == node_id)
                            .expect("node present")
                            .1;
                        handle.lock().handle_incoming(message);
                    }
                }
                if predicate() {
                    return;
                }
            }
            panic!("predicate did not hold within {max_ticks} ticks");
        };

        let leader_id = || {
            handles
                .iter()
                .find_map(|(_, handle)| handle.lock().get_current_leader())
        };
        drive_until(&|| leader_id().is_some(), 500);
        let leader = leader_id().expect("leader elected");
        let leader_handle = handles
            .iter()
            .find(|(id, _)| *id == leader)
            .expect("leader present")
            .1
            .clone();

        // Decide three Advance entries on the leader.
        {
            let mut omnipaxos = leader_handle.lock();
            for at_least in [10u64, 20, 30] {
                omnipaxos
                    .append(HighWaterCommand::Advance(AdvancePayload { at_least }))
                    .expect("append on leader");
            }
        }
        drive_until(
            &|| {
                handles
                    .iter()
                    .all(|(_, handle)| handle.lock().get_decided_idx() >= 3)
            },
            500,
        );

        let recovered_decided = leader_handle.lock().get_decided_idx();
        assert!(
            recovered_decided >= 3,
            "fixture must produce a non-empty decided log",
        );

        // Build a fresh host over the already-decided handle (restart shape).
        let host = StandaloneHost::new(
            leader_handle.clone(),
            leader,
            Vec::new(),
            Duration::from_millis(2),
            SnapshotPolicy::disabled(),
            DEFAULT_BARRIER_TIMEOUT,
        );

        assert_eq!(
            *host.apply_cursor.lock(),
            recovered_decided,
            "apply cursor must be seeded at the recovered decided index, not re-drained from 0",
        );
    }

    /// A node that crashes after a `Barrier` is fsynced into the log but
    /// before the *non-synced* `set_decided_idx` write that records its
    /// decision recovers a `decided_idx` below that barrier. The barrier still
    /// lives in the durable log; only the decided-index bump was lost. Seeding
    /// the barrier-nonce counter from the recovered decided fold would learn
    /// nothing about that barrier's seq, so a freshly minted post-restart nonce
    /// could collide with it — and the recovered `(self, seq)` would falsely
    /// satisfy the new read before its own barrier was folded. The seed must
    /// instead come from the actual durable log contents, so the next nonce is
    /// strictly greater than any `(self, seq)` the log can still surface.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn barrier_seq_seed_survives_a_lost_decided_suffix() {
        use omnipaxos::ballot_leader_election::Ballot;
        use omnipaxos::storage::Storage as _;
        use omnipaxos::{ClusterConfig, OmniPaxosConfig, ServerConfig};
        use tsoracle_paxos_toolkit::test_fakes::mem_storage::MemStorage;

        const MY_NODE: u64 = 1;
        const DURABLE_BARRIER_SEQ: u64 = 9;

        // Stage the post-crash storage shape directly. A promise must be
        // present or OmniPaxos's failure-recovery load treats the store as
        // empty and ignores the staged log and decided index.
        let mut storage = MemStorage::<HighWaterCommand>::new();
        storage
            .set_promise(Ballot::with(1, 1, 0, MY_NODE))
            .expect("set promise");
        storage
            .append_entries(vec![
                HighWaterCommand::Advance(AdvancePayload { at_least: 100 }),
                HighWaterCommand::Barrier {
                    node: MY_NODE,
                    seq: DURABLE_BARRIER_SEQ,
                },
            ])
            .expect("append durable log");
        // decided_idx covers only the Advance at index 0; the barrier at index
        // 1 is durably logged but its decided bump was the write that was lost.
        storage
            .set_decided_idx(1)
            .expect("persist stale decided idx");

        let cluster_config = ClusterConfig {
            configuration_id: 1,
            nodes: vec![1, 2, 3],
            flexible_quorum: None,
        };
        let server_config = ServerConfig {
            pid: MY_NODE,
            ..Default::default()
        };
        let omnipaxos = OmniPaxosConfig {
            cluster_config,
            server_config,
        }
        .build(storage)
        .expect("build omnipaxos over staged storage");
        let handle = Arc::new(Mutex::new(omnipaxos));

        // The decided view really does hide the barrier: the old decided-fold
        // seed would have learned nothing about seq 9.
        assert_eq!(
            handle.lock().get_decided_idx(),
            1,
            "fixture must keep the barrier past the recovered decided_idx",
        );

        let host = StandaloneHost::new(
            handle,
            MY_NODE,
            Vec::new(),
            Duration::from_millis(2),
            SnapshotPolicy::disabled(),
            DEFAULT_BARRIER_TIMEOUT,
        );

        let seed = host.barrier_seq.load(Ordering::SeqCst);
        assert!(
            seed >= DURABLE_BARRIER_SEQ,
            "seed {seed} must cover the durable barrier seq {DURABLE_BARRIER_SEQ}, \
             so the next minted nonce ({}) cannot collide with a recovered barrier",
            seed + 1,
        );
    }

    #[test]
    fn builder_missing_omnipaxos_errors() {
        use tsoracle_paxos_toolkit::test_fakes::mem_storage::MemStorage;
        let result: Result<StandaloneHost<MemStorage<HighWaterCommand>>, _> =
            StandaloneHost::builder().my_node_id(1).build();
        assert!(matches!(result, Err(BuilderError::MissingOmnipaxos)));
    }

    #[test]
    fn builder_missing_node_id_errors() {
        use omnipaxos::{ClusterConfig, OmniPaxosConfig, ServerConfig};
        use tsoracle_paxos_toolkit::test_fakes::mem_storage::MemStorage;
        let cluster_config = ClusterConfig {
            configuration_id: 1,
            nodes: vec![1, 2, 3],
            flexible_quorum: None,
        };
        let server_config = ServerConfig {
            pid: 1,
            ..Default::default()
        };
        let config = OmniPaxosConfig {
            cluster_config,
            server_config,
        };
        let omnipaxos = config
            .build(MemStorage::<HighWaterCommand>::new())
            .expect("build");
        let arc = Arc::new(Mutex::new(omnipaxos));
        let result = StandaloneHost::builder().omnipaxos(arc).build();
        assert!(matches!(result, Err(BuilderError::MissingNodeId)));
    }
}
