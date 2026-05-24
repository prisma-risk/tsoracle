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
use tsoracle_paxos_toolkit::lifecycle::{LeaderEventStream, MessageSink, PaxosRunner, TsoPeer};

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
    leader_stream: Mutex<Option<LeaderEventStream>>,
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
    /// Decided-log cursor for the synchronous [`Self::step`] / [`Self::apply_once`]
    /// stepping path, seeded past any recovered suffix. The async apply task
    /// keeps its own task-local cursor; a host is driven by exactly one path.
    step_cursor: Mutex<u64>,
}

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
    ) -> Self {
        let mut runner = PaxosRunner::new(omnipaxos.clone(), my_node_id, peers, tick_interval);
        let leader_stream = runner.take_leader_stream();

        // Resume the barrier-nonce counter above any seq this node already
        // used in a prior process lifetime. `barrier_seq` is process-local
        // and resets to 0 on restart, but the `applied_barriers` ledger is
        // durable — restored from decided-log replay and snapshot transfer.
        // `current_high_water` waits for `applied_barrier_seq(self) >=
        // minted_seq`; minting from 0 would hand back a seq a recovered
        // `(self, old_seq)` entry already satisfies, letting the read return
        // before its own barrier is applied and seeding the new leader's
        // allocator below the prior ceiling. Folding the recovered decided
        // suffix here learns this node's highest durable seq and lifts the
        // counter above it, so a freshly minted seq can only be satisfied by
        // this lifetime's own barrier. The apply task re-drains from its own
        // cursor on start; the fold is idempotent (max over advances and
        // per-node seqs), so seeding the shared state early is safe.
        let engine = ApplyEngine::new(policy);
        let mut recovery_cursor = 0u64;
        engine.recover(&omnipaxos, &mut recovery_cursor);
        let recovered_seq = engine.applied_barrier_seq(my_node_id);

        Self {
            omnipaxos,
            my_node_id,
            barrier_seq: AtomicU64::new(recovered_seq),
            runner,
            leader_stream: Mutex::new(leader_stream),
            engine,
            task: None,
            step_cursor: Mutex::new(recovery_cursor),
        }
    }

    /// Take ownership of the leader-event stream. Returns `None` if
    /// already taken. The driver consumes this at construction.
    pub fn take_leader_stream(&self) -> Option<LeaderEventStream> {
        self.leader_stream.lock().take()
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
    /// # Preconditions
    ///
    /// Must not be called while the host is already running. Debug
    /// builds assert this; release builds would leave the previous
    /// apply task orphaned.
    pub fn start<Sink: MessageSink<HighWaterCommand>>(&mut self, sink: Arc<Sink>) {
        debug_assert!(
            self.task.is_none(),
            "StandaloneHost::start called while already running",
        );

        self.task = Some(
            self.engine
                .spawn(self.runner.apply_notify(), self.omnipaxos.clone()),
        );
        self.runner.start(sink);
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
    /// policy, advancing the step cursor. The synchronous sibling of the async
    /// apply task; idempotent (max over advances and per-node barrier seqs).
    pub fn apply_once(&self) {
        let mut cursor = self.step_cursor.lock();
        self.engine.apply_step(&self.omnipaxos, &mut cursor);
    }

    /// Deliver an inbound message synchronously (deterministic test stepping).
    /// Mutually exclusive with [`Self::start`]'s pump path.
    pub fn deliver(&self, message: Message<HighWaterCommand>) {
        self.runner.handle_incoming(message);
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

    pub fn build(self) -> Result<StandaloneHost<S>, BuilderError> {
        let omnipaxos = self.omnipaxos.ok_or(BuilderError::MissingOmnipaxos)?;
        let my_node_id = self.my_node_id.ok_or(BuilderError::MissingNodeId)?;
        Ok(StandaloneHost::new(
            omnipaxos,
            my_node_id,
            self.peers,
            self.tick_interval,
            self.policy,
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
        let notifier = self.engine.apply_notifier();
        tsoracle_yieldpoint::yieldpoint!(
            "standalone_host::current_high_water::after_append_before_await"
        );
        loop {
            // Register as waiter before checking state; otherwise a
            // notify_waiters that fires between the previous iteration's
            // check and the next notified().await is lost.
            let notified = notifier.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.engine.applied_barrier_seq(self.my_node_id) >= seq {
                return Ok(self.engine.high_water());
            }
            notified.await;
        }
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
        let notifier = self.engine.apply_notifier();
        tsoracle_yieldpoint::yieldpoint!(
            "standalone_host::submit_advance::after_append_before_await"
        );
        loop {
            // Register as waiter before checking state; otherwise a
            // notify_waiters that fires between the previous iteration's
            // check and the next notified().await is lost.
            let notified = notifier.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.engine.applied_barrier_seq(self.my_node_id) >= seq
                && self.engine.high_water() >= at_least
            {
                return Ok(self.engine.high_water());
            }
            notified.await;
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("barrier append failed: {0}")]
struct BarrierAppendError(String);

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
