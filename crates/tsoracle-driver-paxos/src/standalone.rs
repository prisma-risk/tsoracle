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
use omnipaxos::storage::Storage;
use parking_lot::Mutex;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tracing::warn;
use tsoracle_consensus::ConsensusError;
use tsoracle_paxos_toolkit::lifecycle::{LeaderEventStream, MessageSink, PaxosRunner, Peer};

use crate::host::PaxosHighWaterHost;
use crate::log_entry::HighWaterCommand;
use crate::snapshot_policy::SnapshotPolicy;
use crate::state_machine::{ApplyState, drain_decided_into, maybe_snapshot};

/// Standalone host owning its own paxos cluster + apply pipeline.
pub struct StandaloneHost<S>
where
    S: Storage<HighWaterCommand> + Send + 'static,
{
    omnipaxos: Arc<Mutex<OmniPaxos<HighWaterCommand, S>>>,
    my_node_id: u64,
    barrier_seq: AtomicU64,
    runner: PaxosRunner<HighWaterCommand, S>,
    apply_state: ApplyState,
    leader_stream: Mutex<Option<LeaderEventStream>>,
    apply_handle: Mutex<Option<JoinHandle<()>>>,
    apply_shutdown: Arc<Notify>,
    policy: Arc<Mutex<SnapshotPolicy>>,
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
        peers: Vec<Peer>,
        tick_interval: Duration,
        policy: SnapshotPolicy,
    ) -> Self {
        let mut runner = PaxosRunner::new(omnipaxos.clone(), my_node_id, peers, tick_interval);
        let leader_stream = runner.take_leader_stream();
        Self {
            omnipaxos,
            my_node_id,
            barrier_seq: AtomicU64::new(0),
            runner,
            apply_state: ApplyState::new(),
            leader_stream: Mutex::new(leader_stream),
            apply_handle: Mutex::new(None),
            apply_shutdown: Arc::new(Notify::new()),
            policy: Arc::new(Mutex::new(policy)),
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
            self.apply_handle.lock().is_none(),
            "StandaloneHost::start called while already running",
        );

        let omnipaxos = self.omnipaxos.clone();
        let apply_notify = self.runner.apply_notify();
        let apply_state = self.apply_state.clone();
        let policy = self.policy.clone();
        let shutdown = self.apply_shutdown.clone();

        let handle = tokio::spawn(async move {
            let mut cursor: u64 = 0;
            loop {
                tokio::select! {
                    _ = apply_notify.notified() => {
                        let decided_idx = drain_decided_into(&omnipaxos, &mut cursor, &apply_state);
                        {
                            let mut policy = policy.lock();
                            maybe_snapshot(&omnipaxos, &mut policy, decided_idx);
                        }
                        tsoracle_yieldpoint::yieldpoint!(
                            "standalone_host::apply_task::between_iterations"
                        );
                    }
                    _ = shutdown.notified() => {
                        break;
                    }
                }
            }
        });
        *self.apply_handle.lock() = Some(handle);

        self.runner.start(sink);
    }

    /// Signal shutdown and await both the runner tick task and the
    /// apply task. Surfaces a `tracing::warn!` if the apply task
    /// terminated abnormally.
    pub async fn stop(&mut self) {
        // notify_one stores a permit; notify_waiters would be lost if the
        // apply task is mid-drain when stop() fires, livelocking against
        // the runner's per-tick apply_notify.
        self.apply_shutdown.notify_one();
        let handle = self.apply_handle.lock().take();
        if let Some(handle) = handle {
            if let Err(err) = handle.await {
                warn!(error = ?err, "paxos driver apply task terminated abnormally");
            }
        }
        self.runner.stop().await;
    }

    /// Current in-memory high-water value (no consensus round-trip).
    /// Used internally by `current_high_water` after a barrier decides.
    pub fn current_value(&self) -> u64 {
        self.apply_state.high_water()
    }
}

/// Builder for [`StandaloneHost`].
pub struct StandaloneHostBuilder<S>
where
    S: Storage<HighWaterCommand> + Send + 'static,
{
    omnipaxos: Option<Arc<Mutex<OmniPaxos<HighWaterCommand, S>>>>,
    my_node_id: Option<u64>,
    peers: Vec<Peer>,
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

    pub fn peers(mut self, peers: Vec<Peer>) -> Self {
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
        let notifier = self.apply_state.apply_notifier();
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
            if self.apply_state.applied_barrier_seq(self.my_node_id) >= seq {
                return Ok(self.apply_state.high_water());
            }
            notified.await;
        }
    }

    async fn submit_advance(&self, at_least: u64) -> Result<u64, ConsensusError> {
        let snapshot_decided = self.omnipaxos.lock().get_decided_idx();
        self.omnipaxos
            .lock()
            .append(HighWaterCommand::Advance { at_least })
            .map_err(|err| {
                ConsensusError::TransientDriver(Box::new(AdvanceAppendError(format!("{err:?}"))))
            })?;
        let notifier = self.apply_state.apply_notifier();
        tsoracle_yieldpoint::yieldpoint!(
            "standalone_host::submit_advance::after_append_before_await"
        );
        loop {
            let notified = notifier.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let new_decided = self.omnipaxos.lock().get_decided_idx();
            if new_decided > snapshot_decided && self.apply_state.high_water() >= at_least {
                return Ok(self.apply_state.high_water());
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
