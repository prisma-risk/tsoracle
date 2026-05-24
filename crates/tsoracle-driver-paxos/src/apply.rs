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

//! Apply pipeline for the standalone host.
//!
//! [`ApplyEngine`] bundles the apply state, the snapshot policy, and the
//! single drain-and-snapshot step they drive. [`ApplyTask`] owns the
//! lifecycle of the spawned async apply task. [`crate::StandaloneHost`]
//! holds exactly one engine and at most one task.
//!
//! [`ApplyEngine::apply_step`] is the one drain+snapshot step shared by the
//! host's synchronous stepping path ([`crate::StandaloneHost::apply_once`])
//! and the async apply task spawned by [`ApplyEngine::spawn`]. A host is
//! driven by exactly one of those two paths, so the two never run together.
//!
//! These types are keyed on [`HighWaterCommand`] and are internal to the
//! standalone host: a piggyback host replicating a wider envelope entry
//! cannot reuse them and instead builds its own pipeline directly on the
//! public [`crate::state_machine`] primitives.

use std::sync::Arc;

use omnipaxos::OmniPaxos;
use omnipaxos::storage::Storage;
use parking_lot::Mutex;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tracing::warn;

use crate::log_entry::HighWaterCommand;
use crate::snapshot_policy::SnapshotPolicy;
use crate::state_machine::{ApplyState, drain_decided_into, maybe_snapshot};

/// Apply state + snapshot policy + the drain/snapshot step.
///
/// Cheap to clone (Arc-wrapped fields). [`ApplyEngine::spawn`] moves a clone
/// into the async apply task; the host keeps its own copy for the synchronous
/// stepping path and the barrier-linearized reads.
#[derive(Clone)]
pub(crate) struct ApplyEngine {
    apply_state: ApplyState,
    policy: Arc<Mutex<SnapshotPolicy>>,
}

impl ApplyEngine {
    pub(crate) fn new(policy: SnapshotPolicy) -> Self {
        Self {
            apply_state: ApplyState::new(),
            policy: Arc::new(Mutex::new(policy)),
        }
    }

    /// Fold the decided suffix from `*cursor` into the apply state *without*
    /// snapshotting, advancing the cursor. Used once at host construction to
    /// seed the in-memory high-water and the barrier ledger from recovered
    /// storage. Recovery deliberately skips compaction — a node that just
    /// recovered should not immediately re-snapshot — which is why this is
    /// distinct from [`Self::apply_step`].
    pub(crate) fn recover<S>(
        &self,
        omnipaxos: &Arc<Mutex<OmniPaxos<HighWaterCommand, S>>>,
        cursor: &mut u64,
    ) where
        S: Storage<HighWaterCommand> + Send + 'static,
    {
        drain_decided_into(omnipaxos, cursor, &self.apply_state);
    }

    /// Drain newly-decided entries from `*cursor` into the apply state, then
    /// snapshot per policy, advancing the cursor. The single step shared by
    /// the host's synchronous `apply_once` and the async apply task body;
    /// idempotent (max over advances and per-node barrier seqs).
    pub(crate) fn apply_step<S>(
        &self,
        omnipaxos: &Arc<Mutex<OmniPaxos<HighWaterCommand, S>>>,
        cursor: &mut u64,
    ) where
        S: Storage<HighWaterCommand> + Send + 'static,
    {
        let decided_idx = drain_decided_into(omnipaxos, cursor, &self.apply_state);
        let mut policy = self.policy.lock();
        maybe_snapshot(omnipaxos, &mut policy, decided_idx);
    }

    /// Current in-memory high-water value (no consensus round-trip).
    pub(crate) fn high_water(&self) -> u64 {
        self.apply_state.high_water()
    }

    /// Latest applied barrier sequence the apply path has folded for `node`.
    pub(crate) fn applied_barrier_seq(&self, node: u64) -> u64 {
        self.apply_state.applied_barrier_seq(node)
    }

    /// Notifier the host's blocking-read methods loop on. Edge-triggered,
    /// all-waiters-wake; callers MUST loop and re-check their condition.
    pub(crate) fn apply_notifier(&self) -> Arc<Notify> {
        self.apply_state.apply_notifier()
    }

    /// Spawn the async apply task and return its [`ApplyTask`] handle.
    ///
    /// On each `apply_notify` wake the task drains and snapshots via
    /// [`Self::apply_step`] over a task-local cursor seeded at 0; the fold is
    /// idempotent, so re-draining a recovered suffix is harmless. The task
    /// runs until the returned [`ApplyTask`] is stopped.
    ///
    /// A fresh shutdown `Notify` is minted per spawn rather than reused across
    /// the host's lifetime: `stop` signals with `notify_one`, which stores a
    /// permit when no task is parked on `notified()`; a reused `Notify` would
    /// carry that stale permit into the next task, which would consume it and
    /// exit immediately. Confining each `Notify` to the task it was minted for
    /// keeps every permit scoped to its own task.
    pub(crate) fn spawn<S>(
        &self,
        apply_notify: Arc<Notify>,
        omnipaxos: Arc<Mutex<OmniPaxos<HighWaterCommand, S>>>,
    ) -> ApplyTask
    where
        S: Storage<HighWaterCommand> + Send + 'static,
        <HighWaterCommand as omnipaxos::storage::Entry>::Snapshot: Send,
    {
        let shutdown = Arc::new(Notify::new());
        let task_shutdown = shutdown.clone();
        let engine = self.clone();
        let handle = tokio::spawn(async move {
            let mut cursor: u64 = 0;
            loop {
                tokio::select! {
                    _ = apply_notify.notified() => {
                        engine.apply_step(&omnipaxos, &mut cursor);
                        tsoracle_yieldpoint::yieldpoint!(
                            "standalone_host::apply_task::between_iterations"
                        );
                    }
                    _ = task_shutdown.notified() => {
                        break;
                    }
                }
            }
        });
        ApplyTask { handle, shutdown }
    }
}

/// Lifecycle handle for the spawned apply task.
///
/// Bundling the join handle and the per-spawn shutdown `Notify` in one value
/// lets the host represent "running" as a single `Option<ApplyTask>` — the
/// apply task cannot be left half-installed (handle without shutdown, or vice
/// versa).
pub(crate) struct ApplyTask {
    handle: JoinHandle<()>,
    shutdown: Arc<Notify>,
}

impl ApplyTask {
    /// Signal shutdown and await the task, surfacing a `tracing::warn!` if it
    /// terminated abnormally.
    ///
    /// `notify_one` (not `notify_waiters`): the task may be mid-drain rather
    /// than parked on `notified()` when this fires; the stored permit is then
    /// consumed on its next `select!` turn instead of being lost and
    /// livelocking against the runner's per-tick `apply_notify`. Consuming
    /// `self` makes double-stop unrepresentable.
    pub(crate) async fn stop(self) {
        self.shutdown.notify_one();
        if let Err(err) = self.handle.await {
            warn!(error = ?err, "paxos driver apply task terminated abnormally");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_engine_starts_at_zero_high_water() {
        let engine = ApplyEngine::new(SnapshotPolicy::disabled());
        assert_eq!(engine.high_water(), 0);
    }

    #[test]
    fn applied_barrier_seq_is_zero_for_unseen_node() {
        let engine = ApplyEngine::new(SnapshotPolicy::disabled());
        assert_eq!(engine.applied_barrier_seq(7), 0);
    }

    #[test]
    fn apply_notifier_is_stable_across_calls() {
        let engine = ApplyEngine::new(SnapshotPolicy::disabled());
        assert!(Arc::ptr_eq(
            &engine.apply_notifier(),
            &engine.apply_notifier()
        ));
    }

    #[test]
    fn clone_shares_apply_state() {
        // The spawned task gets a clone; it must observe the same notifier the
        // host's blocking reads loop on, or wakeups would never reach them.
        let engine = ApplyEngine::new(SnapshotPolicy::disabled());
        let clone = engine.clone();
        assert!(Arc::ptr_eq(
            &engine.apply_notifier(),
            &clone.apply_notifier()
        ));
    }
}
