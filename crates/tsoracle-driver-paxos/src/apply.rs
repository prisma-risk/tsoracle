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
use std::sync::atomic::{AtomicU8, Ordering};

use omnipaxos::OmniPaxos;
use omnipaxos::storage::Storage;
use parking_lot::Mutex;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tracing::warn;

use crate::log_entry::HighWaterCommand;
use crate::snapshot_policy::SnapshotPolicy;
use crate::state_machine::{ApplyState, drain_decided_into, maybe_snapshot};

/// Liveness of the async apply task, tracked so a barrier waiter can tell
/// "no task has ever been spawned" (the synchronous stepping path drives
/// `apply_once` itself) from "a task was spawned and has since died" (a panic
/// or shutdown — the barrier can never be folded, so the waiter must fail
/// fast rather than park).
const APPLY_NEVER_SPAWNED: u8 = 0;
const APPLY_ALIVE: u8 = 1;
const APPLY_DEAD: u8 = 2;

/// Apply state + snapshot policy + the drain/snapshot step.
///
/// Cheap to clone (Arc-wrapped fields). [`ApplyEngine::spawn`] moves a clone
/// into the async apply task; the host keeps its own copy for the synchronous
/// stepping path and the barrier-linearized reads.
#[derive(Clone)]
pub(crate) struct ApplyEngine {
    apply_state: ApplyState,
    policy: Arc<Mutex<SnapshotPolicy>>,
    /// Shared liveness of the spawned apply task — see the `APPLY_*` constants.
    /// `spawn` flips it to `APPLY_ALIVE`; the task's death-guard flips it to
    /// `APPLY_DEAD` on any exit. Read by the host's barrier waits via
    /// [`Self::apply_task_died`].
    apply_liveness: Arc<AtomicU8>,
}

impl ApplyEngine {
    pub(crate) fn new(policy: SnapshotPolicy) -> Self {
        Self {
            apply_state: ApplyState::new(),
            policy: Arc::new(Mutex::new(policy)),
            apply_liveness: Arc::new(AtomicU8::new(APPLY_NEVER_SPAWNED)),
        }
    }

    /// Whether a spawned apply task has died (panicked or been shut down).
    ///
    /// `false` while no task has ever been spawned — the synchronous stepping
    /// path ([`crate::StandaloneHost::apply_once`]) folds barriers itself, so
    /// "never spawned" must not be mistaken for "dead". A barrier waiter uses
    /// this to fail fast once the task that would fold its barrier is gone.
    pub(crate) fn apply_task_died(&self) -> bool {
        self.apply_liveness.load(Ordering::Acquire) == APPLY_DEAD
    }

    /// Fold the decided suffix from `*cursor` into the apply state *without*
    /// snapshotting, advancing the cursor. Used once at host construction to
    /// seed the in-memory high-water and the barrier ledger from recovered
    /// storage. Recovery deliberately skips compaction — a node that just
    /// recovered should not immediately re-snapshot — which is why this is
    /// distinct from [`Self::apply_step`].
    ///
    /// Recovery also rebases the snapshot policy's baseline to the recovered
    /// decided index. The baseline starts at 0, so without this a node
    /// reopening its log at a decided index past the snapshot interval would
    /// snapshot spuriously on its first post-recovery [`Self::apply_step`]
    /// (`decided_idx >= 0 + every_n`). Rebasing makes the next snapshot fire
    /// `every_n` entries past the restart point — extending the "skip
    /// compaction at recovery" intent to the first decision after recovery.
    pub(crate) fn recover<S>(
        &self,
        omnipaxos: &Arc<Mutex<OmniPaxos<HighWaterCommand, S>>>,
        cursor: &mut u64,
    ) where
        S: Storage<HighWaterCommand> + Send + 'static,
    {
        drain_decided_into(omnipaxos, cursor, &self.apply_state);
        self.policy.lock().rebase(*cursor);
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
    /// `cursor` is the shared decided-log cursor the host seeded in
    /// [`crate::StandaloneHost::new`] past any recovered suffix; the task
    /// resumes from there rather than re-draining the whole decided log from 0
    /// on its first wake. It is the same cursor the synchronous stepping path
    /// (`apply_once`) locks — a host is driven by exactly one path, so the lock
    /// is uncontended. On each `apply_notify` wake the task drains and
    /// snapshots via [`Self::apply_step`]. The task runs until the returned
    /// [`ApplyTask`] is stopped.
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
        cursor: Arc<Mutex<u64>>,
    ) -> ApplyTask
    where
        S: Storage<HighWaterCommand> + Send + 'static,
        <HighWaterCommand as omnipaxos::storage::Entry>::Snapshot: Send,
    {
        let shutdown = Arc::new(Notify::new());
        let task_shutdown = shutdown.clone();
        let engine = self.clone();

        // Mark the task live *before* spawning so a `start()` caller that
        // immediately issues a barrier read observes `APPLY_ALIVE`, never a
        // transient `APPLY_NEVER_SPAWNED`. The death-guard, moved into the
        // task, flips it back to `APPLY_DEAD` on any exit — the shutdown
        // break, a panic in `apply_step`, or the task being aborted/dropped —
        // and wakes parked barrier readers on the apply-state notifier so they
        // fail fast instead of hanging on a task that will never fold again.
        self.apply_liveness.store(APPLY_ALIVE, Ordering::Release);
        let death_guard = ApplyDeathGuard {
            liveness: self.apply_liveness.clone(),
            waiters: self.apply_notifier(),
        };

        let handle = tokio::spawn(async move {
            let _death_guard = death_guard;
            loop {
                tokio::select! {
                    _ = apply_notify.notified() => {
                        // Scope the cursor guard so it drops before the await
                        // below: the parking_lot guard is !Send and may not be
                        // held across the yield point.
                        {
                            let mut cursor = cursor.lock();
                            engine.apply_step(&omnipaxos, &mut cursor);
                        }
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

/// Drop-guard moved into the spawned apply task. On the task's exit — by any
/// path, including a panic unwind or an abort — it marks the apply path dead
/// and wakes parked barrier readers so they observe the death and fail fast.
///
/// `store(DEAD)` happens *before* `notify_waiters` so a reader woken by the
/// notify always observes `APPLY_DEAD` on its re-check, never a stale
/// `APPLY_ALIVE`.
struct ApplyDeathGuard {
    liveness: Arc<AtomicU8>,
    waiters: Arc<Notify>,
}

impl Drop for ApplyDeathGuard {
    fn drop(&mut self) {
        self.liveness.store(APPLY_DEAD, Ordering::Release);
        self.waiters.notify_waiters();
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
