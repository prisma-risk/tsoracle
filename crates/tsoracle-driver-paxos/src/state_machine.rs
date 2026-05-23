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

//! Apply-task state machine.
//!
//! Owns the in-memory high-water mark (an `AtomicU64`) and the apply
//! notifier the host's blocking-read methods loop on. The apply task is
//! spawned by the host (`StandaloneHost` in a follow-up sub-issue),
//! awaits the toolkit runner's `apply_notify`, then drains the
//! OmniPaxos log's decided suffix into the AtomicU64 — exactly once per
//! decided entry.
//!
//! No per-proposal tracking. Blocking-read methods (`submit_advance`,
//! `current_high_water`) snapshot `decided_idx` and the AtomicU64
//! before they append, then loop on the `apply_notifier` until their
//! waitcondition is satisfied. This is the consequence of
//! `OmniPaxos::append` not returning a log index for the proposing node.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use omnipaxos::OmniPaxos;
use omnipaxos::storage::Storage;
use omnipaxos::util::LogEntry;
use parking_lot::Mutex;
use tokio::sync::Notify;
use tracing::trace;

use crate::log_entry::HighWaterCommand;
use crate::snapshot_policy::SnapshotPolicy;

/// Shared apply-task state.
///
/// Cheap to clone (Arc-wrapped fields). The host clones one copy into
/// the spawned apply task and keeps another for its blocking-read
/// methods to poll.
#[derive(Clone)]
pub struct ApplyState {
    high_water: Arc<AtomicU64>,
    apply_notifier: Arc<Notify>,
}

impl Default for ApplyState {
    fn default() -> Self {
        Self::new()
    }
}

impl ApplyState {
    #[must_use]
    pub fn new() -> Self {
        Self {
            high_water: Arc::new(AtomicU64::new(0)),
            apply_notifier: Arc::new(Notify::new()),
        }
    }

    /// Current in-memory high-water value.
    #[must_use]
    pub fn high_water(&self) -> u64 {
        self.high_water.load(Ordering::SeqCst)
    }

    /// Notifier the host's blocking-read methods loop on.
    ///
    /// Edge-triggered, all-waiters-wake (matches `Notify::notify_waiters`
    /// semantics). Callers MUST loop and re-check their condition; one
    /// wake does not correspond to one decided entry.
    #[must_use]
    pub fn apply_notifier(&self) -> Arc<Notify> {
        self.apply_notifier.clone()
    }
}

/// Drain decided entries from `omnipaxos` starting at `*cursor`, folding
/// `Advance` commands into the AtomicU64, then notify pollers. Returns
/// the new `decided_idx` (so the caller can update its cursor).
///
/// Designed to be called from the host's spawned apply task. The host
/// awaits the toolkit runner's `apply_notify` between calls; this
/// function is the apply step itself and does no waiting of its own.
pub fn drain_decided_into<S>(
    omnipaxos: &Arc<Mutex<OmniPaxos<HighWaterCommand, S>>>,
    cursor: &mut u64,
    state: &ApplyState,
) -> u64
where
    S: Storage<HighWaterCommand> + Send + 'static,
{
    let (decided_idx, entries) = {
        let handle = omnipaxos.lock();
        let decided_idx = handle.get_decided_idx();
        if decided_idx <= *cursor {
            return decided_idx;
        }
        let entries = handle.read_decided_suffix(*cursor);
        (decided_idx, entries)
    };

    if let Some(entries) = entries {
        for entry in &entries {
            match entry {
                LogEntry::Decided(HighWaterCommand::Advance { at_least }) => {
                    let prev = state.high_water.load(Ordering::SeqCst);
                    if *at_least > prev {
                        state.high_water.store(*at_least, Ordering::SeqCst);
                        trace!(prev, new = at_least, "high-water advanced");
                    }
                }
                LogEntry::Decided(HighWaterCommand::Barrier) => {
                    // No-op marker; the AtomicU64 is unchanged.
                }
                LogEntry::Snapshotted(snapshotted) => {
                    // OmniPaxos may surface a Snapshotted entry on log
                    // catch-up; reflect its value if it's higher.
                    let prev = state.high_water.load(Ordering::SeqCst);
                    if snapshotted.snapshot.value > prev {
                        state
                            .high_water
                            .store(snapshotted.snapshot.value, Ordering::SeqCst);
                    }
                }
                LogEntry::Trimmed(_) | LogEntry::StopSign(_, _) | LogEntry::Undecided(_) => {
                    // Out of scope for the high-water fold.
                }
            }
        }
    }

    *cursor = decided_idx;
    state.apply_notifier.notify_waiters();
    decided_idx
}

/// Maybe trigger a snapshot via the given policy. Called by the host's
/// apply task after each successful drain.
pub fn maybe_snapshot<S>(
    omnipaxos: &Arc<Mutex<OmniPaxos<HighWaterCommand, S>>>,
    policy: &mut SnapshotPolicy,
    decided_idx: u64,
) where
    S: Storage<HighWaterCommand> + Send + 'static,
{
    if policy.should_snapshot(decided_idx) {
        let mut handle = omnipaxos.lock();
        // local_only=false: best-effort cluster-wide snapshot. Errors
        // are logged but not propagated; snapshot failure is not fatal
        // for liveness.
        if let Err(err) = handle.snapshot(Some(decided_idx), false) {
            tracing::warn!(?err, "snapshot trigger failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn new_state_starts_at_zero() {
        let state = ApplyState::new();
        assert_eq!(state.high_water(), 0);
    }

    #[tokio::test]
    async fn apply_notifier_handle_is_shared() {
        let state = ApplyState::new();
        let first = state.apply_notifier();
        let second = state.apply_notifier();
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[tokio::test]
    async fn apply_notifier_wakes_parked_waiters() {
        let state = ApplyState::new();
        let notifier = state.apply_notifier();
        let waiter = tokio::spawn(async move { notifier.notified().await });
        // Give the spawned task time to park on the notify.
        tokio::time::sleep(Duration::from_millis(10)).await;
        state.apply_notifier.notify_waiters();
        tokio::time::timeout(Duration::from_millis(50), waiter)
            .await
            .expect("waiter wakes")
            .expect("task joins");
    }

    #[tokio::test]
    async fn high_water_load_reflects_atomic_stores() {
        let state = ApplyState::new();
        state.high_water.store(42, Ordering::SeqCst);
        assert_eq!(state.high_water(), 42);
    }
}
