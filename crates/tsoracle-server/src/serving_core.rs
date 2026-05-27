//
//  ░▀█▀░█▀▀░█▀█░█▀▄░█▀█░█▀▀░█░░░█▀▀
//  ░░█░░▀▀█░█░█░█▀▄░█▀█░█░░░█░░░█▀▀
//  ░░▀░░▀▀▀░▀▀▀░▀░▀░▀░▀░▀▀▀░▀▀▀░▀▀▀
//
//  tsoracle — Distributed Timestamp Oracle
//  https://www.tsoracle.rs
//
//  Copyright (c) 2026 Prisma Risk
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

//! The serving-state + allocator + extension-lock domain, extracted from
//! `Server` so the load-bearing ordering invariants are private and testable.
//!
//! Four invariants used to live as prose spread across `server.rs`,
//! `service.rs`, and `fence.rs`. They now live here:
//!
//! 1. `extension_lock` is acquired before `extension_gate.read()`. Enforced by
//!    construction: the read barrier is [`ExtensionSlot::drain_barrier`], a
//!    method on the guard returned by [`ServingCore::extension_slot`], which
//!    already holds `extension_lock`. There is no other way to reach the read
//!    lock, so the order cannot be inverted.
//! 2. Clearing the allocator and publishing `NotServing` happen together, clear
//!    *before* publish, so a racing `try_grant` never sees `Serving` together
//!    with a still-leader allocator at a stale epoch. Enforced by construction:
//!    [`ServingCore::step_down`] and [`ServingCore::enter_fencing`] are the only
//!    ways to clear the allocator (there is no standalone clear primitive), and
//!    both bake in the order, so no call site can invert it.
//! 3. [`ServingCore::step_down`] does **not** take `extension_gate.write()`.
//!    Doing so would deadlock against in-flight extensions holding the read
//!    lock and awaiting `persist_high_water`.
//! 4. The fence takes `extension_gate.write()` via
//!    [`ServingCore::drain_barrier_write`] and never takes `extension_lock`, so
//!    it cannot close a lock cycle with an extender.

use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::{
    Mutex as AsyncMutex, MutexGuard as AsyncMutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard,
    watch,
};
use tsoracle_core::{
    Allocator, CommitOutcome, CoreError, Epoch, PeerEndpoint, PhysicalMs, WindowGrant,
};

use crate::server::ServingState;

pub(crate) struct ServingCore {
    allocator: Mutex<Allocator>,
    /// Source of truth for serving state. Mints receivers for `subscribe` and is
    /// read synchronously via `serving_state`; publish sites use `send_replace`
    /// so transitions land even with zero receivers (see #346).
    state_tx: watch::Sender<ServingState>,
    /// Serializes window extensions so a stampeding burst of `WindowExhausted`
    /// requests resolves to a single `persist_high_water` round-trip. Acquired
    /// before `extension_gate` — enforced by the [`ExtensionSlot`] API.
    extension_lock: AsyncMutex<()>,
    /// Read-locked by window-extension calls for their prepare → persist →
    /// commit dance. The fence takes the write lock between clearing serving and
    /// reloading the high-water, draining all in-flight extensions started under
    /// the prior epoch before it proceeds.
    extension_gate: RwLock<()>,
    /// Mirrors `Server::window_ahead`; held here so the safe-point accessor
    /// (`current_max_safe_physical_ms`) does not need a back-reference to
    /// `Server`.
    window_ahead: Duration,
}

impl ServingCore {
    pub(crate) fn new(window_ahead: Duration) -> Self {
        let (state_tx, _) = watch::channel(ServingState::NotServing {
            leader_endpoint: None,
            leader_epoch: None,
        });
        ServingCore {
            allocator: Mutex::new(Allocator::new()),
            state_tx,
            extension_lock: AsyncMutex::new(()),
            extension_gate: RwLock::new(()),
            window_ahead,
        }
    }

    /// Current safe-point in physical-millisecond units, matching the
    /// `GetCurrentMaxSafeResponse.max_safe_physical_ms` contract: the highest
    /// physical_ms such that any timestamp issued at or before this physical_ms
    /// is durably backed by the persisted window. Returns `0` when this node
    /// is not the leader, or before the leader admits its first window.
    pub(crate) fn current_max_safe_physical_ms(&self) -> u64 {
        let Some(persisted) = self.allocator.lock().committed_high_water() else {
            return 0;
        };
        let window_ms = self.window_ahead.as_millis() as u64;
        persisted.saturating_sub(window_ms + 1)
    }

    /// Current leader epoch, or `None` when not the leader. Mirrors
    /// `Allocator::epoch` for the service layer (which holds `Arc<ServingCore>`
    /// and has no direct allocator handle).
    pub(crate) fn current_epoch(&self) -> Option<Epoch> {
        self.allocator.lock().epoch()
    }

    pub(crate) fn subscribe(&self) -> watch::Receiver<ServingState> {
        self.state_tx.subscribe()
    }

    pub(crate) fn serving_state(&self) -> ServingState {
        self.state_tx.borrow().clone()
    }

    /// Non-cloning serving check for the hot-path NOT_LEADER gate. Borrows the
    /// watch value and inspects the variant in place, where `serving_state`
    /// would clone (and the caller then discard) a whole `ServingState`. The
    /// gate only needs the yes/no answer; the rejected path re-reads through
    /// `serving_state` to build the redirect hint, where cloning the
    /// leader-endpoint `String` into the response is unavoidable regardless.
    pub(crate) fn is_serving(&self) -> bool {
        matches!(&*self.state_tx.borrow(), ServingState::Serving)
    }

    pub(crate) fn publish_serving(&self) {
        self.state_tx.send_replace(ServingState::Serving);
    }

    pub(crate) fn publish_not_serving(
        &self,
        leader_endpoint: Option<PeerEndpoint>,
        leader_epoch: Option<Epoch>,
    ) {
        self.state_tx.send_replace(ServingState::NotServing {
            leader_endpoint,
            leader_epoch,
        });
    }

    /// Step down in response to authoritative evidence the current epoch is no
    /// longer valid (consensus rejection, leader-watch termination).
    ///
    /// Clears the allocator BEFORE publishing `NotServing` (invariant 2) and
    /// deliberately does NOT take `extension_gate.write()` (invariant 3): an
    /// in-flight extension holding the read lock and awaiting `persist_high_water`
    /// would deadlock against a write here. Those in-flights complete cleanly or
    /// fail and reach this method themselves — it is idempotent.
    pub(crate) fn step_down(
        &self,
        leader_endpoint: Option<PeerEndpoint>,
        leader_epoch: Option<Epoch>,
    ) {
        self.allocator.lock().on_leadership_lost();
        self.state_tx.send_replace(ServingState::NotServing {
            leader_endpoint,
            leader_epoch,
        });
    }

    /// Enter the fencing window at the start of a leadership transition: clear
    /// the allocator and publish `NotServing` (with no leader hint) so a racing
    /// `try_grant` returns NOT_LEADER until the fence republishes `Serving`.
    ///
    /// Shares [`step_down`](Self::step_down)'s clear-before-publish body
    /// (invariant 2); it is named for the leadership-*gain* path, where
    /// `step_down` would read backwards at the call site. Together with
    /// `step_down` these are the *only* ways to clear the allocator: there is no
    /// standalone clear primitive, so a clear can never be published out of
    /// order with `NotServing`.
    pub(crate) fn enter_fencing(&self) {
        self.step_down(None, None);
    }

    pub(crate) fn try_grant(&self, now_ms: u64, count: u32) -> Result<WindowGrant, CoreError> {
        self.allocator.lock().try_grant(now_ms, count)
    }

    pub(crate) fn seed_on_leadership_gained(
        &self,
        serving_floor: u64,
        committed_ceiling: u64,
        epoch: Epoch,
    ) -> Result<(), CoreError> {
        // PhysicalMs wrap at the ServingCore boundary — the per-parameter
        // PHYSICAL_MS_MAX check that used to live inside the allocator now
        // lives in PhysicalMs::try_new and surfaces here.
        let serving_floor = PhysicalMs::try_new(serving_floor)?;
        let committed_ceiling = PhysicalMs::try_new(committed_ceiling)?;
        self.allocator
            .lock()
            .try_on_leadership_gained(serving_floor, committed_ceiling, epoch)
    }

    pub(crate) fn commit_extension(
        &self,
        actual: u64,
        epoch: Epoch,
    ) -> Result<CommitOutcome, CoreError> {
        let actual = PhysicalMs::try_new(actual)?;
        self.allocator
            .lock()
            .try_commit_window_extension(actual, epoch)
    }

    /// Enter the window-extension single-flight slot, holding `extension_lock`
    /// for the returned guard's lifetime. The read barrier is reachable only
    /// from this guard (see [`ExtensionSlot::drain_barrier`]).
    pub(crate) async fn extension_slot(&self) -> ExtensionSlot<'_> {
        let lock = self.extension_lock.lock().await;
        ExtensionSlot {
            _lock: lock,
            core: self,
        }
    }

    /// The fence's drain barrier (write side). Fence-only: it takes
    /// `extension_gate.write()` and never `extension_lock`, so it cannot close a
    /// lock cycle with an extender (invariant 4). Held by the fence across its
    /// load → persist → seed → publish sequence; in-flight extension readers
    /// drain before it is granted.
    pub(crate) async fn drain_barrier_write(&self) -> RwLockWriteGuard<'_, ()> {
        self.extension_gate.write().await
    }
}

/// RAII guard holding `extension_lock`. The only way to obtain the extension
/// read barrier, so `extension_lock → extension_gate.read()` ordering is a
/// compile-time guarantee (invariant 1).
pub(crate) struct ExtensionSlot<'a> {
    _lock: AsyncMutexGuard<'a, ()>,
    core: &'a ServingCore,
}

impl<'a> ExtensionSlot<'a> {
    /// Recheck-after-acquire: would the outer retry's `try_grant(now_ms, count)`
    /// now succeed (a peer extender already made room)?
    pub(crate) fn would_grant(&self, now_ms: u64, count: u32) -> bool {
        self.core.allocator.lock().would_grant(now_ms, count)
    }

    /// Compute the pre-extended ceiling to request from consensus, under a single
    /// allocator lock. Returns `Err(CoreError::NotLeader)` if leadership was lost
    /// (no epoch) between the outer fast gate and here — the caller surfaces that
    /// as a leader redirect. `try_prepare_window_extension` never itself returns
    /// `NotLeader`, so the two error classes stay disjoint.
    pub(crate) fn prepare_extension(
        &self,
        now_ms: u64,
        window_ahead_ms: u64,
    ) -> Result<(u64, Epoch), CoreError> {
        let allocator = self.core.allocator.lock();
        let Some(epoch) = allocator.epoch() else {
            return Err(CoreError::NotLeader);
        };
        let now_ms = PhysicalMs::try_new(now_ms)?;
        let requested = allocator
            .try_prepare_window_extension(now_ms, window_ahead_ms)?
            .get();
        Ok((requested, epoch))
    }

    /// The extension drain barrier (read side). Reachable only through the slot,
    /// so the `extension_lock → extension_gate` order cannot be inverted. The
    /// returned guard's lifetime is the slot's `'a` (not the `&self` borrow), so
    /// the caller can keep using the slot — e.g. call `prepare_extension` —
    /// while holding the barrier.
    pub(crate) async fn drain_barrier(&self) -> RwLockReadGuard<'a, ()> {
        let core = self.core;
        core.extension_gate.read().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn step_down_clears_and_publishes_not_serving_with_hint() {
        // Regression for #346: publish sites use `send_replace`, so a transition
        // lands even when no receiver is subscribed (`receiver_count == 0`). Were
        // a publish site to use `send`, this would be silently dropped and a
        // leaderless-then-leader core could stay NotServing forever.
        let core = ServingCore::new(Duration::from_secs(3));
        assert_eq!(core.state_tx.receiver_count(), 0);

        let hint = PeerEndpoint::try_from("new-leader:9000").unwrap();
        core.step_down(Some(hint.clone()), Some(Epoch(7)));

        match core.serving_state() {
            ServingState::NotServing {
                leader_endpoint,
                leader_epoch,
            } => {
                assert_eq!(leader_endpoint, Some(hint));
                assert_eq!(leader_epoch, Some(Epoch(7)));
            }
            ServingState::Serving => panic!("expected NotServing after step_down"),
        }
    }

    #[tokio::test]
    async fn step_down_does_not_take_the_gate() {
        // Invariant 3: step_down must NOT acquire `extension_gate.write()`. Hold
        // the read barrier on the current task; if step_down tried to take the
        // write lock it would deadlock here (single-threaded runtime, lock held
        // by us). Reaching the assertion proves it never touches the gate.
        let core = ServingCore::new(Duration::from_secs(3));
        let slot = core.extension_slot().await;
        let _barrier = slot.drain_barrier().await;

        core.step_down(None, None);

        assert!(matches!(
            core.serving_state(),
            ServingState::NotServing { .. }
        ));
    }

    #[tokio::test]
    async fn write_barrier_excludes_held_read_barrier() {
        // Invariants 1 & 4: the fence's write barrier must wait for in-flight
        // extension readers to drain. `try_write` is a deterministic probe of
        // that exclusion (no timing): it fails while a read barrier is held and
        // succeeds once it is released.
        let core = ServingCore::new(Duration::from_secs(3));
        let slot = core.extension_slot().await;
        let barrier = slot.drain_barrier().await;

        assert!(
            core.extension_gate.try_write().is_err(),
            "write barrier must be blocked while a read barrier is held"
        );

        drop(barrier);
        drop(slot);
        assert!(
            core.extension_gate.try_write().is_ok(),
            "write barrier must be free once the read barrier drains"
        );
    }

    #[tokio::test]
    async fn prepare_extension_without_leadership_is_not_leader() {
        // A fresh core is NotLeader (no epoch). prepare_extension must surface
        // that as `NotLeader` so the caller emits a redirect, and would_grant is
        // false.
        let core = ServingCore::new(Duration::from_secs(3));
        let slot = core.extension_slot().await;
        assert!(matches!(
            slot.prepare_extension(1, 1_000),
            Err(CoreError::NotLeader)
        ));
        assert!(!slot.would_grant(1, 1));
    }

    #[test]
    fn is_serving_tracks_state_in_lockstep_with_serving_state() {
        // The hot-path gate reads `is_serving` in place of cloning a
        // `ServingState`; it must agree with `serving_state`'s variant across
        // every transition. Fresh core is NotServing -> false; publish_serving
        // -> true; step_down -> false again.
        let core = ServingCore::new(Duration::from_secs(3));
        assert!(!core.is_serving(), "fresh core is NotServing");
        assert!(matches!(
            core.serving_state(),
            ServingState::NotServing { .. }
        ));

        core.publish_serving();
        assert!(
            core.is_serving(),
            "publish_serving must flip is_serving true"
        );
        assert!(matches!(core.serving_state(), ServingState::Serving));

        core.step_down(
            Some(PeerEndpoint::try_from("leader:9000").unwrap()),
            Some(Epoch(1)),
        );
        assert!(!core.is_serving(), "step_down must flip is_serving false");
    }

    #[test]
    fn try_grant_without_leadership_is_not_leader() {
        let core = ServingCore::new(Duration::from_secs(3));
        assert!(matches!(core.try_grant(1, 1), Err(CoreError::NotLeader)));
    }

    #[test]
    fn seed_then_try_grant_succeeds_at_seeded_epoch() {
        // try_on_leadership_gained(floor, ceiling, epoch) seeds a serveable
        // window; a grant at the floor returns timestamps stamped with the
        // seeded epoch.
        let core = ServingCore::new(Duration::from_secs(3));
        core.seed_on_leadership_gained(1_000, 5_000, Epoch(3))
            .expect("seed must succeed (ceiling >= floor)");
        let grant = core.try_grant(1_000, 1).expect("grant must succeed");
        assert_eq!(grant.epoch(), Epoch(3));
    }

    #[test]
    fn enter_fencing_clears_allocator_then_publishes_not_serving() {
        // The leadership-gain path enters the fence through this single method,
        // so the clear-before-publish order (invariant 2) cannot be inverted at
        // the call site. Seed a serveable window first so the clear is
        // observable, then assert both effects: NotServing with no leader hint,
        // and a now-cleared allocator (try_grant -> NotLeader).
        let core = ServingCore::new(Duration::from_secs(3));
        core.seed_on_leadership_gained(1_000, 5_000, Epoch(3))
            .expect("seed must succeed (ceiling >= floor)");
        assert!(core.try_grant(1_000, 1).is_ok(), "seeded core must grant");

        core.enter_fencing();

        assert!(
            matches!(
                core.serving_state(),
                ServingState::NotServing {
                    leader_endpoint: None,
                    leader_epoch: None,
                }
            ),
            "enter_fencing must publish NotServing with no leader hint"
        );
        assert!(
            matches!(core.try_grant(1_000, 1), Err(CoreError::NotLeader)),
            "enter_fencing must clear the allocator"
        );
    }
}
