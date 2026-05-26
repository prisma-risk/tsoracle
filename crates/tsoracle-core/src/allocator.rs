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

// #[PerformanceCriticalPath]
//! The window allocator state machine. Sync, no I/O.

use crate::{Epoch, LOGICAL_MAX, PHYSICAL_MS_MAX, Timestamp};

/// A contiguous block of `count` timestamps starting at
/// `(physical_ms, logical_start)`, all sharing one leadership `epoch`.
///
/// Fields are private and the only public constructor is
/// [`try_new`](Self::try_new), which validates that every timestamp the grant
/// covers fits the packed 46-bit physical / 18-bit logical layout. A value of
/// this type is therefore proof that [`first`](Self::first) and
/// [`last`](Self::last) can pack without panicking — the in-range invariant is
/// guaranteed by the type, not by the one constructor that happens to build it.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct WindowGrant {
    physical_ms: u64,
    logical_start: u32,
    count: u32,
    epoch: Epoch,
}

impl WindowGrant {
    /// Construct a grant, checking that every timestamp it covers packs
    /// cleanly. This is the only way to build a `WindowGrant` outside this
    /// module, so a constructed value witnesses that `first`/`last` are
    /// infallible.
    ///
    /// Rejects `count == 0` (a grant covers at least one timestamp, and
    /// `last`'s `logical_start + count - 1` would underflow). The range check
    /// defers to [`Timestamp::try_pack`] on the *last* logical the grant emits:
    /// it is the single source of truth for the bit layout, and since
    /// `logical_start <= last_logical <= LOGICAL_MAX` validating the last
    /// boundary validates the first by implication.
    pub fn try_new(
        physical_ms: u64,
        logical_start: u32,
        count: u32,
        epoch: Epoch,
    ) -> Result<Self, CoreError> {
        if count == 0 {
            return Err(CoreError::InvalidCount(0));
        }
        let last_logical =
            logical_start
                .checked_add(count - 1)
                .ok_or(CoreError::LogicalRangeOutOfRange {
                    logical_start,
                    count,
                })?;
        Timestamp::try_pack(physical_ms, last_logical).map_err(|err| match err {
            crate::TimestampError::PhysicalMsOutOfRange { physical_ms, .. } => {
                CoreError::PhysicalMsOutOfRange(physical_ms)
            }
            crate::TimestampError::LogicalOutOfRange { .. } => CoreError::LogicalRangeOutOfRange {
                logical_start,
                count,
            },
        })?;
        Ok(WindowGrant {
            physical_ms,
            logical_start,
            count,
            epoch,
        })
    }

    pub fn physical_ms(&self) -> u64 {
        self.physical_ms
    }
    pub fn logical_start(&self) -> u32 {
        self.logical_start
    }
    pub fn count(&self) -> u32 {
        self.count
    }
    pub fn epoch(&self) -> Epoch {
        self.epoch
    }

    /// The first timestamp in the grant. Infallible: [`try_new`](Self::try_new)
    /// validated `(physical_ms, logical_start)` is in range, so `pack` cannot
    /// trip its `assert!`.
    pub fn first(&self) -> Timestamp {
        Timestamp::pack(self.physical_ms, self.logical_start)
    }
    /// The last timestamp in the grant. Infallible: [`try_new`](Self::try_new)
    /// validated `(physical_ms, logical_start + count - 1)` is in range (and
    /// `count >= 1`, so the subtraction cannot underflow), so `pack` cannot
    /// trip its `assert!`.
    pub fn last(&self) -> Timestamp {
        Timestamp::pack(self.physical_ms, self.logical_start + self.count - 1)
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CoreError {
    #[error("not leader")]
    NotLeader,
    #[error("window exhausted; caller must extend before retrying")]
    WindowExhausted,
    #[error("invalid count: {0}")]
    InvalidCount(u32),
    #[error("physical_ms {0} exceeds 46-bit maximum")]
    PhysicalMsOutOfRange(u64),
    #[error("logical range [{logical_start}, +{count}) exceeds the 18-bit logical field")]
    LogicalRangeOutOfRange { logical_start: u32, count: u32 },
    #[error(
        "invalid leadership window: fence_floor {fence_floor} exceeds committed_ceiling {committed_ceiling}"
    )]
    InvalidLeadershipWindow {
        fence_floor: u64,
        committed_ceiling: u64,
    },
    #[error(
        "window extension overflow: max(floor {floor}, now_ms {now_ms}) + ahead_ms {ahead_ms} exceeds u64::MAX"
    )]
    WindowExtensionOverflow {
        floor: u64,
        now_ms: u64,
        ahead_ms: u64,
    },
}

/// The result of a `try_commit_window_extension` that passed range validation.
///
/// A commit either raises the durable bound or is dropped for one of three
/// benign, expected reasons. Collapsing both into `Ok(())` left a caller that
/// just paid for a `persist_high_water` round-trip unable to tell "I raised the
/// bound" from "I silently dropped your durably-persisted value." This type
/// preserves the distinction so the server can log/metric the dropped commits —
/// a leading indicator of epoch churn or persist reordering.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum CommitOutcome {
    /// The durable bound advanced to this value.
    Applied(u64),
    /// The bound did not move; see [`IgnoreReason`] for why.
    Ignored(IgnoreReason),
}

/// Why a [`CommitOutcome::Ignored`] commit left the durable bound unchanged.
///
/// All three are benign and expected under normal failover: the epoch-fencing
/// design of `try_commit_window_extension` deliberately drops late commits from
/// a superseded epoch rather than erroring, and the monotonic bound rejects a
/// value that does not advance. They are kept apart so a caller can distinguish
/// epoch churn ([`NotLeader`](Self::NotLeader) / [`EpochMismatch`](Self::EpochMismatch))
/// from persist reordering ([`NotAdvanced`](Self::NotAdvanced)).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum IgnoreReason {
    /// The allocator is no longer a leader, so the commit has no window to raise.
    NotLeader,
    /// The allocator leads a different epoch than the commit targeted; the
    /// commit is a late persist from a superseded epoch, fenced out.
    EpochMismatch { expected: Epoch, current: Epoch },
    /// The allocator still leads the targeted epoch, but the persisted value did
    /// not exceed the current bound, so the monotonic bound rejects it.
    NotAdvanced { persisted: u64, committed: u64 },
}

#[derive(Debug)]
enum State {
    NotLeader,
    Leader {
        epoch: Epoch,
        /// Persisted upper bound: the allocator will not issue any timestamp with
        /// `physical_ms` greater than this without a fresh `try_commit_window_extension`.
        committed_high_water: u64,
        /// Next `physical_ms` we are willing to issue at. Initialized to
        /// `fence_floor` on leadership gain, then advances monotonically — never
        /// retreats below the fence even when `now_ms` is a past value.
        next_physical_ms: u64,
        /// Next logical counter within `next_physical_ms`.
        next_logical: u32,
    },
}

pub struct Allocator {
    state: State,
}

impl Allocator {
    pub fn new() -> Self {
        Allocator {
            state: State::NotLeader,
        }
    }

    /// Seed the allocator once the failover fence has durably persisted both
    /// the floor and the pre-extended ceiling.
    ///
    /// `fence_floor` is the first `physical_ms` the new leader may issue —
    /// the server sets it to `prior_high_water + 1` so the new leader's
    /// timestamps are strictly above any the prior leader could have issued.
    ///
    /// `committed_ceiling` is the pre-extended upper bound the server has
    /// already persisted (typically `fence_floor + window_ms`). It must
    /// satisfy `committed_ceiling >= fence_floor` so the allocator can serve
    /// `try_grant` immediately without an additional extension round-trip.
    pub fn try_on_leadership_gained(
        &mut self,
        fence_floor: u64,
        committed_ceiling: u64,
        epoch: Epoch,
    ) -> Result<(), CoreError> {
        if fence_floor > PHYSICAL_MS_MAX {
            return Err(CoreError::PhysicalMsOutOfRange(fence_floor));
        }
        if committed_ceiling > PHYSICAL_MS_MAX {
            return Err(CoreError::PhysicalMsOutOfRange(committed_ceiling));
        }
        if committed_ceiling < fence_floor {
            return Err(CoreError::InvalidLeadershipWindow {
                fence_floor,
                committed_ceiling,
            });
        }
        self.state = State::Leader {
            epoch,
            committed_high_water: committed_ceiling,
            next_physical_ms: fence_floor,
            next_logical: 0,
        };
        Ok(())
    }

    pub fn on_leadership_lost(&mut self) {
        self.state = State::NotLeader;
    }

    pub fn is_leader(&self) -> bool {
        matches!(self.state, State::Leader { .. })
    }

    pub fn epoch(&self) -> Option<Epoch> {
        match self.state {
            State::Leader { epoch, .. } => Some(epoch),
            State::NotLeader => None,
        }
    }

    /// Current committed high-water in physical-millisecond units, or `None`
    /// when not the leader. The high-water is the upper bound the allocator
    /// will not exceed without a fresh `try_commit_window_extension`.
    pub fn committed_high_water(&self) -> Option<u64> {
        match self.state {
            State::Leader {
                committed_high_water,
                ..
            } => Some(committed_high_water),
            State::NotLeader => None,
        }
    }

    /// Shared count guard for `try_grant` and `would_grant`, kept here so the
    /// two entry points cannot drift. The server's extension single-flight
    /// relies on `would_grant(now_ms, count) == true` implying the retry
    /// `try_grant(now_ms, count)` succeeds (see `service::extend_window`), so
    /// the set of rejected counts must be identical on both paths — splitting
    /// this check across both methods would let a future edit to one degrade
    /// the recheck into a spurious consensus round-trip (or worse).
    ///
    /// `count == 0` reuses the oversized case's `InvalidCount(count)`; since
    /// `count` is `0` there, the surfaced value matches the prior explicit
    /// `InvalidCount(0)`.
    fn validate_count(count: u32) -> Result<(), CoreError> {
        if count == 0 || count > LOGICAL_MAX + 1 {
            return Err(CoreError::InvalidCount(count));
        }
        Ok(())
    }

    /// Single source of truth for the window-advance simulation and its bounds
    /// checks, shared by `try_grant` and `would_grant`. Pure: it takes the
    /// relevant state fields by value and mutates nothing, so a failed
    /// projection cannot leave allocator state advanced.
    ///
    /// On success returns the `(physical_ms, logical_start)` the grant would
    /// occupy. The two failure variants are kept distinct so `try_grant` can
    /// surface the precise error its callers (and tests) expect;
    /// `would_grant` collapses both to `false` via `.is_ok()`.
    fn project_grant(
        next_physical_ms: u64,
        next_logical: u32,
        committed_high_water: u64,
        now_ms: u64,
        count: u32,
    ) -> Result<(u64, u32), CoreError> {
        let mut physical_ms = next_physical_ms;
        let mut logical = next_logical;

        // Advance physical_ms toward wall clock if ahead. next_physical_ms is
        // already at or above fence_floor, so a low now_ms simply leaves it there.
        if now_ms > physical_ms {
            physical_ms = now_ms;
            logical = 0;
        }

        // If the current physical_ms cannot fit the request in its remaining
        // logical range, advance to the next physical_ms.
        if logical as u64 + count as u64 > LOGICAL_MAX as u64 + 1 {
            physical_ms += 1;
            logical = 0;
        }

        if physical_ms > PHYSICAL_MS_MAX {
            return Err(CoreError::PhysicalMsOutOfRange(physical_ms));
        }

        // The fence: never issue a timestamp at a physical_ms above the committed
        // high-water. If we are at or past the bound, the caller must extend.
        if physical_ms > committed_high_water {
            return Err(CoreError::WindowExhausted);
        }

        Ok((physical_ms, logical))
    }

    /// Hot path. Issue `count` timestamps from the in-memory window.
    ///
    /// Returns `WindowExhausted` when the in-memory remainder cannot cover the request;
    /// the caller (typically the server) then runs prepare → persist → commit and retries.
    ///
    /// State is written back only on success: a failed grant (out-of-range or
    /// exhausted window) leaves `next_physical_ms`/`next_logical` untouched.
    pub fn try_grant(&mut self, now_ms: u64, count: u32) -> Result<WindowGrant, CoreError> {
        Self::validate_count(count)?;
        let State::Leader {
            epoch,
            committed_high_water,
            next_physical_ms,
            next_logical,
        } = &mut self.state
        else {
            return Err(CoreError::NotLeader);
        };

        let (physical_ms, logical_start) = Self::project_grant(
            *next_physical_ms,
            *next_logical,
            *committed_high_water,
            now_ms,
            count,
        )?;

        // project_grant already guarantees physical_ms and the logical range
        // are in bounds, so this construction never fails in practice — but
        // routing through the checked constructor propagates a CoreError rather
        // than panicking should that invariant ever be violated, and keeps this
        // path free of the unwrap/expect the crate's panic policy bans.
        let grant = WindowGrant::try_new(physical_ms, logical_start, count, *epoch)?;
        // Normalize the write-back so the stored cursor is always a packable
        // (physical_ms, logical) pair. `project_grant` admits a grant that fills
        // the millisecond exactly, so `logical_start + count` can reach
        // LOGICAL_MAX + 1 — a logical the packed layout cannot hold. Rather than
        // store that sentinel and rely on the next `project_grant` call to wrap
        // it, roll to the next millisecond at logical 0 here. This is precisely
        // the position the next call would compute, so behavior is unchanged;
        // the stored state just no longer depends on the implicit
        // "next call always wraps or resets" invariant. (`next_physical_ms` may
        // become PHYSICAL_MS_MAX + 1, which is never packed directly and is
        // rejected as out-of-range by the next grant's `project_grant`.)
        let next_logical_after = logical_start + count;
        if next_logical_after > LOGICAL_MAX {
            *next_physical_ms = physical_ms + 1;
            *next_logical = 0;
        } else {
            *next_physical_ms = physical_ms;
            *next_logical = next_logical_after;
        }
        Ok(grant)
    }

    /// Non-mutating predicate: would `try_grant(now_ms, count)` succeed right
    /// now? Used by the server's extension single-flight to decide whether a
    /// peer extender has already added enough room, avoiding a redundant
    /// `persist_high_water` round-trip. Delegates to the same `project_grant`
    /// helper `try_grant` uses, so the exhaustion check cannot drift — a
    /// coarser predicate would risk false positives (skip the extension, then
    /// fail the outer retry) for requests whose `count` straddles the window edge.
    pub fn would_grant(&self, now_ms: u64, count: u32) -> bool {
        if Self::validate_count(count).is_err() {
            return false;
        }
        let State::Leader {
            committed_high_water,
            next_physical_ms,
            next_logical,
            ..
        } = &self.state
        else {
            return false;
        };

        Self::project_grant(
            *next_physical_ms,
            *next_logical,
            *committed_high_water,
            now_ms,
            count,
        )
        .is_ok()
    }

    /// Compute the high-water value the caller should durably persist before
    /// calling `try_commit_window_extension`. Does not mutate.
    ///
    /// Returns `max(committed_high_water + 1, now_ms) + ahead_ms`. The +1 on
    /// `committed_high_water` guarantees forward progress when wall clock is
    /// behind the persisted bound (rare, but possible after a clock-step-back).
    ///
    /// Returns `Err(CoreError::NotLeader)` off-leader, matching every other
    /// mutating method. A `0` sentinel here would be indistinguishable from a
    /// legitimately prepared bound, letting a caller that skipped `is_leader()`
    /// proceed as if preparation had succeeded.
    pub fn try_prepare_window_extension(
        &self,
        now_ms: u64,
        ahead_ms: u64,
    ) -> Result<u64, CoreError> {
        match &self.state {
            State::NotLeader => Err(CoreError::NotLeader),
            State::Leader {
                committed_high_water,
                ..
            } => {
                let floor = committed_high_water
                    .checked_add(1)
                    .ok_or(CoreError::PhysicalMsOutOfRange(*committed_high_water))?;
                let requested = core::cmp::max(floor, now_ms).checked_add(ahead_ms).ok_or(
                    CoreError::WindowExtensionOverflow {
                        floor,
                        now_ms,
                        ahead_ms,
                    },
                )?;
                if requested > PHYSICAL_MS_MAX {
                    return Err(CoreError::PhysicalMsOutOfRange(requested));
                }
                Ok(requested)
            }
        }
    }

    /// Apply a durably-persisted window extension. `persisted_high_water` is
    /// the value returned by `ConsensusDriver::persist_high_water`, which is
    /// monotonic — it may equal or exceed the value passed to prepare.
    ///
    /// The `expected_epoch` argument fences out late-arriving commits from a
    /// prior leader epoch: if the allocator is no longer at this epoch (either
    /// it has lost leadership or a new leader took over), the commit is
    /// dropped. Combined with the server's drain barrier, this guarantees a
    /// late persist from epoch N cannot raise the durable bound observed by
    /// epoch N+M.
    ///
    /// Returns [`CommitOutcome`]: `Applied` when the bound advanced, or
    /// `Ignored` (with the reason) for the three benign drop cases. A value
    /// exceeding the 46-bit physical ceiling is an invariant violation, not a
    /// benign drop, so it stays `Err(PhysicalMsOutOfRange)`.
    pub fn try_commit_window_extension(
        &mut self,
        persisted_high_water: u64,
        expected_epoch: Epoch,
    ) -> Result<CommitOutcome, CoreError> {
        if persisted_high_water > PHYSICAL_MS_MAX {
            return Err(CoreError::PhysicalMsOutOfRange(persisted_high_water));
        }
        let State::Leader {
            epoch,
            committed_high_water,
            ..
        } = &mut self.state
        else {
            return Ok(CommitOutcome::Ignored(IgnoreReason::NotLeader));
        };
        // Epoch fencing takes precedence over the monotonic check: a late
        // persist from a superseded epoch must report EpochMismatch even when
        // its value also fails to advance, so churn is not masked as reordering.
        if *epoch != expected_epoch {
            return Ok(CommitOutcome::Ignored(IgnoreReason::EpochMismatch {
                expected: expected_epoch,
                current: *epoch,
            }));
        }
        if persisted_high_water <= *committed_high_water {
            return Ok(CommitOutcome::Ignored(IgnoreReason::NotAdvanced {
                persisted: persisted_high_water,
                committed: *committed_high_water,
            }));
        }
        *committed_high_water = persisted_high_water;
        Ok(CommitOutcome::Applied(persisted_high_water))
    }
}

impl Default for Allocator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_allocator_is_not_leader() {
        let allocator = Allocator::new();
        assert!(!allocator.is_leader());
        assert_eq!(allocator.epoch(), None);
    }

    #[test]
    fn on_leadership_gained_sets_epoch() {
        let mut allocator = Allocator::new();
        allocator
            .try_on_leadership_gained(1000, 5000, Epoch(5))
            .unwrap();
        assert!(allocator.is_leader());
        assert_eq!(allocator.epoch(), Some(Epoch(5)));
    }

    #[test]
    fn try_on_leadership_gained_rejects_out_of_range_window() {
        let mut allocator = Allocator::new();
        assert_eq!(
            allocator.try_on_leadership_gained(PHYSICAL_MS_MAX + 1, PHYSICAL_MS_MAX + 1, Epoch(5)),
            Err(CoreError::PhysicalMsOutOfRange(PHYSICAL_MS_MAX + 1))
        );
        // fence_floor in-range, ceiling out-of-range — separate guard.
        assert_eq!(
            allocator.try_on_leadership_gained(1_000, PHYSICAL_MS_MAX + 1, Epoch(5)),
            Err(CoreError::PhysicalMsOutOfRange(PHYSICAL_MS_MAX + 1))
        );
        assert_eq!(
            allocator.try_on_leadership_gained(5000, 4000, Epoch(5)),
            Err(CoreError::InvalidLeadershipWindow {
                fence_floor: 5000,
                committed_ceiling: 4000
            })
        );
    }

    #[test]
    fn on_leadership_lost_clears_state() {
        let mut allocator = Allocator::new();
        allocator
            .try_on_leadership_gained(1000, 5000, Epoch(5))
            .unwrap();
        allocator.on_leadership_lost();
        assert!(!allocator.is_leader());
        assert_eq!(allocator.epoch(), None);
    }

    #[test]
    fn committed_high_water_tracks_leader_state_and_extensions() {
        let mut allocator = Allocator::new();
        assert_eq!(allocator.committed_high_water(), None);

        allocator
            .try_on_leadership_gained(1_000, 5_000, Epoch(1))
            .unwrap();
        assert_eq!(allocator.committed_high_water(), Some(5_000));

        let target = allocator
            .try_prepare_window_extension(2_000, 3_000)
            .unwrap();
        allocator
            .try_commit_window_extension(target, Epoch(1))
            .unwrap();
        assert_eq!(allocator.committed_high_water(), Some(target));

        allocator.on_leadership_lost();
        assert_eq!(allocator.committed_high_water(), None);
    }

    #[test]
    fn try_grant_not_leader() {
        let mut allocator = Allocator::new();
        assert_eq!(allocator.try_grant(1000, 1), Err(CoreError::NotLeader));
    }

    #[test]
    fn try_grant_zero_count() {
        let mut allocator = Allocator::new();
        allocator
            .try_on_leadership_gained(1000, 5000, Epoch(1))
            .unwrap();
        assert_eq!(
            allocator.try_grant(1000, 0),
            Err(CoreError::InvalidCount(0))
        );
    }

    #[test]
    fn try_grant_oversized_count() {
        let mut allocator = Allocator::new();
        allocator
            .try_on_leadership_gained(1000, 5000, Epoch(1))
            .unwrap();
        let oversized = LOGICAL_MAX + 2;
        assert_eq!(
            allocator.try_grant(1000, oversized),
            Err(CoreError::InvalidCount(oversized))
        );
    }

    #[test]
    fn try_grant_above_committed_is_window_exhausted() {
        // Advancing `now_ms` past `committed_high_water` correctly returns
        // WindowExhausted; the server then extends.
        let mut allocator = Allocator::new();
        // fence_floor=5_000, ceiling=5_000 (tight window, no pre-extended gap).
        allocator
            .try_on_leadership_gained(5_000, 5_000, Epoch(1))
            .unwrap();
        // now_ms below floor: clamps to floor=5_000, which equals the ceiling → succeeds.
        allocator.try_grant(4_999, 1).unwrap();
        // now_ms above ceiling: window exhausted.
        assert_eq!(
            allocator.try_grant(5_001, 1),
            Err(CoreError::WindowExhausted)
        );
    }

    #[test]
    fn failed_try_grant_does_not_advance_state() {
        // A grant that fails the exhaustion check must leave the allocator's
        // advance state untouched, so a later grant at a lower `now_ms` is not
        // pinned to the failed attempt's wall clock.
        let mut allocator = Allocator::new();
        // Tight initial window: fence_floor == ceiling == 1_000.
        allocator
            .try_on_leadership_gained(1_000, 1_000, Epoch(1))
            .unwrap();
        // now_ms far past the ceiling exhausts the window.
        assert_eq!(
            allocator.try_grant(5_000, 1),
            Err(CoreError::WindowExhausted)
        );
        // Extend the durable bound to exactly 2_000.
        let target = allocator.try_prepare_window_extension(2_000, 0).unwrap();
        assert_eq!(target, 2_000); // max(committed+1=1_001, now=2_000) + 0
        allocator
            .try_commit_window_extension(target, Epoch(1))
            .unwrap();
        // The failed grant must not have pinned next_physical_ms at 5_000: a
        // grant at now_ms=2_000 advances cleanly to physical_ms=2_000 (<= the
        // committed 2_000). If state had advanced on the failure, next_physical_ms
        // would still be 5_000 and this would exhaust the window again.
        let grant = allocator.try_grant(2_000, 1).unwrap();
        assert_eq!(grant.physical_ms, 2_000);
        assert_eq!(grant.logical_start, 0);
    }

    #[test]
    fn try_grant_after_gain_serves_immediately() {
        // The fence has already persisted a pre-extended window, so the allocator
        // can serve immediately. Grants start at fence_floor regardless of now_ms.
        let mut allocator = Allocator::new();
        allocator
            .try_on_leadership_gained(5_000, 10_000, Epoch(1))
            .unwrap();
        let grant = allocator.try_grant(1_000, 1).unwrap();
        // now_ms=1_000 < fence_floor=5_000, so next_physical_ms stays at 5_000.
        assert_eq!(grant.physical_ms, 5_000);
        assert_eq!(grant.logical_start, 0);
        assert_eq!(grant.epoch, Epoch(1));
    }

    #[test]
    fn prepare_window_extension_not_leader() {
        // Off-leader prepare must error like every other mutating method, not
        // return a `0` that a caller could mistake for a prepared bound.
        let allocator = Allocator::new();
        assert_eq!(
            allocator.try_prepare_window_extension(1000, 3000),
            Err(CoreError::NotLeader)
        );
    }

    #[test]
    fn prepare_window_extension_uses_now_ms_when_ahead_of_high_water() {
        let mut allocator = Allocator::new();
        allocator
            .try_on_leadership_gained(1000, 1000, Epoch(1))
            .unwrap();
        let target = allocator.try_prepare_window_extension(2000, 3000).unwrap();
        assert_eq!(target, 5000); // max(1001, 2000) + 3000
    }

    #[test]
    fn prepare_window_extension_uses_high_water_floor_when_clock_behind() {
        let mut allocator = Allocator::new();
        allocator
            .try_on_leadership_gained(10_000, 10_000, Epoch(1))
            .unwrap();
        let target = allocator.try_prepare_window_extension(500, 3000).unwrap();
        // floor = 10_001, clock = 500. max = 10_001. + 3000 = 13_001.
        assert_eq!(target, 13_001);
    }

    #[test]
    fn prepare_window_extension_rejects_out_of_range_target() {
        let mut allocator = Allocator::new();
        allocator
            .try_on_leadership_gained(PHYSICAL_MS_MAX, PHYSICAL_MS_MAX, Epoch(1))
            .unwrap();
        assert_eq!(
            allocator.try_prepare_window_extension(PHYSICAL_MS_MAX, 1),
            Err(CoreError::PhysicalMsOutOfRange(PHYSICAL_MS_MAX + 2))
        );
    }

    #[test]
    fn prepare_window_extension_overflow_names_all_operands() {
        // A saturated clock (SystemClock::now_ms saturates to u64::MAX) plus any
        // non-zero ahead_ms overflows max(floor, now_ms) + ahead_ms. The error
        // must name the three real operands so the log points at the clock, not
        // a phantom "someone passed an absurd physical_ms".
        let mut allocator = Allocator::new();
        allocator
            .try_on_leadership_gained(1_000, 1_000, Epoch(1))
            .unwrap();
        assert_eq!(
            allocator.try_prepare_window_extension(u64::MAX, 1),
            Err(CoreError::WindowExtensionOverflow {
                floor: 1_001,
                now_ms: u64::MAX,
                ahead_ms: 1,
            })
        );
    }

    #[test]
    fn commit_then_try_grant_succeeds() {
        let mut allocator = Allocator::new();
        allocator
            .try_on_leadership_gained(1000, 1000, Epoch(7))
            .unwrap();
        let target = allocator.try_prepare_window_extension(1000, 3000).unwrap();
        assert_eq!(
            allocator.try_commit_window_extension(target, Epoch(7)),
            Ok(CommitOutcome::Applied(target))
        );
        let grant = allocator.try_grant(1000, 5).unwrap();
        assert_eq!(grant.count, 5);
        assert_eq!(grant.logical_start, 0);
        assert_eq!(grant.epoch, Epoch(7));
        // physical_ms should be at most the persisted high-water.
        assert!(grant.physical_ms <= target);
    }

    #[test]
    fn commit_with_lower_value_is_ignored() {
        let mut allocator = Allocator::new();
        allocator
            .try_on_leadership_gained(1000, 1000, Epoch(1))
            .unwrap();
        assert_eq!(
            allocator.try_commit_window_extension(5000, Epoch(1)),
            Ok(CommitOutcome::Applied(5000))
        );
        // A non-advancing commit reports the values so the caller can tell a
        // monotonic-bound regression (persist reordering) from epoch churn.
        assert_eq!(
            allocator.try_commit_window_extension(3000, Epoch(1)),
            Ok(CommitOutcome::Ignored(IgnoreReason::NotAdvanced {
                persisted: 3000,
                committed: 5000,
            }))
        );
        // try_grant up to physical_ms=5000 should still work.
        let grant = allocator.try_grant(4500, 1).unwrap();
        assert_eq!(grant.physical_ms, 4500);
    }

    #[test]
    fn commit_with_equal_value_is_ignored_not_applied() {
        // persist_high_water is monotonic and may *equal* the prepared bound; an
        // equal value moves nothing, so it is Ignored(NotAdvanced), not Applied.
        let mut allocator = Allocator::new();
        allocator
            .try_on_leadership_gained(1000, 5000, Epoch(1))
            .unwrap();
        assert_eq!(
            allocator.try_commit_window_extension(5000, Epoch(1)),
            Ok(CommitOutcome::Ignored(IgnoreReason::NotAdvanced {
                persisted: 5000,
                committed: 5000,
            }))
        );
    }

    #[test]
    fn commit_rejects_out_of_range_high_water() {
        let mut allocator = Allocator::new();
        allocator
            .try_on_leadership_gained(1000, 1000, Epoch(1))
            .unwrap();
        assert_eq!(
            allocator.try_commit_window_extension(PHYSICAL_MS_MAX + 1, Epoch(1)),
            Err(CoreError::PhysicalMsOutOfRange(PHYSICAL_MS_MAX + 1))
        );
    }

    #[test]
    fn try_grant_rejects_out_of_range_clock() {
        let mut allocator = Allocator::new();
        allocator
            .try_on_leadership_gained(1000, PHYSICAL_MS_MAX, Epoch(1))
            .unwrap();
        assert_eq!(
            allocator.try_grant(PHYSICAL_MS_MAX + 1, 1),
            Err(CoreError::PhysicalMsOutOfRange(PHYSICAL_MS_MAX + 1))
        );
    }

    #[test]
    fn commit_at_wrong_epoch_is_silently_dropped() {
        let mut allocator = Allocator::new();
        // fence_floor=1000, ceiling=1000: tight initial window.
        allocator
            .try_on_leadership_gained(1000, 1000, Epoch(5))
            .unwrap();
        // A late persist from epoch 4 (the prior leader) — fenced out. The
        // outcome names both epochs so the caller can metric epoch churn.
        assert_eq!(
            allocator.try_commit_window_extension(9_999, Epoch(4)),
            Ok(CommitOutcome::Ignored(IgnoreReason::EpochMismatch {
                expected: Epoch(4),
                current: Epoch(5),
            }))
        );
        // The allocator's bound did not move; a grant at now=900 clamps to
        // floor=1000, and a request with now=1_100 exhausts the window.
        allocator.try_grant(900, 1).unwrap();
        assert_eq!(
            allocator.try_grant(1_100, 1),
            Err(CoreError::WindowExhausted)
        );
    }

    #[test]
    fn commit_after_leadership_lost_is_ignored() {
        let mut allocator = Allocator::new();
        allocator
            .try_on_leadership_gained(1000, 5000, Epoch(1))
            .unwrap();
        allocator.on_leadership_lost();
        assert_eq!(
            allocator.try_commit_window_extension(9_999, Epoch(1)),
            Ok(CommitOutcome::Ignored(IgnoreReason::NotLeader))
        );
        assert!(!allocator.is_leader());
    }

    #[test]
    fn would_grant_matches_try_grant_outcome() {
        let mut allocator = Allocator::new();
        // Not leader: never grants.
        assert!(!allocator.would_grant(1_000, 1));
        // Invalid counts: never grants.
        allocator
            .try_on_leadership_gained(1_000, 5_000, Epoch(1))
            .unwrap();
        assert!(!allocator.would_grant(1_000, 0));
        assert!(!allocator.would_grant(1_000, LOGICAL_MAX + 2));
        // Within-window: matches try_grant. now_ms below floor still grants
        // (clamped to floor=1_000, ceiling=5_000).
        assert!(allocator.would_grant(0, 1));
        // now_ms above ceiling: predicate refuses (would exhaust).
        assert!(!allocator.would_grant(5_001, 1));
        // Mid-window now_ms advances the predicate's internal physical_ms.
        assert!(allocator.would_grant(2_500, 1));
    }

    #[test]
    fn would_grant_predicts_logical_wrap_advance() {
        // When (logical + count) overflows the per-ms logical range, the
        // predicate (like try_grant) advances physical_ms by 1. If that
        // advance leaves the window, would_grant must return false.
        let mut allocator = Allocator::new();
        allocator
            .try_on_leadership_gained(1_000, 1_000, Epoch(1))
            .unwrap();
        // count >= LOGICAL_MAX + 1 forces the advance branch on a fresh
        // window: logical(0) + count(LOGICAL_MAX+1) doesn't overflow on its
        // own, but anything one bigger does. Use LOGICAL_MAX + 1 to land at
        // the edge, then any non-zero issue advances physical_ms.
        allocator.try_grant(1_000, LOGICAL_MAX + 1).unwrap();
        // Next grant of size 1 would advance to physical_ms = 1_001, which
        // exceeds the committed ceiling of 1_000.
        assert!(!allocator.would_grant(1_000, 1));
    }

    #[test]
    fn would_grant_returns_false_when_advance_exceeds_physical_max() {
        // Construct an allocator at PHYSICAL_MS_MAX so the +1 advance
        // crosses the 46-bit ceiling and the predicate refuses.
        let mut allocator = Allocator::new();
        allocator
            .try_on_leadership_gained(PHYSICAL_MS_MAX, PHYSICAL_MS_MAX, Epoch(1))
            .unwrap();
        // Fill the logical range so the next would_grant call has to
        // advance physical_ms.
        allocator
            .try_grant(PHYSICAL_MS_MAX, LOGICAL_MAX + 1)
            .unwrap();
        assert!(!allocator.would_grant(PHYSICAL_MS_MAX, 1));
    }

    #[test]
    fn default_constructs_not_leader_allocator() {
        let allocator = Allocator::default();
        assert!(!allocator.is_leader());
        assert_eq!(allocator.epoch(), None);
    }

    #[test]
    fn logical_wraps_to_next_physical_ms() {
        let mut allocator = Allocator::new();
        // fence_floor=0, ceiling=0; extend to 10 before granting.
        allocator.try_on_leadership_gained(0, 0, Epoch(1)).unwrap();
        allocator.try_commit_window_extension(10, Epoch(1)).unwrap();
        // Issue LOGICAL_MAX+1 logicals at physical_ms=1, then one more should bump to 2.
        let first = allocator.try_grant(1, LOGICAL_MAX + 1).unwrap();
        assert_eq!(first.physical_ms, 1);
        assert_eq!(first.logical_start, 0);
        let second = allocator.try_grant(1, 1).unwrap();
        assert_eq!(second.physical_ms, 2);
        assert_eq!(second.logical_start, 0);
    }

    #[test]
    fn exact_fill_grant_normalizes_stored_state_to_packable() {
        // A grant that consumes a millisecond's entire logical range
        // (logical_start + count == LOGICAL_MAX + 1) must not leave the
        // LOGICAL_MAX+1 sentinel in next_logical: that value cannot be packed
        // (Timestamp::pack asserts logical <= LOGICAL_MAX), so the stored state
        // would only be safe by the implicit "next call always wraps" invariant.
        // The write-back normalizes it to the already-rolled position
        // (physical_ms + 1, 0), so stored state is always directly packable.
        let mut allocator = Allocator::new();
        allocator.try_on_leadership_gained(0, 0, Epoch(1)).unwrap();
        allocator.try_commit_window_extension(10, Epoch(1)).unwrap();
        // Fill physical_ms=1 exactly: logical [0, LOGICAL_MAX].
        let grant = allocator.try_grant(1, LOGICAL_MAX + 1).unwrap();
        assert_eq!(grant.physical_ms, 1);
        assert_eq!(grant.logical_start, 0);

        let State::Leader {
            next_physical_ms,
            next_logical,
            ..
        } = allocator.state
        else {
            panic!("expected leader state after a successful grant");
        };
        // The stored cursor rolled to the next millisecond at logical 0 …
        assert_eq!(next_physical_ms, 2);
        assert_eq!(next_logical, 0);
        // … and is, by construction, a packable timestamp.
        assert!(Timestamp::try_pack(next_physical_ms, next_logical).is_ok());
    }

    #[test]
    fn try_new_accepts_valid_grant_and_packs_boundaries() {
        // A checked grant exposes its fields through accessors and its
        // boundary timestamps pack without panicking — first() at
        // logical_start, last() at logical_start + count - 1.
        let grant = WindowGrant::try_new(1_000, 5, 3, Epoch(7)).unwrap();
        assert_eq!(grant.physical_ms(), 1_000);
        assert_eq!(grant.logical_start(), 5);
        assert_eq!(grant.count(), 3);
        assert_eq!(grant.epoch(), Epoch(7));
        assert_eq!(grant.first(), Timestamp::pack(1_000, 5));
        assert_eq!(grant.last(), Timestamp::pack(1_000, 7));
    }

    #[test]
    fn try_new_accepts_max_logical_boundary() {
        // logical_start + count - 1 == LOGICAL_MAX is the widest in-range grant.
        let grant = WindowGrant::try_new(1_000, 0, LOGICAL_MAX + 1, Epoch(1)).unwrap();
        assert_eq!(grant.last(), Timestamp::pack(1_000, LOGICAL_MAX));
    }

    #[test]
    fn try_new_rejects_zero_count() {
        // count == 0 would underflow logical_start + count - 1 in last().
        assert_eq!(
            WindowGrant::try_new(1_000, 0, 0, Epoch(1)),
            Err(CoreError::InvalidCount(0))
        );
    }

    #[test]
    fn try_new_rejects_out_of_range_physical_ms() {
        assert_eq!(
            WindowGrant::try_new(PHYSICAL_MS_MAX + 1, 0, 1, Epoch(1)),
            Err(CoreError::PhysicalMsOutOfRange(PHYSICAL_MS_MAX + 1))
        );
    }

    #[test]
    fn try_new_rejects_logical_range_overflow() {
        // last logical (logical_start + count - 1) exceeds the 18-bit field.
        assert_eq!(
            WindowGrant::try_new(1_000, LOGICAL_MAX, 2, Epoch(1)),
            Err(CoreError::LogicalRangeOutOfRange {
                logical_start: LOGICAL_MAX,
                count: 2
            })
        );
    }

    #[test]
    fn try_new_rejects_logical_count_u32_overflow() {
        // logical_start + (count - 1) overflows u32 before any range check.
        assert_eq!(
            WindowGrant::try_new(1_000, u32::MAX, 2, Epoch(1)),
            Err(CoreError::LogicalRangeOutOfRange {
                logical_start: u32::MAX,
                count: 2
            })
        );
    }
}
