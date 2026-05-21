// #[PerformanceCriticalPath]
//! The window allocator state machine. Sync, no I/O.

use crate::{Epoch, LOGICAL_MAX, PHYSICAL_MS_MAX, Timestamp};

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct WindowGrant {
    pub physical_ms: u64,
    pub logical_start: u32,
    pub count: u32,
    pub epoch: Epoch,
}

impl WindowGrant {
    pub fn first(&self) -> Timestamp {
        Timestamp::pack(self.physical_ms, self.logical_start)
    }
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
    #[error(
        "invalid leadership window: fence_floor {fence_floor} exceeds committed_ceiling {committed_ceiling}"
    )]
    InvalidLeadershipWindow {
        fence_floor: u64,
        committed_ceiling: u64,
    },
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

    /// Hot path. Issue `count` timestamps from the in-memory window.
    ///
    /// Returns `WindowExhausted` when the in-memory remainder cannot cover the request;
    /// the caller (typically the server) then runs prepare → persist → commit and retries.
    pub fn try_grant(&mut self, now_ms: u64, count: u32) -> Result<WindowGrant, CoreError> {
        if count == 0 {
            return Err(CoreError::InvalidCount(0));
        }
        if count > LOGICAL_MAX + 1 {
            return Err(CoreError::InvalidCount(count));
        }
        let State::Leader {
            epoch,
            committed_high_water,
            next_physical_ms,
            next_logical,
        } = &mut self.state
        else {
            return Err(CoreError::NotLeader);
        };

        // Advance physical_ms toward wall clock if ahead. next_physical_ms is
        // already at or above fence_floor, so a low now_ms simply leaves it there.
        if now_ms > *next_physical_ms {
            *next_physical_ms = now_ms;
            *next_logical = 0;
        }

        // If the current physical_ms cannot fit the request in its remaining
        // logical range, advance to the next physical_ms.
        if *next_logical as u64 + count as u64 > LOGICAL_MAX as u64 + 1 {
            *next_physical_ms += 1;
            *next_logical = 0;
        }

        if *next_physical_ms > PHYSICAL_MS_MAX {
            return Err(CoreError::PhysicalMsOutOfRange(*next_physical_ms));
        }

        // The fence: never issue a timestamp at a physical_ms above the committed
        // high-water. If we are at or past the bound, the caller must extend.
        if *next_physical_ms > *committed_high_water {
            return Err(CoreError::WindowExhausted);
        }

        let grant = WindowGrant {
            physical_ms: *next_physical_ms,
            logical_start: *next_logical,
            count,
            epoch: *epoch,
        };
        *next_logical += count;
        Ok(grant)
    }

    /// Non-mutating predicate: would `try_grant(now_ms, count)` succeed right
    /// now? Used by the server's extension single-flight to decide whether a
    /// peer extender has already added enough room, avoiding a redundant
    /// `persist_high_water` round-trip. Mirrors `try_grant`'s exhaustion check
    /// exactly — a coarser predicate would risk false positives (skip the
    /// extension, then fail the outer retry) for requests whose `count`
    /// straddles the window edge.
    pub fn would_grant(&self, now_ms: u64, count: u32) -> bool {
        if count == 0 || count > LOGICAL_MAX + 1 {
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

        let mut physical_ms = *next_physical_ms;
        let mut logical = *next_logical;
        if now_ms > physical_ms {
            physical_ms = now_ms;
            logical = 0;
        }
        if logical as u64 + count as u64 > LOGICAL_MAX as u64 + 1 {
            physical_ms += 1;
        }
        if physical_ms > PHYSICAL_MS_MAX {
            return false;
        }
        physical_ms <= *committed_high_water
    }

    /// Compute the high-water value the caller should durably persist before
    /// calling `try_commit_window_extension`. Does not mutate.
    ///
    /// Returns `max(committed_high_water + 1, now_ms) + ahead_ms`. The +1 on
    /// `committed_high_water` guarantees forward progress when wall clock is
    /// behind the persisted bound (rare, but possible after a clock-step-back).
    pub fn try_prepare_window_extension(
        &self,
        now_ms: u64,
        ahead_ms: u64,
    ) -> Result<u64, CoreError> {
        match &self.state {
            State::NotLeader => Ok(0),
            State::Leader {
                committed_high_water,
                ..
            } => {
                let floor = committed_high_water
                    .checked_add(1)
                    .ok_or(CoreError::PhysicalMsOutOfRange(*committed_high_water))?;
                let requested = core::cmp::max(floor, now_ms)
                    .checked_add(ahead_ms)
                    .ok_or(CoreError::PhysicalMsOutOfRange(u64::MAX))?;
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
    /// silently dropped. Combined with the server's drain barrier, this
    /// guarantees a late persist from epoch N cannot raise the durable bound
    /// observed by epoch N+M.
    pub fn try_commit_window_extension(
        &mut self,
        persisted_high_water: u64,
        expected_epoch: Epoch,
    ) -> Result<(), CoreError> {
        if persisted_high_water > PHYSICAL_MS_MAX {
            return Err(CoreError::PhysicalMsOutOfRange(persisted_high_water));
        }
        if let State::Leader {
            epoch,
            committed_high_water,
            ..
        } = &mut self.state
            && *epoch == expected_epoch
            && persisted_high_water > *committed_high_water
        {
            *committed_high_water = persisted_high_water;
        }
        Ok(())
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
    fn commit_then_try_grant_succeeds() {
        let mut allocator = Allocator::new();
        allocator
            .try_on_leadership_gained(1000, 1000, Epoch(7))
            .unwrap();
        let target = allocator.try_prepare_window_extension(1000, 3000).unwrap();
        allocator
            .try_commit_window_extension(target, Epoch(7))
            .unwrap();
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
        allocator
            .try_commit_window_extension(5000, Epoch(1))
            .unwrap();
        allocator
            .try_commit_window_extension(3000, Epoch(1))
            .unwrap(); // attempt to regress
        // try_grant up to physical_ms=5000 should still work.
        let grant = allocator.try_grant(4500, 1).unwrap();
        assert_eq!(grant.physical_ms, 4500);
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
        // A late persist from epoch 4 (the prior leader) — fenced out.
        allocator
            .try_commit_window_extension(9_999, Epoch(4))
            .unwrap();
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
        allocator
            .try_commit_window_extension(9_999, Epoch(1))
            .unwrap();
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
}
