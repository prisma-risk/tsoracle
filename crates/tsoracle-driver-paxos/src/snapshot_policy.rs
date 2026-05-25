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

//! Compaction trigger policy.
//!
//! The apply task calls `should_snapshot(decided_idx)` after each
//! successful drain. A `true` return triggers `OmniPaxos::snapshot` on
//! the current `decided_idx`, which compacts the persistent log up to
//! that point.

/// Trigger a snapshot every N decided entries.
#[derive(Clone, Copy, Debug)]
pub struct SnapshotPolicy {
    every_n_decided: u64,
    last_snapshot_at: u64,
}

impl SnapshotPolicy {
    /// Build a policy that snapshots every `every_n_decided` entries.
    ///
    /// A value of 0 disables automatic snapshotting (the policy will
    /// always return `false`). The first snapshot fires when
    /// `decided_idx >= every_n_decided`.
    #[must_use]
    pub const fn every(every_n_decided: u64) -> Self {
        Self {
            every_n_decided,
            last_snapshot_at: 0,
        }
    }

    /// Disable automatic snapshotting.
    #[must_use]
    pub const fn disabled() -> Self {
        Self::every(0)
    }

    /// Rebase the snapshot baseline to `decided_idx`.
    ///
    /// `every` starts the baseline at 0, which is correct for a fresh log but
    /// wrong after a restart: a node reopening its log at a decided index far
    /// past `every_n_decided` would satisfy `decided_idx >= 0 + N` on its first
    /// post-recovery check and snapshot spuriously. Seeding the baseline to the
    /// recovered decided index makes the next snapshot fire `every_n_decided`
    /// entries past the restart point instead. Called once at recovery; a
    /// disabled policy is unaffected (it never fires regardless of baseline).
    pub fn rebase(&mut self, decided_idx: u64) {
        self.last_snapshot_at = decided_idx;
    }

    /// Decide whether to trigger a snapshot for the given decided index.
    ///
    /// Updates internal state to remember the last triggered index so
    /// subsequent calls don't fire on every drain.
    pub fn should_snapshot(&mut self, decided_idx: u64) -> bool {
        if self.every_n_decided == 0 {
            return false;
        }
        if decided_idx >= self.last_snapshot_at + self.every_n_decided {
            self.last_snapshot_at = decided_idx;
            true
        } else {
            false
        }
    }
}

impl Default for SnapshotPolicy {
    fn default() -> Self {
        Self::disabled()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_never_triggers() {
        let mut policy = SnapshotPolicy::disabled();
        assert!(!policy.should_snapshot(1));
        assert!(!policy.should_snapshot(1_000_000));
    }

    #[test]
    fn every_n_fires_at_multiples() {
        let mut policy = SnapshotPolicy::every(100);
        assert!(!policy.should_snapshot(50));
        assert!(policy.should_snapshot(100));
        assert!(!policy.should_snapshot(150));
        assert!(policy.should_snapshot(200));
    }

    #[test]
    fn every_n_advances_remembers_last_trigger() {
        let mut policy = SnapshotPolicy::every(10);
        assert!(policy.should_snapshot(10));
        // Same value again does not retrigger.
        assert!(!policy.should_snapshot(10));
        // Has to clear the threshold before next trigger.
        assert!(!policy.should_snapshot(15));
        assert!(policy.should_snapshot(20));
    }

    #[test]
    fn default_is_disabled() {
        let mut policy = SnapshotPolicy::default();
        assert!(!policy.should_snapshot(u64::MAX));
    }

    #[test]
    fn rebase_suppresses_spurious_first_trigger_after_restart() {
        // Simulates a restart at a decided index far past the interval. A
        // freshly-constructed `every(100)` has `last_snapshot_at = 0`, so its
        // first check would fire (5000 >= 0 + 100) and snapshot spuriously.
        // Rebasing the baseline to the recovered index suppresses that.
        let mut policy = SnapshotPolicy::every(100);
        policy.rebase(5_000);
        assert!(
            !policy.should_snapshot(5_000),
            "must not snapshot on the first decision after recovery",
        );
        assert!(!policy.should_snapshot(5_099));
        assert!(
            policy.should_snapshot(5_100),
            "the every-N cadence resumes relative to the rebased baseline",
        );
    }

    #[test]
    fn rebase_on_disabled_policy_stays_disabled() {
        let mut policy = SnapshotPolicy::disabled();
        policy.rebase(5_000);
        assert!(!policy.should_snapshot(u64::MAX));
    }
}
