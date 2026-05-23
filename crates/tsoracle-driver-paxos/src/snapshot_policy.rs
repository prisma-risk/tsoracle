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
}
