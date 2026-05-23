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

//! The single command type replicated through the OmniPaxos log.
//!
//! Two variants:
//! - [`Advance`](HighWaterCommand::Advance) — bump the high-water to at least
//!   the given value. Apply is `max(prev, at_least)`.
//! - [`Barrier`](HighWaterCommand::Barrier) — no-op marker used to linearize
//!   reads. `current_high_water` appends a Barrier and reads the in-memory
//!   high-water once it decides; the explicit variant keeps the apply path
//!   from needing to compute `max(prev, 0)` for every read.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum HighWaterCommand {
    Advance { at_least: u64 },
    Barrier,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct HighWaterSnapshot {
    pub value: u64,
}

impl omnipaxos::storage::Entry for HighWaterCommand {
    type Snapshot = HighWaterSnapshot;
}

impl omnipaxos::storage::Snapshot<HighWaterCommand> for HighWaterSnapshot {
    fn create(entries: &[HighWaterCommand]) -> Self {
        let max = entries
            .iter()
            .filter_map(|command| match command {
                HighWaterCommand::Advance { at_least } => Some(*at_least),
                HighWaterCommand::Barrier => None,
            })
            .max()
            .unwrap_or(0);
        Self { value: max }
    }

    fn merge(&mut self, other: Self) {
        self.value = self.value.max(other.value);
    }

    fn use_snapshots() -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use omnipaxos::storage::Snapshot;

    #[test]
    fn snapshot_create_picks_max_advance() {
        let entries = vec![
            HighWaterCommand::Advance { at_least: 10 },
            HighWaterCommand::Barrier,
            HighWaterCommand::Advance { at_least: 30 },
            HighWaterCommand::Advance { at_least: 20 },
        ];
        let snap = HighWaterSnapshot::create(&entries);
        assert_eq!(snap.value, 30);
    }

    #[test]
    fn snapshot_create_on_empty_yields_zero() {
        let snap = HighWaterSnapshot::create(&[]);
        assert_eq!(snap.value, 0);
    }

    #[test]
    fn snapshot_merge_picks_higher_value() {
        let mut first = HighWaterSnapshot { value: 5 };
        let second = HighWaterSnapshot { value: 12 };
        first.merge(second);
        assert_eq!(first.value, 12);
    }

    #[test]
    fn snapshot_merge_keeps_higher_value() {
        let mut first = HighWaterSnapshot { value: 50 };
        let second = HighWaterSnapshot { value: 12 };
        first.merge(second);
        assert_eq!(first.value, 50);
    }

    #[test]
    fn barrier_does_not_affect_snapshot() {
        let snap = HighWaterSnapshot::create(&[HighWaterCommand::Barrier]);
        assert_eq!(snap.value, 0);
    }
}
