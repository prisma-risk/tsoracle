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
//! - [`Advance`](HighWaterCommand::Advance) — advance the high-water to at least
//!   the [`AdvancePayload::at_least`] value. Apply is `max(prev, at_least)`. The
//!   payload is the cross-backend [`tsoracle_consensus::AdvancePayload`], shared
//!   with the openraft driver so the "advance" command carries one name and one
//!   field across backends.
//! - [`Barrier`](HighWaterCommand::Barrier) — identified no-op used to
//!   linearize reads. `current_high_water` mints a `(node, seq)` nonce,
//!   appends `Barrier { node, seq }`, and waits until the apply path
//!   observes that specific nonce — not merely until `decided_idx`
//!   advances. The nonce is required to distinguish "my read's barrier
//!   committed" from "some unrelated earlier entry committed" when the
//!   local node has prior-leader entries between its `decided_idx`
//!   snapshot and the appended barrier's log index.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tsoracle_consensus::AdvancePayload;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum HighWaterCommand {
    Advance(AdvancePayload),
    Barrier { node: u64, seq: u64 },
}

/// Compacted view of a log range. Carries the high-water value and the
/// per-appending-node ledger of barrier sequences observed in the range.
/// The latter is needed so a node that catches up via snapshot transfer
/// (rather than by streaming individual entries) still sees its own
/// barriers as "applied" — otherwise `current_high_water` would hang
/// after recovery, waiting on a seq it can never observe.
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct HighWaterSnapshot {
    pub value: u64,
    pub applied_barriers: HashMap<u64, u64>,
}

impl omnipaxos::storage::Entry for HighWaterCommand {
    type Snapshot = HighWaterSnapshot;
}

impl omnipaxos::storage::Snapshot<HighWaterCommand> for HighWaterSnapshot {
    fn create(entries: &[HighWaterCommand]) -> Self {
        let mut value = 0u64;
        let mut applied_barriers: HashMap<u64, u64> = HashMap::new();
        for command in entries {
            match command {
                HighWaterCommand::Advance(AdvancePayload { at_least }) => {
                    if *at_least > value {
                        value = *at_least;
                    }
                }
                HighWaterCommand::Barrier { node, seq } => {
                    let slot = applied_barriers.entry(*node).or_insert(0);
                    if *seq > *slot {
                        *slot = *seq;
                    }
                }
            }
        }
        Self {
            value,
            applied_barriers,
        }
    }

    fn merge(&mut self, other: Self) {
        self.value = self.value.max(other.value);
        for (node, seq) in other.applied_barriers {
            let slot = self.applied_barriers.entry(node).or_insert(0);
            if seq > *slot {
                *slot = seq;
            }
        }
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
            HighWaterCommand::Advance(AdvancePayload { at_least: 10 }),
            HighWaterCommand::Barrier { node: 1, seq: 1 },
            HighWaterCommand::Advance(AdvancePayload { at_least: 30 }),
            HighWaterCommand::Advance(AdvancePayload { at_least: 20 }),
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
        let mut first = HighWaterSnapshot {
            value: 5,
            ..Default::default()
        };
        let second = HighWaterSnapshot {
            value: 12,
            ..Default::default()
        };
        first.merge(second);
        assert_eq!(first.value, 12);
    }

    #[test]
    fn snapshot_merge_keeps_higher_value() {
        let mut first = HighWaterSnapshot {
            value: 50,
            ..Default::default()
        };
        let second = HighWaterSnapshot {
            value: 12,
            ..Default::default()
        };
        first.merge(second);
        assert_eq!(first.value, 50);
    }

    #[test]
    fn barrier_does_not_affect_snapshot_value() {
        let snap = HighWaterSnapshot::create(&[HighWaterCommand::Barrier { node: 42, seq: 7 }]);
        assert_eq!(snap.value, 0);
    }

    #[test]
    fn snapshot_create_folds_barriers_into_ledger() {
        let entries = vec![
            HighWaterCommand::Barrier { node: 1, seq: 3 },
            HighWaterCommand::Advance(AdvancePayload { at_least: 100 }),
            HighWaterCommand::Barrier { node: 1, seq: 5 },
            HighWaterCommand::Barrier { node: 2, seq: 9 },
        ];
        let snap = HighWaterSnapshot::create(&entries);
        assert_eq!(snap.value, 100);
        assert_eq!(snap.applied_barriers.get(&1).copied(), Some(5));
        assert_eq!(snap.applied_barriers.get(&2).copied(), Some(9));
    }

    #[test]
    fn snapshot_merge_takes_max_barrier_seq_per_node() {
        let mut first = HighWaterSnapshot {
            value: 0,
            applied_barriers: HashMap::from([(1, 5), (2, 3)]),
        };
        let second = HighWaterSnapshot {
            value: 0,
            applied_barriers: HashMap::from([(1, 7), (3, 2)]),
        };
        first.merge(second);
        assert_eq!(first.applied_barriers.get(&1).copied(), Some(7));
        assert_eq!(first.applied_barriers.get(&2).copied(), Some(3));
        assert_eq!(first.applied_barriers.get(&3).copied(), Some(2));
    }
}
