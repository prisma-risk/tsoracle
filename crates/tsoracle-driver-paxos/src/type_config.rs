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

//! OmniPaxos type aliases, peer topology, and `Epoch ↔ Ballot` encoding.
//!
//! The `Epoch` packing folds the round-changing fields of a `Ballot` into
//! the 64-bit Epoch type so the driver's fence check (compare the epoch a
//! client supplied against the epoch we currently observe) is a single
//! integer equality.

use omnipaxos::ballot_leader_election::Ballot;
use tsoracle_core::Epoch;

/// Pack a Ballot into the 128-bit Epoch space.
///
/// Layout (high bits to low):
/// - `config_id` → bits 96..128 (32 bits)
/// - `n` (round number) → bits 64..96 (32 bits)
/// - `pid` (node id) → bits 0..64 (64 bits)
///
/// The full Ballot identity (`config_id: u32`, `n: u32`, `pid: u64`) is 128
/// bits and fits exactly, so the encoding is lossless and total — no
/// truncation, no validation. Monotonicity holds across reconfigurations
/// (`config_id` dominates) and across elections (`n` dominates within a
/// config), which the fence's equality check and the client's monotone-forward
/// leader cache both rely on. `priority` is intentionally not encoded: it is a
/// static per-node tiebreaker fully determined by `pid`, so `(config_id, n,
/// pid)` already identifies a leader-round uniquely.
#[must_use]
pub fn encode_epoch(ballot: Ballot) -> Epoch {
    Epoch(
        (u128::from(ballot.config_id) << 96)
            | (u128::from(ballot.n) << 64)
            | u128::from(ballot.pid),
    )
}

/// Exact inverse of [`encode_epoch`]; returns `(config_id, n, pid)`.
#[must_use]
pub fn decode_epoch(epoch: Epoch) -> (u32, u32, u64) {
    let raw = epoch.0;
    let config_id = (raw >> 96) as u32;
    let n = (raw >> 64) as u32;
    let pid = raw as u64;
    (config_id, n, pid)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn ballot(config_id: u32, n: u32, pid: u64) -> Ballot {
        Ballot {
            config_id,
            n,
            priority: 0,
            pid,
        }
    }

    #[test]
    fn encode_then_decode_round_trip() {
        let (config_id, n, pid) = decode_epoch(encode_epoch(ballot(7, 42, 3)));
        assert_eq!((config_id, n, pid), (7, 42, 3));
    }

    #[test]
    fn encode_is_lossless_at_full_field_widths() {
        // Every field at its maximum must survive the round-trip; this is the
        // property the 64-bit packing could not provide.
        let (config_id, n, pid) = decode_epoch(encode_epoch(ballot(u32::MAX, u32::MAX, u64::MAX)));
        assert_eq!((config_id, n, pid), (u32::MAX, u32::MAX, u64::MAX));
    }

    #[test]
    fn higher_config_id_dominates_lower_round() {
        let early = encode_epoch(ballot(1, u32::MAX, 5));
        let later = encode_epoch(ballot(2, 0, 5));
        assert!(
            later > early,
            "config_id bump must outrank a saturated round"
        );
    }

    #[test]
    fn higher_round_dominates_within_same_config() {
        let earlier = encode_epoch(ballot(1, 5, 9));
        let later = encode_epoch(ballot(1, 6, 1));
        assert!(later > earlier, "round bump must outrank a pid change");
    }

    #[test]
    fn distinct_pids_at_same_round_have_distinct_epochs() {
        assert_ne!(encode_epoch(ballot(1, 5, 2)), encode_epoch(ballot(1, 5, 3)));
    }

    #[test]
    fn formerly_colliding_ballots_now_have_distinct_epochs() {
        // Under the 16-bit packing, pid 1 and pid 65537 both masked to 1, and
        // config_id 1 and 65537 both masked to 1. The 128-bit packing keeps
        // them distinct.
        assert_ne!(
            encode_epoch(ballot(1, 7, 1)),
            encode_epoch(ballot(1, 7, 65537))
        );
        assert_ne!(
            encode_epoch(ballot(1, 7, 2)),
            encode_epoch(ballot(65537, 7, 2))
        );
    }

    #[test]
    fn priority_is_intentionally_excluded() {
        // priority is a static per-node tiebreaker fully determined by pid, so
        // two ballots differing only in priority denote the same leader-round
        // and must encode identically.
        let with_priority = Ballot {
            config_id: 1,
            n: 5,
            priority: 99,
            pid: 2,
        };
        let without_priority = Ballot {
            config_id: 1,
            n: 5,
            priority: 0,
            pid: 2,
        };
        assert_eq!(encode_epoch(with_priority), encode_epoch(without_priority));
    }

    proptest! {
        // Lossless over the entire (config_id, n, pid) domain: decode recovers
        // every field exactly. This is the guarantee the 64-bit packing could
        // not make.
        #[test]
        fn encode_decode_is_lossless(
            config_id in any::<u32>(),
            n in any::<u32>(),
            pid in any::<u64>(),
        ) {
            let recovered = decode_epoch(encode_epoch(ballot(config_id, n, pid)));
            prop_assert_eq!(recovered, (config_id, n, pid));
        }

        // Order-preserving — and therefore injective: epoch ordering equals
        // lexicographic ordering of (config_id, n, pid), so distinct ballots
        // never collide and a later ballot always outranks an earlier one.
        #[test]
        fn encode_preserves_lexicographic_order(
            c1 in any::<u32>(), n1 in any::<u32>(), p1 in any::<u64>(),
            c2 in any::<u32>(), n2 in any::<u32>(), p2 in any::<u64>(),
        ) {
            let e1 = encode_epoch(ballot(c1, n1, p1));
            let e2 = encode_epoch(ballot(c2, n2, p2));
            prop_assert_eq!(e1.cmp(&e2), (c1, n1, p1).cmp(&(c2, n2, p2)));
        }
    }
}
