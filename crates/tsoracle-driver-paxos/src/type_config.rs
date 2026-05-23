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

/// A peer node in the paxos cluster, with the endpoint used to populate
/// `LeaderState::Follower::leader_endpoint` for follower-redirect hints.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PaxosPeer {
    pub node_id: u64,
    pub endpoint: String,
}

/// Pack a Ballot into the 64-bit Epoch space.
///
/// Layout (high bits to low):
/// - `config_id` → bits 48..64 (16 bits)
/// - `n` (round number) → bits 16..48 (32 bits)
/// - `pid` (node id) → bits 0..16 (16 bits)
///
/// The 16-bit truncation on `config_id` and `pid` is acceptable for
/// realistic tsoracle deployments (<65k reconfigurations and <65k node
/// ids). Monotonicity holds across reconfigurations (config_id bumps
/// dominate) and across elections (n bumps dominate within a config).
#[must_use]
pub fn encode_epoch(ballot: Ballot) -> Epoch {
    let config = u64::from(ballot.config_id & 0xFFFF) << 48;
    let round = u64::from(ballot.n) << 16;
    let pid = ballot.pid & 0xFFFF;
    Epoch(config | round | pid)
}

/// Inverse of [`encode_epoch`]; returns `(config_id, n, pid)`. The encoding
/// is lossy on `config_id` and `pid` if they exceed 16 bits, so this is
/// for diagnostics only — never use the returned values to reconstruct
/// the original Ballot for protocol decisions.
#[must_use]
pub fn decode_epoch(epoch: Epoch) -> (u32, u32, u64) {
    let raw = epoch.0;
    let config_id = ((raw >> 48) & 0xFFFF) as u32;
    let n = ((raw >> 16) & 0xFFFF_FFFF) as u32;
    let pid = raw & 0xFFFF;
    (config_id, n, pid)
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let original = ballot(7, 42, 3);
        let epoch = encode_epoch(original);
        let (config_id, n, pid) = decode_epoch(epoch);
        assert_eq!(config_id, 7);
        assert_eq!(n, 42);
        assert_eq!(pid, 3);
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
        let first = encode_epoch(ballot(1, 5, 2));
        let second = encode_epoch(ballot(1, 5, 3));
        assert_ne!(first, second);
    }

    #[test]
    fn priority_field_is_ignored() {
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
}
