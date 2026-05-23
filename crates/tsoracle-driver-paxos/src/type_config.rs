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

/// Error returned by [`encode_epoch`] when a `Ballot` field falls outside the
/// range the 64-bit `Epoch` packing can represent without loss.
///
/// The fence in [`crate::PaxosDriver`] decides "same leader or not" by exact
/// `Epoch` equality, so a silent truncation that mapped two distinct ballots
/// to the same `Epoch` would let a superseded leader's commit pass the fence.
/// Encoding therefore refuses out-of-range inputs rather than truncating them.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum EpochEncodingError {
    /// `config_id` does not fit the 16 bits reserved for it in the `Epoch`.
    #[error("config_id {0} exceeds the 16-bit fence-encoding bound (max 65535)")]
    ConfigIdOutOfRange(u32),
    /// `pid` (node id) does not fit the 16 bits reserved for it in the `Epoch`.
    #[error("pid {0} exceeds the 16-bit fence-encoding bound (max 65535)")]
    PidOutOfRange(u64),
}

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
/// `priority` is intentionally not encoded: it is a static per-node
/// tiebreaker fully determined by `pid`, so `(config_id, n, pid)` already
/// identifies a leader-round uniquely.
///
/// The 64-bit `Epoch` cannot hold the full `Ballot` domain (`config_id` and
/// `n` are `u32`, `pid` is `u64`), so `config_id` and `pid` each get 16 bits.
/// Rather than silently truncate — which would collide distinct ballots and
/// defeat the equality fence in [`crate::PaxosDriver`] — out-of-range inputs
/// are rejected. Within the bound, monotonicity holds across reconfigurations
/// (`config_id` bumps dominate) and elections (`n` bumps dominate in a config).
///
/// # Errors
///
/// Returns [`EpochEncodingError`] if `config_id` or `pid` exceeds 65535.
pub fn encode_epoch(ballot: Ballot) -> Result<Epoch, EpochEncodingError> {
    if ballot.config_id > 0xFFFF {
        return Err(EpochEncodingError::ConfigIdOutOfRange(ballot.config_id));
    }
    if ballot.pid > 0xFFFF {
        return Err(EpochEncodingError::PidOutOfRange(ballot.pid));
    }
    let config = u64::from(ballot.config_id) << 48;
    let round = u64::from(ballot.n) << 16;
    Ok(Epoch(config | round | ballot.pid))
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

    fn encode(config_id: u32, n: u32, pid: u64) -> Epoch {
        encode_epoch(ballot(config_id, n, pid)).expect("in-bounds ballot")
    }

    #[test]
    fn encode_then_decode_round_trip() {
        let epoch = encode(7, 42, 3);
        let (config_id, n, pid) = decode_epoch(epoch);
        assert_eq!(config_id, 7);
        assert_eq!(n, 42);
        assert_eq!(pid, 3);
    }

    #[test]
    fn higher_config_id_dominates_lower_round() {
        let early = encode(1, u32::MAX, 5);
        let later = encode(2, 0, 5);
        assert!(
            later > early,
            "config_id bump must outrank a saturated round"
        );
    }

    #[test]
    fn higher_round_dominates_within_same_config() {
        let earlier = encode(1, 5, 9);
        let later = encode(1, 6, 1);
        assert!(later > earlier, "round bump must outrank a pid change");
    }

    #[test]
    fn distinct_pids_at_same_round_have_distinct_epochs() {
        assert_ne!(encode(1, 5, 2), encode(1, 5, 3));
    }

    #[test]
    fn priority_is_intentionally_excluded() {
        // priority is a static per-node tiebreaker fully determined by pid,
        // so two ballots that differ only in priority denote the same
        // leader-round and must encode identically. This is a deliberate
        // omission, distinct from the truncation bug on config_id/pid.
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
        assert_eq!(
            encode_epoch(with_priority).unwrap(),
            encode_epoch(without_priority).unwrap(),
        );
    }

    #[test]
    fn encode_epoch_rejects_oversized_config_id() {
        let err = encode_epoch(ballot(0x1_0000, 7, 1)).unwrap_err();
        assert_eq!(err, EpochEncodingError::ConfigIdOutOfRange(0x1_0000));
    }

    #[test]
    fn encode_epoch_rejects_oversized_pid() {
        let err = encode_epoch(ballot(1, 7, 0x1_0000)).unwrap_err();
        assert_eq!(err, EpochEncodingError::PidOutOfRange(0x1_0000));
    }

    #[test]
    fn encode_epoch_distinct_pids_no_longer_silently_collide() {
        // Before the fix, pid=1 and pid=65537 both masked to 1 and produced
        // the same Epoch. Now the in-bounds pid encodes and the oversized one
        // is rejected, so the two can never share a fence-distinguishing value.
        assert!(encode_epoch(ballot(1, 7, 1)).is_ok());
        assert_eq!(
            encode_epoch(ballot(1, 7, 65537)).unwrap_err(),
            EpochEncodingError::PidOutOfRange(65537),
        );
    }
}
