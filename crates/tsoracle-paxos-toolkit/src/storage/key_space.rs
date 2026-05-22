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

//! Key-space layout for the toolkit's RocksDB-backed Storage impl.
//!
//! All OmniPaxos persistent state lives in a single column family using two
//! byte-prefix namespaces:
//!
//! * `L/<u64 BE>` — log entries, indexed by their position
//! * `M/<name>`   — metadata: promise, accepted_round, decided_idx,
//!   compacted_idx, snapshot, stopsign
//!
//! Big-endian log indices preserve log order under RocksDB's lexicographic
//! iteration. Single-CF layout (rather than separate CFs per concern) lets
//! the Storage impl batch log + decided-idx writes in one WriteBatch, which
//! OmniPaxos's `append_on_prefix` semantics require.

pub const LOG_PREFIX: &[u8] = b"L/";
pub const META_PREFIX: &[u8] = b"M/";

const META_PROMISE: &[u8] = b"M/promise";
const META_ACCEPTED_ROUND: &[u8] = b"M/accepted_round";
const META_DECIDED_IDX: &[u8] = b"M/decided_idx";
const META_COMPACTED_IDX: &[u8] = b"M/compacted_idx";
const META_SNAPSHOT: &[u8] = b"M/snapshot";
const META_STOPSIGN: &[u8] = b"M/stopsign";

#[must_use]
pub fn log_key(idx: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(LOG_PREFIX.len() + 8);
    out.extend_from_slice(LOG_PREFIX);
    out.extend_from_slice(&idx.to_be_bytes());
    out
}

#[must_use]
pub fn log_key_range() -> (Vec<u8>, Vec<u8>) {
    // For iteration: scan `log_key(0)..META_PREFIX`. The upper bound is
    // `META_PREFIX` because `M` > `L` lexicographically, so any byte beyond
    // a full LOG_PREFIX-keyed entry falls into META.
    (log_key(0), META_PREFIX.to_vec())
}

#[must_use]
pub fn meta_promise_key() -> Vec<u8> {
    META_PROMISE.to_vec()
}
#[must_use]
pub fn meta_accepted_round_key() -> Vec<u8> {
    META_ACCEPTED_ROUND.to_vec()
}
#[must_use]
pub fn meta_decided_idx_key() -> Vec<u8> {
    META_DECIDED_IDX.to_vec()
}
#[must_use]
pub fn meta_compacted_idx_key() -> Vec<u8> {
    META_COMPACTED_IDX.to_vec()
}
#[must_use]
pub fn meta_snapshot_key() -> Vec<u8> {
    META_SNAPSHOT.to_vec()
}
#[must_use]
pub fn meta_stopsign_key() -> Vec<u8> {
    META_STOPSIGN.to_vec()
}

pub fn parse_log_key(key: &[u8]) -> Option<u64> {
    if !key.starts_with(LOG_PREFIX) || key.len() != LOG_PREFIX.len() + 8 {
        return None;
    }
    let bytes = &key[LOG_PREFIX.len()..];
    let mut buf = [0u8; 8];
    buf.copy_from_slice(bytes);
    Some(u64::from_be_bytes(buf))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_keys_iterate_in_index_order() {
        // Big-endian encoding is required so RocksDB's lexicographic CF
        // iteration walks log indices monotonically. Pick two indices that
        // would invert under little-endian and verify they don't.
        let low = log_key(0x0000_0000_0000_00FF);
        let high = log_key(0x0000_0000_0000_0100);
        assert!(low < high, "log_key(255) < log_key(256) must hold");
    }

    #[test]
    fn log_keys_have_log_prefix() {
        assert!(log_key(0).starts_with(LOG_PREFIX));
        assert!(log_key(u64::MAX).starts_with(LOG_PREFIX));
    }

    #[test]
    fn meta_keys_have_meta_prefix() {
        let keys = [
            meta_promise_key(),
            meta_accepted_round_key(),
            meta_decided_idx_key(),
            meta_compacted_idx_key(),
            meta_snapshot_key(),
            meta_stopsign_key(),
        ];
        for key in &keys {
            assert!(
                key.starts_with(META_PREFIX),
                "key {key:?} missing META_PREFIX"
            );
        }
    }

    #[test]
    fn log_and_meta_prefixes_do_not_collide() {
        // RocksDB iteration must distinguish log entries from metadata
        // unambiguously. The two prefixes must not be each other's prefix.
        assert_ne!(LOG_PREFIX[0], META_PREFIX[0]);
    }

    #[test]
    fn log_key_round_trips_through_parse() {
        for idx in [0_u64, 1, 255, 256, u64::MAX] {
            let key = log_key(idx);
            assert_eq!(parse_log_key(&key), Some(idx));
        }
    }

    #[test]
    fn parse_log_key_rejects_non_log_keys() {
        assert!(parse_log_key(META_PREFIX).is_none());
        assert!(parse_log_key(b"L/").is_none()); // too short
        assert!(parse_log_key(&meta_promise_key()).is_none());
    }
}
