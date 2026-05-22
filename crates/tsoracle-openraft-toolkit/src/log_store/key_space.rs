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

//! Strategies for laying out raft keys inside a RocksDB column family.
//!
//! `KeySpace` decides both how a log index becomes a column-family key and how
//! the four pieces of openraft metadata (vote, committed, last-purged, last-
//! applied membership) are namespaced. The two shipped strategies are [`Flat`]
//! for a single-group deployment (one raft instance per process) and
//! [`GroupPrefixed`] for a multi-group deployment that multiplexes N raft
//! instances over the same column families.

use std::fmt::Debug;

/// Labels for the four pieces of openraft metadata that share the meta CF.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetaLabel {
    Vote,
    Committed,
    LastPurged,
    LastMembership,
}

impl MetaLabel {
    pub fn as_bytes(self) -> &'static [u8] {
        match self {
            MetaLabel::Vote => b"vote",
            MetaLabel::Committed => b"committed",
            MetaLabel::LastPurged => b"last_purged",
            MetaLabel::LastMembership => b"last_membership",
        }
    }
}

/// How a raft instance's keys are laid out inside its column families.
///
/// Implementations must guarantee:
/// - `log_key` is strictly ordered by `index` (so RocksDB iteration in key order
///   visits entries in index order).
/// - `log_range` returns the smallest `[lo, hi)` interval that contains every
///   key `log_key(0..=u64::MAX)` produces. Iterators bounded by this range must
///   never see another raft instance's entries.
/// - `meta_key` produces distinct keys per `MetaLabel` and never collides with
///   any `log_key`.
pub trait KeySpace: Clone + Debug + Send + Sync + 'static {
    fn log_key(&self, index: u64) -> Vec<u8>;
    fn log_range(&self) -> (Vec<u8>, Vec<u8>);
    fn meta_key(&self, label: MetaLabel) -> Vec<u8>;
}

/// Single-group layout: log key is `index.to_be_bytes()`, meta key is the
/// label bytes alone.
#[derive(Debug, Clone, Copy, Default)]
pub struct Flat;

impl KeySpace for Flat {
    fn log_key(&self, index: u64) -> Vec<u8> {
        index.to_be_bytes().to_vec()
    }

    fn log_range(&self) -> (Vec<u8>, Vec<u8>) {
        (vec![0u8; 8], vec![0xFFu8; 8])
    }

    fn meta_key(&self, label: MetaLabel) -> Vec<u8> {
        label.as_bytes().to_vec()
    }
}

/// Multi-group layout: every key is prefixed with the group id (big-endian u64).
///
/// Log keys are `[group_id_be | index_be]` (16 bytes). Meta keys are
/// `[group_id_be | b'/' | label_bytes]`. Different groups produce disjoint
/// ranges, so a single column family can host N raft instances safely.
#[derive(Debug, Clone, Copy)]
pub struct GroupPrefixed {
    pub group_id: u64,
}

impl GroupPrefixed {
    pub fn new(group_id: u64) -> Self {
        Self { group_id }
    }
}

impl KeySpace for GroupPrefixed {
    fn log_key(&self, index: u64) -> Vec<u8> {
        let mut out = Vec::with_capacity(16);
        out.extend_from_slice(&self.group_id.to_be_bytes());
        out.extend_from_slice(&index.to_be_bytes());
        out
    }

    fn log_range(&self) -> (Vec<u8>, Vec<u8>) {
        let mut lo = Vec::with_capacity(16);
        lo.extend_from_slice(&self.group_id.to_be_bytes());
        lo.extend_from_slice(&[0u8; 8]);
        let mut hi = Vec::with_capacity(16);
        hi.extend_from_slice(&self.group_id.to_be_bytes());
        hi.extend_from_slice(&[0xFFu8; 8]);
        (lo, hi)
    }

    fn meta_key(&self, label: MetaLabel) -> Vec<u8> {
        let label_bytes = label.as_bytes();
        let mut out = Vec::with_capacity(8 + 1 + label_bytes.len());
        out.extend_from_slice(&self.group_id.to_be_bytes());
        out.push(b'/');
        out.extend_from_slice(label_bytes);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn any_meta_label() -> impl Strategy<Value = MetaLabel> {
        prop_oneof![
            Just(MetaLabel::Vote),
            Just(MetaLabel::Committed),
            Just(MetaLabel::LastPurged),
            Just(MetaLabel::LastMembership),
        ]
    }

    #[test]
    fn flat_log_keys_sort_by_index() {
        let k = Flat;
        let a = k.log_key(1);
        let b = k.log_key(2);
        let c = k.log_key(u64::MAX);
        assert!(a < b);
        assert!(b < c);
        assert_eq!(a.len(), 8);
    }

    #[test]
    fn flat_log_range_brackets_all_indices() {
        let k = Flat;
        let (lo, hi) = k.log_range();
        assert!(lo <= k.log_key(0));
        assert!(k.log_key(u64::MAX) <= hi);
    }

    #[test]
    fn flat_meta_labels_have_distinct_bytes() {
        let labels = [
            MetaLabel::Vote,
            MetaLabel::Committed,
            MetaLabel::LastPurged,
            MetaLabel::LastMembership,
        ];
        let k = Flat;
        for (i, a) in labels.iter().enumerate() {
            for b in &labels[i + 1..] {
                assert_ne!(k.meta_key(*a), k.meta_key(*b));
            }
        }
    }

    proptest! {
        // Strict monotonicity in index space implies strict monotonicity of the
        // encoded key in lex byte order — the contract the rocksdb iterator
        // relies on to walk entries in index order.
        #[test]
        fn flat_log_key_monotonic_in_index(a in any::<u64>(), b in any::<u64>()) {
            let k = Flat;
            let ka = k.log_key(a);
            let kb = k.log_key(b);
            match a.cmp(&b) {
                std::cmp::Ordering::Less    => prop_assert!(ka < kb),
                std::cmp::Ordering::Equal   => prop_assert_eq!(ka, kb),
                std::cmp::Ordering::Greater => prop_assert!(ka > kb),
            }
        }

        // log_range must contain log_key(i) for every reachable index.
        #[test]
        fn flat_log_range_contains_every_index(i in any::<u64>()) {
            let k = Flat;
            let (lo, hi) = k.log_range();
            let key = k.log_key(i);
            prop_assert!(lo <= key);
            prop_assert!(key <= hi);
        }

        // No meta key may collide with any log key (otherwise a meta read could
        // observe an entry's serialized bytes and vice versa).
        #[test]
        fn flat_meta_and_log_keys_never_collide(i in any::<u64>(), label in any_meta_label()) {
            let k = Flat;
            prop_assert_ne!(k.meta_key(label), k.log_key(i));
        }

        // Flat is documented as `u64::to_be_bytes`; this roundtrip pins that so
        // any future "optimization" that breaks decoding fails here.
        #[test]
        fn flat_log_key_roundtrips_as_u64_be(i in any::<u64>()) {
            let k = Flat;
            let bytes = k.log_key(i);
            prop_assert_eq!(bytes.len(), 8);
            let mut arr = [0u8; 8];
            arr.copy_from_slice(&bytes);
            prop_assert_eq!(u64::from_be_bytes(arr), i);
        }
    }
}

#[cfg(test)]
mod group_prefixed_tests {
    use super::*;

    #[test]
    fn different_groups_never_collide() {
        let g1 = GroupPrefixed::new(1);
        let g2 = GroupPrefixed::new(2);
        assert_ne!(g1.log_key(100), g2.log_key(100));
        assert_ne!(g1.meta_key(MetaLabel::Vote), g2.meta_key(MetaLabel::Vote));
    }

    #[test]
    fn group_log_range_excludes_other_groups() {
        let g = GroupPrefixed::new(5);
        let (lo, hi) = g.log_range();
        let other_lo = GroupPrefixed::new(4).log_key(u64::MAX);
        let other_hi = GroupPrefixed::new(6).log_key(0);
        assert!(other_lo < lo);
        assert!(hi < other_hi);
    }

    #[test]
    fn log_key_sorts_by_index_within_group() {
        let g = GroupPrefixed::new(7);
        assert!(g.log_key(1) < g.log_key(2));
        assert!(g.log_key(2) < g.log_key(u64::MAX));
        assert_eq!(g.log_key(0).len(), 16);
    }

    #[test]
    fn meta_key_does_not_collide_with_log_key() {
        let g = GroupPrefixed::new(3);
        // log keys end with the index bytes; meta keys have a `/` separator.
        for label in [
            MetaLabel::Vote,
            MetaLabel::Committed,
            MetaLabel::LastPurged,
            MetaLabel::LastMembership,
        ] {
            let mk = g.meta_key(label);
            for idx in [0u64, 1, 100, u64::MAX] {
                assert_ne!(mk, g.log_key(idx));
            }
        }
    }

    use proptest::prelude::*;

    fn any_meta_label() -> impl Strategy<Value = MetaLabel> {
        prop_oneof![
            Just(MetaLabel::Vote),
            Just(MetaLabel::Committed),
            Just(MetaLabel::LastPurged),
            Just(MetaLabel::LastMembership),
        ]
    }

    proptest! {
        // Within a single group, log_key remains strictly monotonic in index.
        #[test]
        fn group_log_key_monotonic_within_group(
            g_id in any::<u64>(),
            a in any::<u64>(),
            b in any::<u64>(),
        ) {
            let g = GroupPrefixed::new(g_id);
            let ka = g.log_key(a);
            let kb = g.log_key(b);
            match a.cmp(&b) {
                std::cmp::Ordering::Less    => prop_assert!(ka < kb),
                std::cmp::Ordering::Equal   => prop_assert_eq!(ka, kb),
                std::cmp::Ordering::Greater => prop_assert!(ka > kb),
            }
        }

        // For any two distinct groups and any two indices, log keys never collide.
        // This is the load-bearing property for multiplexing N raft instances on
        // one column family.
        #[test]
        fn group_log_keys_disjoint_across_groups(
            g1 in any::<u64>(),
            g2 in any::<u64>(),
            i1 in any::<u64>(),
            i2 in any::<u64>(),
        ) {
            prop_assume!(g1 != g2);
            let k1 = GroupPrefixed::new(g1).log_key(i1);
            let k2 = GroupPrefixed::new(g2).log_key(i2);
            prop_assert_ne!(k1, k2);
        }

        // log_range for group g strictly excludes any other group's keys, in
        // both directions. Iterators bounded by `log_range` therefore cannot
        // leak into a neighbor's data.
        #[test]
        fn group_log_range_strictly_excludes_other_groups(
            g_id in any::<u64>(),
            other_id in any::<u64>(),
            other_idx in any::<u64>(),
        ) {
            prop_assume!(g_id != other_id);
            let (lo, hi) = GroupPrefixed::new(g_id).log_range();
            let other_key = GroupPrefixed::new(other_id).log_key(other_idx);
            // other_key must be either strictly below lo or strictly above hi.
            prop_assert!(other_key < lo || other_key > hi);
        }

        // Inside a single group, log_range brackets every reachable index.
        #[test]
        fn group_log_range_contains_every_index_in_group(
            g_id in any::<u64>(),
            i in any::<u64>(),
        ) {
            let g = GroupPrefixed::new(g_id);
            let (lo, hi) = g.log_range();
            let key = g.log_key(i);
            prop_assert!(lo <= key);
            prop_assert!(key <= hi);
        }

        // No meta key (any group, any label) collides with any log key
        // (any group, any index). The `/` byte separator in meta_key plus the
        // 16-byte log_key length keep these disjoint.
        #[test]
        fn group_meta_and_log_keys_never_collide(
            g_meta in any::<u64>(),
            label in any_meta_label(),
            g_log in any::<u64>(),
            i in any::<u64>(),
        ) {
            let mk = GroupPrefixed::new(g_meta).meta_key(label);
            let lk = GroupPrefixed::new(g_log).log_key(i);
            prop_assert_ne!(mk, lk);
        }
    }
}
