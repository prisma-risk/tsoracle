//! Strategies for laying out raft keys inside a RocksDB column family.
//!
//! `KeySpace` decides both how a log index becomes a column-family key and how
//! the four pieces of openraft metadata (vote, committed, last-purged, last-
//! applied membership) are namespaced. The two shipped strategies are [`Flat`]
//! for a single-group deployment (one raft instance per process) and (in a
//! follow-up) `GroupPrefixed` for a multi-group deployment that multiplexes N
//! raft instances over the same column families.

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
pub trait KeySpace: Debug + Send + Sync + 'static {
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
