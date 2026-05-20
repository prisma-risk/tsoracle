//! RocksDB-backed `RaftLogStorage` implementation.

pub mod key_space;

pub use key_space::{Flat, GroupPrefixed, KeySpace, MetaLabel};
