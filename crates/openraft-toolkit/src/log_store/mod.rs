//! RocksDB-backed `RaftLogStorage` implementation.

pub mod key_space;
#[allow(dead_code)]
mod meta;

pub use key_space::{Flat, GroupPrefixed, KeySpace, MetaLabel};

use thiserror::Error;

/// Errors produced by the rocksdb-backed log store.
#[derive(Debug, Error)]
pub enum RocksdbLogStoreError {
    #[error("rocksdb error: {0}")]
    RocksDb(#[from] rocksdb::Error),
    #[error("decode error: {0}")]
    Decode(#[from] bincode::Error),
    #[error("column family `{0}` not found")]
    MissingColumnFamily(String),
}
