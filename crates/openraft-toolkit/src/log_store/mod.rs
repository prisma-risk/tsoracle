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

use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use openraft::RaftTypeConfig;
use rocksdb::{BoundColumnFamily, DB};

/// RocksDB-backed `RaftLogStorage` implementation.
///
/// Parameterized by:
/// - `C`: the consumer's `RaftTypeConfig`.
/// - `K`: the active [`KeySpace`] — [`Flat`] for single-group deployments,
///   [`GroupPrefixed`] for multi-group deployments that multiplex N raft
///   instances onto shared column families.
///
/// Construct via [`RocksdbLogStore::open`], which validates that the two
/// column-family names you pass already exist on the database.
pub struct RocksdbLogStore<C, K>
where
    C: RaftTypeConfig,
    K: KeySpace,
{
    #[allow(dead_code)]
    db: Arc<DB>,
    log_cf: String,
    meta_cf: String,
    keys: K,
    _phantom: PhantomData<C>,
}

impl<C, K> RocksdbLogStore<C, K>
where
    C: RaftTypeConfig,
    K: KeySpace,
{
    /// Open a log store on top of an already-opened `DB`. Both column families
    /// must already exist; `open_cf_descriptors` should have created them when
    /// the database was opened.
    pub fn open(
        db: Arc<DB>,
        log_cf: impl Into<String>,
        meta_cf: impl Into<String>,
        keys: K,
    ) -> Result<Self, RocksdbLogStoreError> {
        let log_cf = log_cf.into();
        let meta_cf = meta_cf.into();
        db.cf_handle(&log_cf)
            .ok_or_else(|| RocksdbLogStoreError::MissingColumnFamily(log_cf.clone()))?;
        db.cf_handle(&meta_cf)
            .ok_or_else(|| RocksdbLogStoreError::MissingColumnFamily(meta_cf.clone()))?;
        Ok(Self {
            db,
            log_cf,
            meta_cf,
            keys,
            _phantom: PhantomData,
        })
    }

    #[allow(dead_code)]
    pub(super) fn log_cf_handle(&self) -> Arc<BoundColumnFamily<'_>> {
        self.db
            .cf_handle(&self.log_cf)
            .expect("log CF was validated at open")
    }

    #[allow(dead_code)]
    pub(super) fn meta_cf_handle(&self) -> Arc<BoundColumnFamily<'_>> {
        self.db
            .cf_handle(&self.meta_cf)
            .expect("meta CF was validated at open")
    }
}

impl<C, K> fmt::Debug for RocksdbLogStore<C, K>
where
    C: RaftTypeConfig,
    K: KeySpace,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RocksdbLogStore")
            .field("log_cf", &self.log_cf)
            .field("meta_cf", &self.meta_cf)
            .field("keys", &self.keys)
            .finish()
    }
}
