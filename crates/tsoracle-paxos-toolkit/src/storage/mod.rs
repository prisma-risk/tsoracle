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

//! RocksDB-backed `omnipaxos::storage::Storage` implementation.
//!
//! Log keys are absolute; the `compacted_idx` offset is tracked separately
//! via the meta column. `get_log_len()` returns the physical remaining
//! count, matching `omnipaxos_storage::persistent_storage`. OmniPaxos pairs
//! `trim(idx)` with `set_compacted_idx(idx)`; the two are independent here.

pub mod key_space;
pub mod meta;

#[cfg(feature = "rocksdb-storage")]
use std::marker::PhantomData;
#[cfg(feature = "rocksdb-storage")]
use std::sync::Arc;

#[cfg(feature = "rocksdb-storage")]
use omnipaxos::storage::Entry;
#[cfg(feature = "rocksdb-storage")]
use rocksdb::{BoundColumnFamily, DB, WriteBatch, WriteOptions};

#[cfg(feature = "rocksdb-storage")]
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("column family `{0}` not found in the supplied DB handle")]
    ColumnFamilyNotFound(String),
    #[error("rocksdb error: {0}")]
    Rocksdb(#[from] rocksdb::Error),
    #[error("meta serialization error: {0}")]
    Meta(#[from] crate::storage::meta::MetaError),
    #[error("codec error: {0}")]
    Codec(#[from] crate::codec::CodecError),
    #[error("log integrity violation: {0}")]
    LogIntegrity(String),
}

#[cfg(feature = "rocksdb-storage")]
pub struct RocksdbStorage<T: Entry> {
    #[allow(dead_code)]
    db: Arc<DB>,
    #[allow(dead_code)]
    cf_name: String,
    _marker: PhantomData<T>,
}

#[cfg(feature = "rocksdb-storage")]
impl<T: Entry> RocksdbStorage<T> {
    pub fn open_in(db: Arc<DB>, cf_name: &str) -> Result<Self, StorageError> {
        if db.cf_handle(cf_name).is_none() {
            return Err(StorageError::ColumnFamilyNotFound(cf_name.to_string()));
        }
        Ok(Self {
            db,
            cf_name: cf_name.to_string(),
            _marker: PhantomData,
        })
    }

    #[allow(dead_code)]
    pub(crate) fn cf(&self) -> Result<Arc<BoundColumnFamily<'_>>, StorageError> {
        self.db
            .cf_handle(&self.cf_name)
            .ok_or_else(|| StorageError::ColumnFamilyNotFound(self.cf_name.clone()))
    }

    #[allow(dead_code)]
    pub(crate) fn write_opts() -> WriteOptions {
        WriteOptions::default()
    }

    #[allow(dead_code)]
    pub(crate) fn batch_with<F>(&self, f: F) -> Result<(), StorageError>
    where
        F: FnOnce(Arc<BoundColumnFamily<'_>>, &mut WriteBatch) -> Result<(), StorageError>,
    {
        let cf = self.cf()?;
        let mut batch = WriteBatch::default();
        f(cf, &mut batch)?;
        self.db.write_opt(batch, &Self::write_opts())?;
        Ok(())
    }
}

#[cfg(feature = "rocksdb-storage")]
#[allow(dead_code)]
fn box_err<E: std::error::Error + 'static>(err: E) -> Box<dyn std::error::Error> {
    Box::new(err)
}

#[cfg(all(test, feature = "rocksdb-storage"))]
pub(crate) mod open_in_tests {
    use super::*;
    use rocksdb::{ColumnFamilyDescriptor, DB, Options};
    use std::sync::Arc;
    use tempfile::TempDir;

    pub(crate) fn open_db(dir: &TempDir, cf_name: &str) -> Arc<DB> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let cf = ColumnFamilyDescriptor::new(cf_name, Options::default());
        Arc::new(DB::open_cf_descriptors(&opts, dir.path(), vec![cf]).expect("open db"))
    }

    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    pub(crate) struct TestEntry {
        pub value: u64,
    }
    impl omnipaxos::storage::Entry for TestEntry {
        type Snapshot = TestSnapshot;
    }
    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    pub(crate) struct TestSnapshot {
        pub value: u64,
    }
    impl omnipaxos::storage::Snapshot<TestEntry> for TestSnapshot {
        fn create(entries: &[TestEntry]) -> Self {
            Self {
                value: entries.iter().map(|e| e.value).max().unwrap_or(0),
            }
        }
        fn merge(&mut self, other: Self) {
            self.value = self.value.max(other.value);
        }
        fn use_snapshots() -> bool {
            true
        }
    }

    #[test]
    fn open_in_returns_storage_for_existing_cf() {
        let dir = TempDir::new().expect("tempdir");
        let db = open_db(&dir, "tso_paxos");
        let storage: RocksdbStorage<TestEntry> =
            RocksdbStorage::open_in(db.clone(), "tso_paxos").expect("open_in");
        assert!(
            Arc::strong_count(&db) >= 2,
            "storage should hold an Arc to DB"
        );
        drop(storage);
    }

    #[test]
    fn open_in_rejects_missing_cf() {
        let dir = TempDir::new().expect("tempdir");
        let db = open_db(&dir, "tso_paxos");
        let result: Result<RocksdbStorage<TestEntry>, _> =
            RocksdbStorage::open_in(db, "missing_cf");
        assert!(matches!(result, Err(StorageError::ColumnFamilyNotFound(_))));
    }
}
