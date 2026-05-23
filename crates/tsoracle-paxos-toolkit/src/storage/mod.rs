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

/// RocksDB-backed implementation of [`omnipaxos::storage::Storage`].
///
/// The caller owns the `Arc<DB>` and the lifetime of the column family. This
/// crate never opens or closes a database; constructing a `RocksdbStorage`
/// only validates that the requested column family is present. Sharing the
/// same `Arc<DB>` with other column families (e.g., a snapshot store) is
/// supported and is the intended pattern.
///
/// The type parameter `T` is the OmniPaxos `Entry` type the log will hold;
/// the implementation persists log entries via `postcard` and meta fields
/// via fixed-shape encoders in [`crate::storage::meta`].
///
/// Construct with [`RocksdbStorage::open_in`].
#[cfg(feature = "rocksdb-storage")]
pub struct RocksdbStorage<T: Entry> {
    db: Arc<DB>,
    cf_name: String,
    _marker: PhantomData<T>,
}

#[cfg(feature = "rocksdb-storage")]
impl<T: Entry> RocksdbStorage<T> {
    /// Build a storage handle backed by an existing column family.
    ///
    /// The column family must already exist in `db`. The constructor only
    /// validates presence — it does NOT create the CF. Callers typically
    /// open the database with the appropriate `ColumnFamilyDescriptor`s
    /// for both the paxos log and any peer column families (snapshot
    /// store, application state) before invoking this.
    ///
    /// Returns [`StorageError::ColumnFamilyNotFound`] if `cf_name` is not
    /// registered on the supplied database.
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

    pub(crate) fn cf(&self) -> Result<Arc<BoundColumnFamily<'_>>, StorageError> {
        self.db
            .cf_handle(&self.cf_name)
            .ok_or_else(|| StorageError::ColumnFamilyNotFound(self.cf_name.clone()))
    }

    /// Write options for Paxos safety-critical writes.
    ///
    /// `sync = true` forces fsync of the WAL on every call. Used for
    /// [`Storage::set_promise`], [`Storage::set_accepted_round`],
    /// [`Storage::append_entry`], [`Storage::append_entries`], and
    /// [`Storage::append_on_prefix`] — a node that promises ballot N or
    /// accepts a log entry must not forget that promise across a crash, or
    /// Paxos safety is violated.
    pub(crate) fn write_sync_opts() -> WriteOptions {
        let mut opts = WriteOptions::default();
        opts.set_sync(true);
        opts
    }

    /// Write options for non-safety-critical writes.
    ///
    /// Used for [`Storage::set_decided_idx`], [`Storage::set_compacted_idx`],
    /// [`Storage::trim`], [`Storage::set_snapshot`], and
    /// [`Storage::set_stopsign`] — these fields can be reconstructed or
    /// recovered after a crash (decided_idx is monotonic from the log
    /// contents; compacted_idx is bounded by the trimmed range; snapshot
    /// and stopsign can be re-derived). Avoiding fsync on the hot apply
    /// path keeps throughput sane.
    pub(crate) fn write_async_opts() -> WriteOptions {
        WriteOptions::default()
    }

    pub(crate) fn batch_sync<F>(&self, f: F) -> Result<(), StorageError>
    where
        F: FnOnce(Arc<BoundColumnFamily<'_>>, &mut WriteBatch) -> Result<(), StorageError>,
    {
        let cf = self.cf()?;
        let mut batch = WriteBatch::default();
        f(cf, &mut batch)?;
        self.db.write_opt(batch, &Self::write_sync_opts())?;
        Ok(())
    }

    pub(crate) fn batch_async<F>(&self, f: F) -> Result<(), StorageError>
    where
        F: FnOnce(Arc<BoundColumnFamily<'_>>, &mut WriteBatch) -> Result<(), StorageError>,
    {
        let cf = self.cf()?;
        let mut batch = WriteBatch::default();
        f(cf, &mut batch)?;
        self.db.write_opt(batch, &Self::write_async_opts())?;
        Ok(())
    }
}

#[cfg(feature = "rocksdb-storage")]
fn box_err<E: std::error::Error + 'static>(err: E) -> Box<dyn std::error::Error> {
    Box::new(err)
}

#[cfg(feature = "rocksdb-storage")]
impl<T> RocksdbStorage<T>
where
    T: Entry + serde::Serialize + serde::de::DeserializeOwned + Clone + 'static,
{
    fn next_log_idx(&self) -> Result<u64, StorageError> {
        use crate::storage::key_space::{LOG_PREFIX, log_key_range, parse_log_key};
        use rocksdb::IteratorMode;

        // The next absolute write index must satisfy
        // `next >= compacted_idx`; otherwise a snapshot that trims every
        // surviving log key would let a fresh append land at L/0 and
        // become unreachable via get_suffix(compacted_idx).
        let compacted = self.compacted_idx_inner()?;
        let cf = self.cf()?;
        let (_, upper) = log_key_range();
        let iter = self
            .db
            .iterator_cf(&cf, IteratorMode::From(&upper, rocksdb::Direction::Reverse));
        for result in iter {
            let (key, _value) = result?;
            if key.starts_with(LOG_PREFIX) {
                let idx = parse_log_key(&key).ok_or_else(|| {
                    StorageError::LogIntegrity(format!("malformed log key: {key:?}"))
                })?;
                return Ok((idx + 1).max(compacted));
            }
        }
        Ok(compacted)
    }

    fn compacted_idx_inner(&self) -> Result<u64, StorageError> {
        use crate::storage::key_space::meta_compacted_idx_key;
        use crate::storage::meta::decode_u64;
        let cf = self.cf()?;
        match self.db.get_cf(&cf, meta_compacted_idx_key())? {
            Some(bytes) => Ok(decode_u64(&bytes)?),
            None => Ok(0),
        }
    }

    fn store_log_entry(
        &self,
        batch: &mut WriteBatch,
        cf: &Arc<BoundColumnFamily<'_>>,
        idx: u64,
        entry: &T,
    ) -> Result<(), StorageError> {
        use crate::codec::encode as codec_encode;
        use crate::storage::key_space::log_key;
        let key = log_key(idx);
        let value = codec_encode(entry)?;
        batch.put_cf(cf, key, value);
        Ok(())
    }
}

#[cfg(feature = "rocksdb-storage")]
impl<T> omnipaxos::storage::Storage<T> for RocksdbStorage<T>
where
    T: Entry + serde::Serialize + serde::de::DeserializeOwned + Clone + 'static,
    T::Snapshot: serde::Serialize + serde::de::DeserializeOwned + Clone,
{
    fn append_entry(&mut self, entry: T) -> omnipaxos::storage::StorageResult<u64> {
        crate::fail_point!("paxos_toolkit::storage::append_entry");
        let next = self.next_log_idx().map_err(box_err)?;
        self.batch_sync(|cf, batch| self.store_log_entry(batch, &cf, next, &entry))
            .map_err(box_err)?;
        let compacted = self.get_compacted_idx()?;
        Ok((next + 1).saturating_sub(compacted))
    }

    fn append_entries(&mut self, entries: Vec<T>) -> omnipaxos::storage::StorageResult<u64> {
        let start = self.next_log_idx().map_err(box_err)?;
        let count = entries.len() as u64;
        self.batch_sync(|cf, batch| {
            for (offset, entry) in entries.iter().enumerate() {
                self.store_log_entry(batch, &cf, start + offset as u64, entry)?;
            }
            Ok(())
        })
        .map_err(box_err)?;
        let compacted = self.get_compacted_idx()?;
        Ok((start + count).saturating_sub(compacted))
    }

    fn append_on_prefix(
        &mut self,
        from_idx: u64,
        entries: Vec<T>,
    ) -> omnipaxos::storage::StorageResult<u64> {
        use crate::storage::key_space::{log_key, log_key_range};
        let (_, upper) = log_key_range();
        let count = entries.len() as u64;
        self.batch_sync(|cf, batch| {
            let lower = log_key(from_idx);
            batch.delete_range_cf(&cf, &lower, &upper);
            for (offset, entry) in entries.iter().enumerate() {
                self.store_log_entry(batch, &cf, from_idx + offset as u64, entry)?;
            }
            Ok(())
        })
        .map_err(box_err)?;
        let compacted = self.get_compacted_idx()?;
        Ok((from_idx + count).saturating_sub(compacted))
    }

    fn set_promise(
        &mut self,
        n_prom: omnipaxos::ballot_leader_election::Ballot,
    ) -> omnipaxos::storage::StorageResult<()> {
        use crate::storage::key_space::meta_promise_key;
        use crate::storage::meta::encode_ballot;
        let bytes = encode_ballot(&n_prom).map_err(box_err)?;
        self.batch_sync(|cf, batch| {
            batch.put_cf(&cf, meta_promise_key(), bytes);
            Ok(())
        })
        .map_err(box_err)?;
        Ok(())
    }

    fn set_decided_idx(&mut self, ld: u64) -> omnipaxos::storage::StorageResult<()> {
        use crate::storage::key_space::meta_decided_idx_key;
        use crate::storage::meta::encode_u64;
        let bytes = encode_u64(ld);
        self.batch_async(|cf, batch| {
            batch.put_cf(&cf, meta_decided_idx_key(), bytes);
            Ok(())
        })
        .map_err(box_err)?;
        Ok(())
    }

    fn get_decided_idx(&self) -> omnipaxos::storage::StorageResult<u64> {
        use crate::storage::key_space::meta_decided_idx_key;
        use crate::storage::meta::decode_u64;
        let cf = self.cf().map_err(box_err)?;
        match self
            .db
            .get_cf(&cf, meta_decided_idx_key())
            .map_err(box_err)?
        {
            Some(bytes) => Ok(decode_u64(&bytes).map_err(box_err)?),
            None => Ok(0),
        }
    }

    fn set_accepted_round(
        &mut self,
        na: omnipaxos::ballot_leader_election::Ballot,
    ) -> omnipaxos::storage::StorageResult<()> {
        use crate::storage::key_space::meta_accepted_round_key;
        use crate::storage::meta::encode_ballot;
        let bytes = encode_ballot(&na).map_err(box_err)?;
        self.batch_sync(|cf, batch| {
            batch.put_cf(&cf, meta_accepted_round_key(), bytes);
            Ok(())
        })
        .map_err(box_err)?;
        Ok(())
    }

    fn get_accepted_round(
        &self,
    ) -> omnipaxos::storage::StorageResult<Option<omnipaxos::ballot_leader_election::Ballot>> {
        use crate::storage::key_space::meta_accepted_round_key;
        use crate::storage::meta::decode_ballot;
        let cf = self.cf().map_err(box_err)?;
        match self
            .db
            .get_cf(&cf, meta_accepted_round_key())
            .map_err(box_err)?
        {
            Some(bytes) => Ok(Some(decode_ballot(&bytes).map_err(box_err)?)),
            None => Ok(None),
        }
    }

    fn get_entries(&self, from: u64, to: u64) -> omnipaxos::storage::StorageResult<Vec<T>> {
        use crate::codec::decode as codec_decode;
        use crate::storage::key_space::{LOG_PREFIX, log_key};
        use rocksdb::IteratorMode;

        if from >= to {
            return Ok(Vec::new());
        }
        let cf = self.cf().map_err(box_err)?;
        let start = log_key(from);
        let end = log_key(to);
        let expected = (to - from) as usize;
        let iter = self
            .db
            .iterator_cf(&cf, IteratorMode::From(&start, rocksdb::Direction::Forward));
        let mut out = Vec::with_capacity(expected);
        for result in iter {
            let (key, value) = result.map_err(box_err)?;
            if !key.starts_with(LOG_PREFIX) || key.as_ref() >= end.as_slice() {
                break;
            }
            let entry: T = codec_decode(&value).map_err(box_err)?;
            out.push(entry);
            if out.len() == expected {
                break;
            }
        }
        // Gap contract: any missing index in [from, to) yields an empty Vec
        // rather than a partial prefix. The iterator returns entries in
        // ascending log-index order, so if the count is short, at least
        // one index in the requested range is missing.
        if out.len() < expected {
            return Ok(Vec::new());
        }
        Ok(out)
    }

    fn get_log_len(&self) -> omnipaxos::storage::StorageResult<u64> {
        let next = self.next_log_idx().map_err(box_err)?;
        let compacted = self.get_compacted_idx()?;
        Ok(next.saturating_sub(compacted))
    }

    fn get_suffix(&self, from: u64) -> omnipaxos::storage::StorageResult<Vec<T>> {
        use crate::codec::decode as codec_decode;
        use crate::storage::key_space::{LOG_PREFIX, log_key};
        use rocksdb::IteratorMode;

        // `IteratorMode::From(log_key(from), Forward)` seeks to the first
        // key `>= log_key(from)`. Because log keys are big-endian u64,
        // every key yielded by the iterator has an index `>= from`, so we
        // do not need to re-check the index after decoding.
        let cf = self.cf().map_err(box_err)?;
        let start = log_key(from);
        let iter = self
            .db
            .iterator_cf(&cf, IteratorMode::From(&start, rocksdb::Direction::Forward));
        let mut out = Vec::new();
        for result in iter {
            let (key, value) = result.map_err(box_err)?;
            if !key.starts_with(LOG_PREFIX) {
                break;
            }
            let entry: T = codec_decode(&value).map_err(box_err)?;
            out.push(entry);
        }
        Ok(out)
    }

    fn get_promise(
        &self,
    ) -> omnipaxos::storage::StorageResult<Option<omnipaxos::ballot_leader_election::Ballot>> {
        use crate::storage::key_space::meta_promise_key;
        use crate::storage::meta::decode_ballot;
        let cf = self.cf().map_err(box_err)?;
        match self.db.get_cf(&cf, meta_promise_key()).map_err(box_err)? {
            Some(bytes) => Ok(Some(decode_ballot(&bytes).map_err(box_err)?)),
            None => Ok(None),
        }
    }

    fn set_stopsign(
        &mut self,
        stopsign: Option<omnipaxos::storage::StopSign>,
    ) -> omnipaxos::storage::StorageResult<()> {
        use crate::storage::key_space::meta_stopsign_key;
        use crate::storage::meta::encode_postcard;
        match stopsign {
            Some(inner) => {
                let bytes = encode_postcard(&inner).map_err(box_err)?;
                self.batch_async(|cf, batch| {
                    batch.put_cf(&cf, meta_stopsign_key(), bytes);
                    Ok(())
                })
                .map_err(box_err)?;
            }
            None => {
                self.batch_async(|cf, batch| {
                    batch.delete_cf(&cf, meta_stopsign_key());
                    Ok(())
                })
                .map_err(box_err)?;
            }
        }
        Ok(())
    }

    fn get_stopsign(
        &self,
    ) -> omnipaxos::storage::StorageResult<Option<omnipaxos::storage::StopSign>> {
        use crate::storage::key_space::meta_stopsign_key;
        use crate::storage::meta::decode_postcard;
        let cf = self.cf().map_err(box_err)?;
        match self.db.get_cf(&cf, meta_stopsign_key()).map_err(box_err)? {
            Some(bytes) => Ok(Some(decode_postcard(&bytes).map_err(box_err)?)),
            None => Ok(None),
        }
    }

    fn trim(&mut self, idx: u64) -> omnipaxos::storage::StorageResult<()> {
        use crate::storage::key_space::log_key;
        self.batch_async(|cf, batch| {
            let lower = log_key(0);
            let upper = log_key(idx);
            batch.delete_range_cf(&cf, &lower, &upper);
            Ok(())
        })
        .map_err(box_err)?;
        Ok(())
    }

    fn set_compacted_idx(&mut self, idx: u64) -> omnipaxos::storage::StorageResult<()> {
        use crate::storage::key_space::meta_compacted_idx_key;
        use crate::storage::meta::encode_u64;
        let bytes = encode_u64(idx);
        self.batch_async(|cf, batch| {
            batch.put_cf(&cf, meta_compacted_idx_key(), bytes);
            Ok(())
        })
        .map_err(box_err)?;
        Ok(())
    }

    fn get_compacted_idx(&self) -> omnipaxos::storage::StorageResult<u64> {
        self.compacted_idx_inner().map_err(box_err)
    }

    fn set_snapshot(
        &mut self,
        snapshot: Option<T::Snapshot>,
    ) -> omnipaxos::storage::StorageResult<()> {
        use crate::storage::key_space::meta_snapshot_key;
        use crate::storage::meta::encode_postcard;
        match snapshot {
            Some(snap) => {
                let bytes = encode_postcard(&snap).map_err(box_err)?;
                self.batch_async(|cf, batch| {
                    batch.put_cf(&cf, meta_snapshot_key(), bytes);
                    Ok(())
                })
                .map_err(box_err)?;
            }
            None => {
                self.batch_async(|cf, batch| {
                    batch.delete_cf(&cf, meta_snapshot_key());
                    Ok(())
                })
                .map_err(box_err)?;
            }
        }
        Ok(())
    }

    fn get_snapshot(&self) -> omnipaxos::storage::StorageResult<Option<T::Snapshot>> {
        use crate::storage::key_space::meta_snapshot_key;
        use crate::storage::meta::decode_postcard;
        let cf = self.cf().map_err(box_err)?;
        match self.db.get_cf(&cf, meta_snapshot_key()).map_err(box_err)? {
            Some(bytes) => Ok(Some(decode_postcard(&bytes).map_err(box_err)?)),
            None => Ok(None),
        }
    }
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

#[cfg(all(test, feature = "rocksdb-storage"))]
mod log_tests {
    use super::open_in_tests::{TestEntry, open_db};
    use super::*;
    use omnipaxos::storage::Storage;
    use tempfile::TempDir;

    pub(super) fn fresh_storage(dir: &TempDir) -> RocksdbStorage<TestEntry> {
        let db = open_db(dir, "tso_paxos");
        RocksdbStorage::open_in(db, "tso_paxos").expect("open_in")
    }

    #[test]
    fn empty_log_has_zero_len() {
        let dir = TempDir::new().unwrap();
        let storage = fresh_storage(&dir);
        assert_eq!(storage.get_log_len().unwrap(), 0);
    }

    #[test]
    fn append_entry_returns_new_log_len() {
        let dir = TempDir::new().unwrap();
        let mut storage = fresh_storage(&dir);
        let len = storage.append_entry(TestEntry { value: 10 }).unwrap();
        assert_eq!(len, 1);
        let len = storage.append_entry(TestEntry { value: 20 }).unwrap();
        assert_eq!(len, 2);
    }

    #[test]
    fn append_entries_returns_new_log_len() {
        let dir = TempDir::new().unwrap();
        let mut storage = fresh_storage(&dir);
        let len = storage
            .append_entries(vec![
                TestEntry { value: 1 },
                TestEntry { value: 2 },
                TestEntry { value: 3 },
            ])
            .unwrap();
        assert_eq!(len, 3);
    }

    #[test]
    fn get_entries_returns_range() {
        let dir = TempDir::new().unwrap();
        let mut storage = fresh_storage(&dir);
        storage
            .append_entries(vec![
                TestEntry { value: 10 },
                TestEntry { value: 20 },
                TestEntry { value: 30 },
            ])
            .unwrap();
        let got = storage.get_entries(0, 2).unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].value, 10);
        assert_eq!(got[1].value, 20);
    }

    #[test]
    fn get_entries_empty_on_gap() {
        // Contract: any index missing in [from, to) returns an empty Vec.
        let dir = TempDir::new().unwrap();
        let mut storage = fresh_storage(&dir);
        storage
            .append_entries(vec![TestEntry { value: 1 }, TestEntry { value: 2 }])
            .unwrap();
        // Range [0, 5) crosses missing indices 2, 3, 4.
        assert!(storage.get_entries(0, 5).unwrap().is_empty());
    }

    #[test]
    fn get_suffix_returns_from_index_to_end() {
        let dir = TempDir::new().unwrap();
        let mut storage = fresh_storage(&dir);
        storage
            .append_entries(vec![
                TestEntry { value: 100 },
                TestEntry { value: 200 },
                TestEntry { value: 300 },
            ])
            .unwrap();
        let got = storage.get_suffix(1).unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].value, 200);
    }

    #[test]
    fn append_on_prefix_truncates_then_appends() {
        let dir = TempDir::new().unwrap();
        let mut storage = fresh_storage(&dir);
        storage
            .append_entries(vec![
                TestEntry { value: 1 },
                TestEntry { value: 2 },
                TestEntry { value: 3 },
            ])
            .unwrap();
        let len = storage
            .append_on_prefix(1, vec![TestEntry { value: 9 }, TestEntry { value: 8 }])
            .unwrap();
        assert_eq!(len, 3);
        let all = storage.get_suffix(0).unwrap();
        assert_eq!(all[0].value, 1);
        assert_eq!(all[1].value, 9);
        assert_eq!(all[2].value, 8);
    }
}

#[cfg(all(test, feature = "rocksdb-storage"))]
mod ballot_tests {
    use super::log_tests::fresh_storage;
    use omnipaxos::ballot_leader_election::Ballot;
    use omnipaxos::storage::Storage;
    use tempfile::TempDir;

    fn ballot(config_id: u32, n: u32, pid: u64) -> Ballot {
        Ballot {
            config_id,
            n,
            priority: 0,
            pid,
        }
    }

    #[test]
    fn promise_is_none_initially() {
        let dir = TempDir::new().unwrap();
        let storage = fresh_storage(&dir);
        assert!(storage.get_promise().unwrap().is_none());
    }

    #[test]
    fn set_promise_round_trip() {
        let dir = TempDir::new().unwrap();
        let mut storage = fresh_storage(&dir);
        let expected = ballot(1, 5, 2);
        storage.set_promise(expected).unwrap();
        let got = storage.get_promise().unwrap().expect("present");
        assert_eq!(got.n, expected.n);
        assert_eq!(got.pid, expected.pid);
        assert_eq!(got.config_id, expected.config_id);
    }

    #[test]
    fn accepted_round_is_none_initially() {
        let dir = TempDir::new().unwrap();
        let storage = fresh_storage(&dir);
        assert!(storage.get_accepted_round().unwrap().is_none());
    }

    #[test]
    fn set_accepted_round_round_trip() {
        let dir = TempDir::new().unwrap();
        let mut storage = fresh_storage(&dir);
        let expected = ballot(1, 7, 3);
        storage.set_accepted_round(expected).unwrap();
        let got = storage.get_accepted_round().unwrap().expect("present");
        assert_eq!(got.n, expected.n);
    }
}

#[cfg(all(test, feature = "rocksdb-storage"))]
mod decided_compacted_tests {
    use super::log_tests::fresh_storage;
    use super::open_in_tests::TestEntry;
    use omnipaxos::storage::Storage;
    use tempfile::TempDir;

    #[test]
    fn decided_idx_defaults_to_zero() {
        let dir = TempDir::new().unwrap();
        let storage = fresh_storage(&dir);
        assert_eq!(storage.get_decided_idx().unwrap(), 0);
    }

    #[test]
    fn set_decided_idx_round_trip() {
        let dir = TempDir::new().unwrap();
        let mut storage = fresh_storage(&dir);
        storage.set_decided_idx(42).unwrap();
        assert_eq!(storage.get_decided_idx().unwrap(), 42);
    }

    #[test]
    fn compacted_idx_defaults_to_zero() {
        let dir = TempDir::new().unwrap();
        let storage = fresh_storage(&dir);
        assert_eq!(storage.get_compacted_idx().unwrap(), 0);
    }

    #[test]
    fn set_compacted_idx_round_trip() {
        let dir = TempDir::new().unwrap();
        let mut storage = fresh_storage(&dir);
        storage.set_compacted_idx(17).unwrap();
        assert_eq!(storage.get_compacted_idx().unwrap(), 17);
    }

    #[test]
    fn trim_then_compacted_yields_physical_remaining_length() {
        // trim takes an absolute index and deletes [0, idx). set_compacted_idx
        // is the paired call that advances the offset. After both, get_log_len
        // returns `next_abs - compacted = physical remaining`.
        let dir = TempDir::new().unwrap();
        let mut storage = fresh_storage(&dir);
        storage
            .append_entries(vec![
                TestEntry { value: 1 },
                TestEntry { value: 2 },
                TestEntry { value: 3 },
                TestEntry { value: 4 },
            ])
            .unwrap();
        storage.trim(2).unwrap();
        storage.set_compacted_idx(2).unwrap();
        let suffix = storage.get_suffix(2).unwrap();
        assert_eq!(suffix.len(), 2);
        assert_eq!(suffix[0].value, 3);
        // Physical remaining = next_abs (4) - compacted (2) = 2.
        assert_eq!(storage.get_log_len().unwrap(), 2);
    }

    #[test]
    fn append_after_full_trim_writes_at_compacted_idx() {
        // After a snapshot trims every physical log key, the next append
        // must continue at the absolute index `compacted_idx`, not reset
        // to 0. Otherwise OmniPaxos's in-memory accepted_idx diverges
        // from the on-disk key space and the entry becomes unreadable
        // via get_suffix/get_entries.
        let dir = TempDir::new().unwrap();
        let mut storage = fresh_storage(&dir);
        storage
            .append_entries(vec![
                TestEntry { value: 1 },
                TestEntry { value: 2 },
                TestEntry { value: 3 },
                TestEntry { value: 4 },
            ])
            .unwrap();
        storage.trim(4).unwrap();
        storage.set_compacted_idx(4).unwrap();

        let new_len = storage.append_entry(TestEntry { value: 99 }).unwrap();
        // physical remaining = (compacted + 1) - compacted = 1
        assert_eq!(new_len, 1);
        assert_eq!(storage.get_log_len().unwrap(), 1);

        let suffix = storage.get_suffix(4).unwrap();
        assert_eq!(suffix.len(), 1, "entry must be reachable at absolute idx 4");
        assert_eq!(suffix[0].value, 99);

        let range = storage.get_entries(4, 5).unwrap();
        assert_eq!(range.len(), 1);
        assert_eq!(range[0].value, 99);

        // The phantom-write bug stored the entry at L/0 even though
        // compacted_idx is 4, so a get_suffix(0) would surface it.
        // The fix means the entry lives at L/4, so get_suffix(0)
        // returns exactly the same single entry (no phantom at L/0).
        let from_zero = storage.get_suffix(0).unwrap();
        assert_eq!(from_zero.len(), 1);
    }

    #[test]
    fn append_after_full_trim_survives_reopen() {
        // Crash-recovery shape: after full compaction + append, drop the
        // storage and reopen on the same path. The persisted entry must
        // still live at the correct absolute index, and the next append
        // must continue from there.
        use super::open_in_tests::open_db;
        use crate::storage::RocksdbStorage;

        let dir = TempDir::new().unwrap();
        {
            let db = open_db(&dir, "tso_paxos");
            let mut storage: RocksdbStorage<TestEntry> =
                RocksdbStorage::open_in(db.clone(), "tso_paxos").unwrap();
            storage
                .append_entries(vec![
                    TestEntry { value: 10 },
                    TestEntry { value: 20 },
                    TestEntry { value: 30 },
                ])
                .unwrap();
            storage.trim(3).unwrap();
            storage.set_compacted_idx(3).unwrap();
            storage.append_entry(TestEntry { value: 77 }).unwrap();
            drop(storage);
            drop(db);
        }

        let db = open_db(&dir, "tso_paxos");
        let mut storage: RocksdbStorage<TestEntry> =
            RocksdbStorage::open_in(db, "tso_paxos").unwrap();
        assert_eq!(storage.get_compacted_idx().unwrap(), 3);
        let recovered = storage.get_suffix(3).unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].value, 77);

        let next_len = storage.append_entry(TestEntry { value: 88 }).unwrap();
        assert_eq!(next_len, 2);
        let recovered = storage.get_suffix(3).unwrap();
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[0].value, 77);
        assert_eq!(recovered[1].value, 88);
        // The new entry must land at absolute idx 4, not L/1.
        let single = storage.get_entries(4, 5).unwrap();
        assert_eq!(single.len(), 1);
        assert_eq!(single[0].value, 88);
    }
}

#[cfg(all(test, feature = "rocksdb-storage"))]
mod snapshot_stopsign_tests {
    use super::log_tests::fresh_storage;
    use super::open_in_tests::TestSnapshot;
    use omnipaxos::ClusterConfig;
    use omnipaxos::storage::{StopSign, Storage};
    use tempfile::TempDir;

    fn stopsign() -> StopSign {
        StopSign::with(
            ClusterConfig {
                configuration_id: 2,
                nodes: vec![1, 2, 3],
                flexible_quorum: None,
            },
            None,
        )
    }

    #[test]
    fn snapshot_is_none_initially() {
        let dir = TempDir::new().unwrap();
        let storage = fresh_storage(&dir);
        assert!(storage.get_snapshot().unwrap().is_none());
    }

    #[test]
    fn set_snapshot_round_trip() {
        let dir = TempDir::new().unwrap();
        let mut storage = fresh_storage(&dir);
        storage
            .set_snapshot(Some(TestSnapshot { value: 99 }))
            .unwrap();
        let got = storage.get_snapshot().unwrap().expect("present");
        assert_eq!(got.value, 99);
    }

    #[test]
    fn set_snapshot_none_clears() {
        let dir = TempDir::new().unwrap();
        let mut storage = fresh_storage(&dir);
        storage
            .set_snapshot(Some(TestSnapshot { value: 5 }))
            .unwrap();
        storage.set_snapshot(None).unwrap();
        assert!(storage.get_snapshot().unwrap().is_none());
    }

    #[test]
    fn stopsign_is_none_initially() {
        let dir = TempDir::new().unwrap();
        let storage = fresh_storage(&dir);
        assert!(storage.get_stopsign().unwrap().is_none());
    }

    #[test]
    fn set_stopsign_round_trip() {
        let dir = TempDir::new().unwrap();
        let mut storage = fresh_storage(&dir);
        storage.set_stopsign(Some(stopsign())).unwrap();
        let got = storage.get_stopsign().unwrap().expect("present");
        assert_eq!(got.next_config.configuration_id, 2);
        assert_eq!(got.next_config.nodes, vec![1, 2, 3]);
    }

    #[test]
    fn set_stopsign_none_clears() {
        let dir = TempDir::new().unwrap();
        let mut storage = fresh_storage(&dir);
        storage.set_stopsign(Some(stopsign())).unwrap();
        storage.set_stopsign(None).unwrap();
        assert!(storage.get_stopsign().unwrap().is_none());
    }
}
