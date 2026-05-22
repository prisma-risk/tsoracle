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
    db: Arc<DB>,
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

    pub(crate) fn cf(&self) -> Result<Arc<BoundColumnFamily<'_>>, StorageError> {
        self.db
            .cf_handle(&self.cf_name)
            .ok_or_else(|| StorageError::ColumnFamilyNotFound(self.cf_name.clone()))
    }

    pub(crate) fn write_opts() -> WriteOptions {
        WriteOptions::default()
    }

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
                return Ok(idx + 1);
            }
        }
        Ok(0)
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
        let next = self.next_log_idx().map_err(box_err)?;
        self.batch_with(|cf, batch| self.store_log_entry(batch, &cf, next, &entry))
            .map_err(box_err)?;
        let compacted = self.get_compacted_idx()?;
        Ok((next + 1).saturating_sub(compacted))
    }

    fn append_entries(&mut self, entries: Vec<T>) -> omnipaxos::storage::StorageResult<u64> {
        let start = self.next_log_idx().map_err(box_err)?;
        let count = entries.len() as u64;
        self.batch_with(|cf, batch| {
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
        self.batch_with(|cf, batch| {
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
        self.batch_with(|cf, batch| {
            batch.put_cf(&cf, meta_promise_key(), bytes);
            Ok(())
        })
        .map_err(box_err)?;
        Ok(())
    }

    fn set_decided_idx(&mut self, _ld: u64) -> omnipaxos::storage::StorageResult<()> {
        unimplemented!("set_decided_idx: implemented in a later commit")
    }

    fn get_decided_idx(&self) -> omnipaxos::storage::StorageResult<u64> {
        unimplemented!("get_decided_idx: implemented in a later commit")
    }

    fn set_accepted_round(
        &mut self,
        na: omnipaxos::ballot_leader_election::Ballot,
    ) -> omnipaxos::storage::StorageResult<()> {
        use crate::storage::key_space::meta_accepted_round_key;
        use crate::storage::meta::encode_ballot;
        let bytes = encode_ballot(&na).map_err(box_err)?;
        self.batch_with(|cf, batch| {
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
        use crate::storage::key_space::log_key;
        if from >= to {
            return Ok(Vec::new());
        }
        let cf = self.cf().map_err(box_err)?;
        let mut out = Vec::with_capacity((to - from) as usize);
        for idx in from..to {
            let key = log_key(idx);
            match self.db.get_cf(&cf, &key).map_err(box_err)? {
                Some(bytes) => {
                    let entry: T = codec_decode(&bytes).map_err(box_err)?;
                    out.push(entry);
                }
                None => return Ok(Vec::new()),
            }
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
        use crate::storage::key_space::{LOG_PREFIX, log_key, parse_log_key};
        use rocksdb::IteratorMode;

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
            let idx = parse_log_key(&key).ok_or_else(|| {
                box_err(StorageError::LogIntegrity(format!(
                    "malformed log key: {key:?}"
                )))
            })?;
            if idx < from {
                continue;
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
        _s: Option<omnipaxos::storage::StopSign>,
    ) -> omnipaxos::storage::StorageResult<()> {
        unimplemented!("set_stopsign: implemented in a later commit")
    }

    fn get_stopsign(
        &self,
    ) -> omnipaxos::storage::StorageResult<Option<omnipaxos::storage::StopSign>> {
        unimplemented!("get_stopsign: implemented in a later commit")
    }

    fn trim(&mut self, _idx: u64) -> omnipaxos::storage::StorageResult<()> {
        unimplemented!("trim: implemented in a later commit")
    }

    fn set_compacted_idx(&mut self, _idx: u64) -> omnipaxos::storage::StorageResult<()> {
        unimplemented!("set_compacted_idx: implemented in a later commit")
    }

    fn get_compacted_idx(&self) -> omnipaxos::storage::StorageResult<u64> {
        // Temporary impl: needed by append_*/get_log_len in this commit; replaced
        // with the full meta-backed read in a later commit.
        Ok(0)
    }

    fn set_snapshot(
        &mut self,
        _snapshot: Option<T::Snapshot>,
    ) -> omnipaxos::storage::StorageResult<()> {
        unimplemented!("set_snapshot: implemented in a later commit")
    }

    fn get_snapshot(&self) -> omnipaxos::storage::StorageResult<Option<T::Snapshot>> {
        unimplemented!("get_snapshot: implemented in a later commit")
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
        let b = ballot(1, 5, 2);
        storage.set_promise(b).unwrap();
        let got = storage.get_promise().unwrap().expect("present");
        assert_eq!(got.n, b.n);
        assert_eq!(got.pid, b.pid);
        assert_eq!(got.config_id, b.config_id);
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
        let b = ballot(1, 7, 3);
        storage.set_accepted_round(b).unwrap();
        let got = storage.get_accepted_round().unwrap().expect("present");
        assert_eq!(got.n, b.n);
    }
}
