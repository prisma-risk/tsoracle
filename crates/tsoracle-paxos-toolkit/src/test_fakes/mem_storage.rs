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

//! In-memory `omnipaxos::storage::Storage` implementation for unit and
//! integration tests, and for downstream conformance suites that want to
//! drive an OmniPaxos cluster without RocksDB.
//!
//! Same semantics as the production [`crate::storage::RocksdbStorage`]:
//! log keys are absolute, `get_log_len` returns `next_abs - compacted_idx`,
//! `trim` is independent of `set_compacted_idx` (OmniPaxos pairs them),
//! and `get_entries` returns an empty `Vec` on any gap rather than a
//! partial prefix.

use std::collections::BTreeMap;
use std::marker::PhantomData;

use omnipaxos::ballot_leader_election::Ballot;
use omnipaxos::storage::{Entry, StopSign, Storage, StorageResult};
use parking_lot::Mutex;

/// Cloneable in-memory `Storage<T>` backed by a shared mutex.
///
/// Clones share the same underlying state (the toolkit uses interior
/// mutability so the OmniPaxos handle can take `&mut self` without the
/// caller needing exclusive ownership of the storage).
pub struct MemStorage<T: Entry> {
    inner: Mutex<MemInner<T>>,
    _marker: PhantomData<T>,
}

struct MemInner<T: Entry> {
    log: BTreeMap<u64, T>,
    promise: Option<Ballot>,
    accepted_round: Option<Ballot>,
    decided_idx: u64,
    compacted_idx: u64,
    snapshot: Option<T::Snapshot>,
    stopsign: Option<StopSign>,
}

impl<T: Entry> Default for MemStorage<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Entry> MemStorage<T> {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(MemInner {
                log: BTreeMap::new(),
                promise: None,
                accepted_round: None,
                decided_idx: 0,
                compacted_idx: 0,
                snapshot: None,
                stopsign: None,
            }),
            _marker: PhantomData,
        }
    }
}

impl<T> Storage<T> for MemStorage<T>
where
    T: Entry + Clone,
    T::Snapshot: Clone,
{
    fn append_entry(&mut self, entry: T) -> StorageResult<u64> {
        let mut inner = self.inner.lock();
        // After a full trim the log is empty; the next absolute write index
        // must floor at `compacted_idx` (matching production `next_log_idx`),
        // not reset to 0, or the entry lands below the compaction floor and
        // becomes unreachable via get_suffix(compacted_idx).
        let next = inner
            .log
            .keys()
            .next_back()
            .map(|key| key + 1)
            .unwrap_or(inner.compacted_idx);
        inner.log.insert(next, entry);
        let compacted = inner.compacted_idx;
        Ok((next + 1).saturating_sub(compacted))
    }

    fn append_entries(&mut self, entries: Vec<T>) -> StorageResult<u64> {
        let mut inner = self.inner.lock();
        // Floor at `compacted_idx` after a full trim; see `append_entry`.
        let start = inner
            .log
            .keys()
            .next_back()
            .map(|key| key + 1)
            .unwrap_or(inner.compacted_idx);
        let count = entries.len() as u64;
        for (offset, entry) in entries.into_iter().enumerate() {
            inner.log.insert(start + offset as u64, entry);
        }
        let compacted = inner.compacted_idx;
        Ok((start + count).saturating_sub(compacted))
    }

    fn append_on_prefix(&mut self, from_idx: u64, entries: Vec<T>) -> StorageResult<u64> {
        let mut inner = self.inner.lock();
        inner.log.retain(|key, _| *key < from_idx);
        let count = entries.len() as u64;
        for (offset, entry) in entries.into_iter().enumerate() {
            inner.log.insert(from_idx + offset as u64, entry);
        }
        let compacted = inner.compacted_idx;
        Ok((from_idx + count).saturating_sub(compacted))
    }

    fn get_entries(&self, from: u64, to: u64) -> StorageResult<Vec<T>> {
        if from >= to {
            return Ok(Vec::new());
        }
        let inner = self.inner.lock();
        let mut out = Vec::with_capacity((to - from) as usize);
        for idx in from..to {
            match inner.log.get(&idx) {
                Some(entry) => out.push(entry.clone()),
                None => return Ok(Vec::new()),
            }
        }
        Ok(out)
    }

    fn get_log_len(&self) -> StorageResult<u64> {
        let inner = self.inner.lock();
        let next_abs = inner.log.keys().next_back().map(|key| key + 1).unwrap_or(0);
        Ok(next_abs.saturating_sub(inner.compacted_idx))
    }

    fn get_suffix(&self, from: u64) -> StorageResult<Vec<T>> {
        let inner = self.inner.lock();
        Ok(inner
            .log
            .range(from..)
            .map(|(_, entry)| entry.clone())
            .collect())
    }

    fn set_promise(&mut self, ballot: Ballot) -> StorageResult<()> {
        self.inner.lock().promise = Some(ballot);
        Ok(())
    }

    fn get_promise(&self) -> StorageResult<Option<Ballot>> {
        Ok(self.inner.lock().promise)
    }

    fn set_accepted_round(&mut self, ballot: Ballot) -> StorageResult<()> {
        self.inner.lock().accepted_round = Some(ballot);
        Ok(())
    }

    fn get_accepted_round(&self) -> StorageResult<Option<Ballot>> {
        Ok(self.inner.lock().accepted_round)
    }

    fn set_decided_idx(&mut self, idx: u64) -> StorageResult<()> {
        self.inner.lock().decided_idx = idx;
        Ok(())
    }

    fn get_decided_idx(&self) -> StorageResult<u64> {
        Ok(self.inner.lock().decided_idx)
    }

    fn trim(&mut self, idx: u64) -> StorageResult<()> {
        self.inner.lock().log.retain(|key, _| *key >= idx);
        Ok(())
    }

    fn set_compacted_idx(&mut self, idx: u64) -> StorageResult<()> {
        self.inner.lock().compacted_idx = idx;
        Ok(())
    }

    fn get_compacted_idx(&self) -> StorageResult<u64> {
        Ok(self.inner.lock().compacted_idx)
    }

    fn set_snapshot(&mut self, snapshot: Option<T::Snapshot>) -> StorageResult<()> {
        self.inner.lock().snapshot = snapshot;
        Ok(())
    }

    fn get_snapshot(&self) -> StorageResult<Option<T::Snapshot>> {
        Ok(self.inner.lock().snapshot.clone())
    }

    fn set_stopsign(&mut self, stopsign: Option<StopSign>) -> StorageResult<()> {
        self.inner.lock().stopsign = stopsign;
        Ok(())
    }

    fn get_stopsign(&self) -> StorageResult<Option<StopSign>> {
        Ok(self.inner.lock().stopsign.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use omnipaxos::ballot_leader_election::Ballot;
    use omnipaxos::storage::Storage;

    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    struct Cmd(u64);

    impl omnipaxos::storage::Entry for Cmd {
        type Snapshot = Snap;
    }

    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    struct Snap(u64);

    impl omnipaxos::storage::Snapshot<Cmd> for Snap {
        fn create(entries: &[Cmd]) -> Self {
            Self(entries.iter().map(|cmd| cmd.0).max().unwrap_or(0))
        }
        fn merge(&mut self, other: Self) {
            self.0 = self.0.max(other.0);
        }
        fn use_snapshots() -> bool {
            true
        }
    }

    #[test]
    fn empty_log_defaults() {
        let storage: MemStorage<Cmd> = MemStorage::new();
        assert_eq!(storage.get_log_len().unwrap(), 0);
        assert_eq!(storage.get_decided_idx().unwrap(), 0);
        assert_eq!(storage.get_compacted_idx().unwrap(), 0);
        assert!(storage.get_promise().unwrap().is_none());
        assert!(storage.get_accepted_round().unwrap().is_none());
        assert!(storage.get_snapshot().unwrap().is_none());
        assert!(storage.get_stopsign().unwrap().is_none());
    }

    #[test]
    fn append_and_read_round_trip() {
        let mut storage: MemStorage<Cmd> = MemStorage::new();
        let len = storage
            .append_entries(vec![Cmd(1), Cmd(2), Cmd(3)])
            .unwrap();
        assert_eq!(len, 3);
        let suffix = storage.get_suffix(0).unwrap();
        assert_eq!(suffix.len(), 3);
        assert_eq!(suffix[0].0, 1);
        assert_eq!(suffix[2].0, 3);
    }

    #[test]
    fn get_entries_returns_empty_on_gap() {
        // Contract: any missing index in [from, to) yields an empty Vec,
        // never a partial prefix.
        let mut storage: MemStorage<Cmd> = MemStorage::new();
        storage.append_entries(vec![Cmd(1), Cmd(2)]).unwrap();
        // Range [0, 5) crosses missing indices 2, 3, 4.
        assert!(storage.get_entries(0, 5).unwrap().is_empty());
        // Fully-present range [0, 2) returns the two entries.
        let present = storage.get_entries(0, 2).unwrap();
        assert_eq!(present.len(), 2);
    }

    #[test]
    fn trim_then_compacted_yields_physical_remaining_length() {
        let mut storage: MemStorage<Cmd> = MemStorage::new();
        storage
            .append_entries(vec![Cmd(1), Cmd(2), Cmd(3), Cmd(4)])
            .unwrap();
        storage.trim(2).unwrap();
        storage.set_compacted_idx(2).unwrap();
        // Physical remaining = next_abs (4) - compacted (2) = 2.
        assert_eq!(storage.get_log_len().unwrap(), 2);
        let suffix = storage.get_suffix(2).unwrap();
        assert_eq!(suffix.len(), 2);
        assert_eq!(suffix[0].0, 3);
    }

    #[test]
    fn append_entry_after_full_trim_writes_at_compacted_idx() {
        // Conformance with production RocksdbStorage: after a snapshot
        // trims every physical log key, the next append must continue at
        // the absolute index `compacted_idx`, not reset to 0. Otherwise
        // the entry lands below the compaction floor and is unreachable
        // via get_suffix(compacted_idx).
        let mut storage: MemStorage<Cmd> = MemStorage::new();
        storage
            .append_entries(vec![Cmd(1), Cmd(2), Cmd(3), Cmd(4)])
            .unwrap();
        storage.trim(4).unwrap();
        storage.set_compacted_idx(4).unwrap();

        let new_len = storage.append_entry(Cmd(99)).unwrap();
        // physical remaining = (compacted + 1) - compacted = 1
        assert_eq!(new_len, 1);
        assert_eq!(storage.get_log_len().unwrap(), 1);

        let suffix = storage.get_suffix(4).unwrap();
        assert_eq!(suffix.len(), 1, "entry must be reachable at absolute idx 4");
        assert_eq!(suffix[0].0, 99);

        let range = storage.get_entries(4, 5).unwrap();
        assert_eq!(range.len(), 1);
        assert_eq!(range[0].0, 99);

        // No phantom write at L/0: the entry lives at L/4, so get_suffix(0)
        // returns exactly the same single entry.
        assert_eq!(storage.get_suffix(0).unwrap().len(), 1);
    }

    #[test]
    fn append_entries_after_full_trim_writes_at_compacted_idx() {
        // Same conformance guarantee for the batch append path.
        let mut storage: MemStorage<Cmd> = MemStorage::new();
        storage
            .append_entries(vec![Cmd(1), Cmd(2), Cmd(3)])
            .unwrap();
        storage.trim(3).unwrap();
        storage.set_compacted_idx(3).unwrap();

        let new_len = storage.append_entries(vec![Cmd(77), Cmd(88)]).unwrap();
        // physical remaining = (compacted + 2) - compacted = 2
        assert_eq!(new_len, 2);
        assert_eq!(storage.get_log_len().unwrap(), 2);

        let suffix = storage.get_suffix(3).unwrap();
        assert_eq!(suffix.len(), 2, "entries must continue from absolute idx 3");
        assert_eq!(suffix[0].0, 77);
        assert_eq!(suffix[1].0, 88);

        // No phantom writes below the compaction floor.
        assert_eq!(storage.get_suffix(0).unwrap().len(), 2);
    }

    #[test]
    fn promise_round_trip() {
        let mut storage: MemStorage<Cmd> = MemStorage::new();
        let ballot = Ballot {
            config_id: 1,
            n: 5,
            priority: 0,
            pid: 2,
        };
        storage.set_promise(ballot).unwrap();
        let got = storage.get_promise().unwrap().expect("present");
        assert_eq!(got.n, 5);
        assert_eq!(got.pid, 2);
    }

    #[test]
    fn snapshot_round_trip_and_clear() {
        let mut storage: MemStorage<Cmd> = MemStorage::new();
        storage.set_snapshot(Some(Snap(99))).unwrap();
        assert_eq!(storage.get_snapshot().unwrap().expect("present").0, 99);
        storage.set_snapshot(None).unwrap();
        assert!(storage.get_snapshot().unwrap().is_none());
    }
}
