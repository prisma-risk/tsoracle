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

#![cfg(feature = "rocksdb-log-store")]

use std::sync::Arc;

use openraft::entry::RaftEntry;
use openraft::storage::{IOFlushed, RaftLogReader, RaftLogStorage};
use openraft::{Entry, LogId, Vote};
use rocksdb::{ColumnFamilyDescriptor, DB, Options};
use tempfile::TempDir;
use tsoracle_openraft_toolkit::{Flat, GroupPrefixed, KeySpace, MetaLabel, RocksdbLogStore};

mod common;
use common::{TestLeaderId, TestTypeConfig};

const LOG_CF: &str = "raft_log";
const META_CF: &str = "raft_meta";

fn open_db(dir: &TempDir) -> Arc<DB> {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    let cfs = vec![
        ColumnFamilyDescriptor::new(LOG_CF, Options::default()),
        ColumnFamilyDescriptor::new(META_CF, Options::default()),
    ];
    Arc::new(DB::open_cf_descriptors(&opts, dir.path(), cfs).unwrap())
}

#[tokio::test]
async fn opens_empty_store_without_error() {
    let dir = TempDir::new().unwrap();
    let db = open_db(&dir);
    let store: RocksdbLogStore<TestTypeConfig, Flat> =
        RocksdbLogStore::open(db, LOG_CF, META_CF, Flat).unwrap();
    // The Debug impl is part of the public surface — exercise it so a future
    // edit that breaks formatting is caught before it ships.
    let rendered = format!("{store:?}");
    assert!(rendered.contains("RocksdbLogStore"));
    assert!(rendered.contains(LOG_CF));
    assert!(rendered.contains(META_CF));
    // Clone is also public; verify the cheap-clone contract holds and the
    // clone produces equivalent Debug output.
    let cloned = store.clone();
    assert_eq!(format!("{cloned:?}"), rendered);
}

#[test]
fn open_fails_when_log_cf_missing() {
    let dir = TempDir::new().unwrap();
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    let cfs = vec![ColumnFamilyDescriptor::new(META_CF, Options::default())];
    let db = Arc::new(DB::open_cf_descriptors(&opts, dir.path(), cfs).unwrap());
    let err = RocksdbLogStore::<TestTypeConfig, Flat>::open(db, LOG_CF, META_CF, Flat).unwrap_err();
    assert!(
        matches!(err, tsoracle_openraft_toolkit::RocksdbLogStoreError::MissingColumnFamily(ref s) if s == LOG_CF)
    );
}

#[tokio::test]
async fn save_and_read_vote_roundtrips() {
    let dir = TempDir::new().unwrap();
    let db = open_db(&dir);
    let mut store: RocksdbLogStore<TestTypeConfig, Flat> =
        RocksdbLogStore::open(db, LOG_CF, META_CF, Flat).unwrap();

    let vote: Vote<TestLeaderId> = Vote::new_committed(7, 3);
    store.save_vote(&vote).await.unwrap();
    let got = store.read_vote().await.unwrap();
    assert_eq!(got, Some(vote));
}

#[tokio::test]
async fn empty_store_log_state_is_empty() {
    let dir = TempDir::new().unwrap();
    let db = open_db(&dir);
    let mut store: RocksdbLogStore<TestTypeConfig, Flat> =
        RocksdbLogStore::open(db, LOG_CF, META_CF, Flat).unwrap();

    let state = store.get_log_state().await.unwrap();
    assert!(state.last_purged_log_id.is_none());
    assert!(state.last_log_id.is_none());
}

#[tokio::test]
async fn save_and_read_committed_roundtrips() {
    let dir = TempDir::new().unwrap();
    let db = open_db(&dir);
    let mut store: RocksdbLogStore<TestTypeConfig, Flat> =
        RocksdbLogStore::open(db, LOG_CF, META_CF, Flat).unwrap();

    assert!(store.read_committed().await.unwrap().is_none());

    let log_id: LogId<TestLeaderId> = LogId::new(
        TestLeaderId {
            term: 7,
            node_id: 3,
        },
        2,
    );
    store.save_committed(Some(log_id)).await.unwrap();
    assert_eq!(store.read_committed().await.unwrap(), Some(log_id));
}

// `save_committed(None)` after a `Some(...)` write must clear the stored value.
// This is the only call path that exercises `meta::delete`; the openraft
// conformance suite never drives this transition, so without this test the
// delete helper sits at 0% coverage.
#[tokio::test]
async fn save_committed_with_none_clears_existing_record() {
    let dir = TempDir::new().unwrap();
    let db = open_db(&dir);
    let mut store: RocksdbLogStore<TestTypeConfig, Flat> =
        RocksdbLogStore::open(db, LOG_CF, META_CF, Flat).unwrap();

    let log_id: LogId<TestLeaderId> = LogId::new(
        TestLeaderId {
            term: 4,
            node_id: 1,
        },
        9,
    );
    store.save_committed(Some(log_id)).await.unwrap();
    assert_eq!(store.read_committed().await.unwrap(), Some(log_id));

    store.save_committed(None).await.unwrap();
    assert!(store.read_committed().await.unwrap().is_none());
}

// Corrupt the bytes stored at the Vote key, then verify `read_vote` surfaces
// the decode error rather than silently returning `None` or panicking. Drives
// the `postcard::from_bytes` error arm in `meta::read` which is unreachable
// from any legitimate API call sequence.
#[tokio::test]
async fn read_vote_surfaces_decode_error_on_corrupted_meta() {
    let dir = TempDir::new().unwrap();
    let db = open_db(&dir);

    // Inject garbage bytes under the `Flat` keyspace's Vote key directly via the
    // shared `Arc<DB>` — the public store API has no "write raw bytes" door.
    let key = Flat.meta_key(MetaLabel::Vote);
    let cf = db.cf_handle(META_CF).unwrap();
    db.put_cf(&cf, &key, b"not a valid postcard-encoded vote")
        .unwrap();

    let mut store: RocksdbLogStore<TestTypeConfig, Flat> =
        RocksdbLogStore::open(Arc::clone(&db), LOG_CF, META_CF, Flat).unwrap();

    let err = store
        .read_vote()
        .await
        .expect_err("read_vote should propagate the decode failure");
    // The exact message comes from postcard; we just want to confirm an error
    // path actually fires rather than asserting a brittle substring.
    let _ = err.to_string();
}

// Regression test for a cross-group keyspace leak in `last_log_id_in_cf`.
//
// When two `GroupPrefixed` keyspaces share a single log column family — the
// whole point of `GroupPrefixed` — the reverse-scan implementation of
// `last_log_id_in_cf` seeks from `hi` of the *current* group and returns the
// first key that is `<= hi`. It does **not** check that the returned key is
// `>= lo`. If the current group has no entries but a numerically lower group
// does, the iterator surfaces the lower group's last entry as if it belonged
// to the current group. `get_log_state` then reports a `last_log_id` that
// the current group never wrote.
//
// Expected (correct) behaviour: an empty group reports `last_log_id: None`
// regardless of what other groups in the same CF have written.
#[tokio::test]
async fn get_log_state_isolates_groups_in_shared_column_family() {
    let dir = TempDir::new().unwrap();
    let db = open_db(&dir);

    // Group 4 has one entry at index 100.
    let mut store_g4: RocksdbLogStore<TestTypeConfig, GroupPrefixed> =
        RocksdbLogStore::open(Arc::clone(&db), LOG_CF, META_CF, GroupPrefixed::new(4)).unwrap();
    let entry_g4: Entry<TestLeaderId, common::TestAppData, u64, common::TestPeer> =
        Entry::new_blank(LogId::new(
            TestLeaderId {
                term: 1,
                node_id: 1,
            },
            100,
        ));
    store_g4
        .append(std::iter::once(entry_g4), IOFlushed::noop())
        .await
        .unwrap();

    // Group 5 shares the same CF but has written nothing.
    let mut store_g5: RocksdbLogStore<TestTypeConfig, GroupPrefixed> =
        RocksdbLogStore::open(Arc::clone(&db), LOG_CF, META_CF, GroupPrefixed::new(5)).unwrap();

    let state = store_g5.get_log_state().await.unwrap();
    assert!(
        state.last_log_id.is_none(),
        "group 5 has no entries but get_log_state returned {:?}; \
         last_log_id_in_cf leaked group 4's entry across the keyspace boundary",
        state.last_log_id,
    );
    assert!(state.last_purged_log_id.is_none());

    // Sanity: group 4 still sees its own entry, so the bug is direction-specific
    // (reverse scan ignores `lo`, not a generic isolation failure).
    let state_g4 = store_g4.get_log_state().await.unwrap();
    assert_eq!(
        state_g4.last_log_id.as_ref().map(|id| id.index),
        Some(100),
        "group 4 should still observe its own entry",
    );
}

fn blank_entry_at(index: u64) -> Entry<TestLeaderId, common::TestAppData, u64, common::TestPeer> {
    Entry::new_blank(LogId::new(
        TestLeaderId {
            term: 1,
            node_id: 1,
        },
        index,
    ))
}

// truncate_after(Some(k)) must keep [0..=k] and remove the tail (k+1..=N)
// exactly — no over-delete at the boundary, no surviving tail entry.
#[tokio::test]
async fn truncate_after_removes_exact_tail() {
    let dir = TempDir::new().unwrap();
    let db = open_db(&dir);
    let mut store: RocksdbLogStore<TestTypeConfig, Flat> =
        RocksdbLogStore::open(db, LOG_CF, META_CF, Flat).unwrap();

    store
        .append((0..=5).map(blank_entry_at), IOFlushed::noop())
        .await
        .unwrap();

    let keep_through = LogId::new(
        TestLeaderId {
            term: 1,
            node_id: 1,
        },
        3,
    );
    store.truncate_after(Some(keep_through)).await.unwrap();

    let surviving = store.try_get_log_entries(0..=5).await.unwrap();
    let indices: Vec<u64> = surviving.iter().map(|e| e.log_id().index).collect();
    assert_eq!(
        indices,
        vec![0, 1, 2, 3],
        "tail [4,5] must be gone, [0..=3] kept"
    );
}

// truncate_after(None) empties the log entirely.
#[tokio::test]
async fn truncate_after_none_empties_log() {
    let dir = TempDir::new().unwrap();
    let db = open_db(&dir);
    let mut store: RocksdbLogStore<TestTypeConfig, Flat> =
        RocksdbLogStore::open(db, LOG_CF, META_CF, Flat).unwrap();

    store
        .append((0..=3).map(blank_entry_at), IOFlushed::noop())
        .await
        .unwrap();

    store.truncate_after(None).await.unwrap();

    let surviving = store.try_get_log_entries(0..=3).await.unwrap();
    assert!(
        surviving.is_empty(),
        "truncate_after(None) must remove all entries"
    );
    let state = store.get_log_state().await.unwrap();
    assert!(state.last_log_id.is_none());
}

// purge(log_id@index=k) removes [0..=k] inclusive and keeps [k+1..=N]. Pins
// the inclusive-of-index semantics that the `index+1` exclusive end encodes,
// and that LastPurged reads back as the purged log_id.
#[tokio::test]
async fn purge_removes_inclusive_prefix() {
    let dir = TempDir::new().unwrap();
    let db = open_db(&dir);
    let mut store: RocksdbLogStore<TestTypeConfig, Flat> =
        RocksdbLogStore::open(db, LOG_CF, META_CF, Flat).unwrap();

    store
        .append((0..=5).map(blank_entry_at), IOFlushed::noop())
        .await
        .unwrap();

    let purge_through = LogId::new(
        TestLeaderId {
            term: 1,
            node_id: 1,
        },
        2,
    );
    store.purge(purge_through).await.unwrap();

    let surviving = store.try_get_log_entries(0..=5).await.unwrap();
    let indices: Vec<u64> = surviving.iter().map(|e| e.log_id().index).collect();
    assert_eq!(
        indices,
        vec![3, 4, 5],
        "[0..=2] inclusive must be purged, [3..=5] kept"
    );

    let state = store.get_log_state().await.unwrap();
    assert_eq!(
        state.last_purged_log_id.map(|id| id.index),
        Some(2),
        "purge must record LastPurged at the purged index",
    );
}

// A range delete on group g must never reach group (g+1)'s entries sharing the
// same column family — the GroupPrefixed isolation guarantee at the storage
// layer (mirrors the key_space proptest, but end-to-end through RocksDB).
#[tokio::test]
async fn truncate_on_one_group_leaves_neighbor_intact() {
    let dir = TempDir::new().unwrap();
    let db = open_db(&dir);

    let mut store_g4: RocksdbLogStore<TestTypeConfig, GroupPrefixed> =
        RocksdbLogStore::open(Arc::clone(&db), LOG_CF, META_CF, GroupPrefixed::new(4)).unwrap();
    let mut store_g5: RocksdbLogStore<TestTypeConfig, GroupPrefixed> =
        RocksdbLogStore::open(Arc::clone(&db), LOG_CF, META_CF, GroupPrefixed::new(5)).unwrap();

    store_g4
        .append((0..=3).map(blank_entry_at), IOFlushed::noop())
        .await
        .unwrap();
    store_g5
        .append((0..=3).map(blank_entry_at), IOFlushed::noop())
        .await
        .unwrap();

    // Wipe all of group 4's tail; group 5 must be untouched.
    store_g4.truncate_after(None).await.unwrap();

    let g4 = store_g4.try_get_log_entries(0..=3).await.unwrap();
    assert!(
        g4.is_empty(),
        "group 4 should be empty after truncate_after(None)"
    );

    let g5: Vec<u64> = store_g5
        .try_get_log_entries(0..=3)
        .await
        .unwrap()
        .iter()
        .map(|e| e.log_id().index)
        .collect();
    assert_eq!(
        g5,
        vec![0, 1, 2, 3],
        "group 5's entries must survive group 4's range delete"
    );
}
