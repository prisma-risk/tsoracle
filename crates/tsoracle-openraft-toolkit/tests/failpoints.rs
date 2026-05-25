//
//  ░▀█▀░█▀▀░█▀█░█▀▄░█▀█░█▀▀░█░░░█▀▀
//  ░░█░░▀▀█░█░█░█▀▄░█▀█░█░░░█░░░█▀▀
//  ░░▀░░▀▀▀░▀▀▀░▀░▀░▀░▀░▀▀▀░▀▀▀░▀▀▀
//
//  tsoracle — Distributed Timestamp Oracle
//  https://www.tsoracle.rs
//
//  Copyright (c) 2026 Prisma Risk
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

#![cfg(feature = "failpoints")]

use std::sync::Arc;

use openraft::entry::RaftEntry;
use openraft::storage::{IOFlushed, RaftLogStorage};
use openraft::{Entry, LogId};
use rocksdb::{ColumnFamilyDescriptor, DB, Options};
use tempfile::TempDir;
use tsoracle_openraft_toolkit::{Flat, RocksdbLogStore};

mod common;
use common::{TestLeaderId, TestTypeConfig};

const LOG_CF: &str = "raft_log";
const META_CF: &str = "raft_meta";

/// Body-level serialization: the `fail` registry is process-global; even with
/// `FailScenario::setup` snapshotting between tests, multiple test bodies
/// sharing the same registered name will interleave their configurations.
/// Mirrors the pattern in `tsoracle-driver-file/tests/failpoints.rs`.
static FAILPOINT_TEST_SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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

fn log_id_at(index: u64) -> LogId<TestLeaderId> {
    LogId::new(
        TestLeaderId {
            term: 1,
            node_id: 1,
        },
        index,
    )
}

fn blank_entry_at(index: u64) -> Entry<TestLeaderId, common::TestAppData, u64, common::TestPeer> {
    Entry::new_blank(log_id_at(index))
}

/// `tsoracle_openraft_toolkit::log_store::before_write_batch` fires immediately before
/// `db.write_opt(batch, ...)`. A `panic` action terminates the task before the
/// batch reaches RocksDB; after reopening the store, the log column family
/// must still be empty. If a regression moves the failpoint to after the
/// write, this test fails because `last_log_id` becomes `Some(_)`.
///
/// The append runs inside `tokio::spawn` so the panic surfaces as a
/// `JoinError` we can assert on, instead of unwinding the test task itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn panic_at_before_write_batch_leaves_log_empty() {
    let _serial = FAILPOINT_TEST_SERIAL.lock().await;
    let _scenario = tsoracle_failpoint::fail::FailScenario::setup();

    let dir = TempDir::new().unwrap();
    let db = open_db(&dir);

    tsoracle_failpoint::fail::cfg(
        "tsoracle_openraft_toolkit::log_store::before_write_batch",
        "panic",
    )
    .unwrap();

    let writer_db = Arc::clone(&db);
    let join = tokio::spawn(async move {
        let mut store: RocksdbLogStore<TestTypeConfig, Flat> =
            RocksdbLogStore::open(writer_db, LOG_CF, META_CF, Flat).unwrap();
        store
            .append(std::iter::once(blank_entry_at(1)), IOFlushed::noop())
            .await
    });
    let join_err = join
        .await
        .expect_err("expected the panic action to surface as a JoinError");
    assert!(
        join_err.is_panic(),
        "expected JoinError::is_panic(), got {join_err:?}"
    );

    tsoracle_failpoint::fail::cfg(
        "tsoracle_openraft_toolkit::log_store::before_write_batch",
        "off",
    )
    .unwrap();

    let mut store: RocksdbLogStore<TestTypeConfig, Flat> =
        RocksdbLogStore::open(db, LOG_CF, META_CF, Flat).unwrap();
    let state = store.get_log_state().await.unwrap();
    assert!(
        state.last_log_id.is_none(),
        "panic at before_write_batch must fire before db.write_opt; \
         expected empty log but get_log_state returned last_log_id = {:?}",
        state.last_log_id,
    );
}

/// `tsoracle_openraft_toolkit::log_store::after_write_before_sync` fires after the
/// rocksdb write returns and before `callback.io_completed(...)`. A `return`
/// action makes `append` produce `Err(io::Error)` while the WriteBatch has
/// already been applied — the entry is durable on disk even though the
/// openraft IO-completion notification never fired. After reopening the
/// store the entry is observable via `get_log_state`. If a regression moves
/// the failpoint to before the write, this test fails because the log stays
/// empty.
#[tokio::test]
async fn return_at_after_write_before_sync_persists_entry() {
    let _serial = FAILPOINT_TEST_SERIAL.lock().await;
    let _scenario = tsoracle_failpoint::fail::FailScenario::setup();

    let dir = TempDir::new().unwrap();
    let db = open_db(&dir);
    let mut store: RocksdbLogStore<TestTypeConfig, Flat> =
        RocksdbLogStore::open(Arc::clone(&db), LOG_CF, META_CF, Flat).unwrap();

    tsoracle_failpoint::fail::cfg(
        "tsoracle_openraft_toolkit::log_store::after_write_before_sync",
        "return",
    )
    .unwrap();
    let result = store
        .append(std::iter::once(blank_entry_at(42)), IOFlushed::noop())
        .await;
    tsoracle_failpoint::fail::cfg(
        "tsoracle_openraft_toolkit::log_store::after_write_before_sync",
        "off",
    )
    .unwrap();
    assert!(
        result.is_err(),
        "expected return action to surface as Err from append, got {result:?}"
    );

    drop(store);
    let mut store: RocksdbLogStore<TestTypeConfig, Flat> =
        RocksdbLogStore::open(db, LOG_CF, META_CF, Flat).unwrap();
    let state = store.get_log_state().await.unwrap();
    assert_eq!(
        state.last_log_id.as_ref().map(|id| id.index),
        Some(42),
        "return at after_write_before_sync must fire after db.write_opt; \
         expected last_log_id index = 42 but get_log_state returned {:?}",
        state.last_log_id,
    );
}

/// Seed three durable entries (indices 1..=3) into a fresh store, then drop it.
async fn seed_three_entries(db: &Arc<DB>) {
    let mut store: RocksdbLogStore<TestTypeConfig, Flat> =
        RocksdbLogStore::open(Arc::clone(db), LOG_CF, META_CF, Flat).unwrap();
    store
        .append((1..=3).map(blank_entry_at), IOFlushed::noop())
        .await
        .unwrap();
}

/// `tsoracle_openraft_toolkit::log_store::truncate::before_write_batch` fires
/// immediately before `db.write_opt(batch, ...)` in `truncate_after`. A `panic`
/// action terminates the task before the deletions reach RocksDB; after
/// reopening, the log tail must be intact (`last_log_id` index = 3). If a
/// regression moves the failpoint to after the write, the truncation lands and
/// `last_log_id` drops to 1, failing this test.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn panic_at_truncate_before_write_batch_leaves_log_intact() {
    let _serial = FAILPOINT_TEST_SERIAL.lock().await;
    let _scenario = tsoracle_failpoint::fail::FailScenario::setup();

    let dir = TempDir::new().unwrap();
    let db = open_db(&dir);
    seed_three_entries(&db).await;

    tsoracle_failpoint::fail::cfg(
        "tsoracle_openraft_toolkit::log_store::truncate::before_write_batch",
        "panic",
    )
    .unwrap();

    let writer_db = Arc::clone(&db);
    let join = tokio::spawn(async move {
        let mut store: RocksdbLogStore<TestTypeConfig, Flat> =
            RocksdbLogStore::open(writer_db, LOG_CF, META_CF, Flat).unwrap();
        store.truncate_after(Some(log_id_at(1))).await
    });
    let join_err = join
        .await
        .expect_err("expected the panic action to surface as a JoinError");
    assert!(
        join_err.is_panic(),
        "expected JoinError::is_panic(), got {join_err:?}"
    );

    tsoracle_failpoint::fail::cfg(
        "tsoracle_openraft_toolkit::log_store::truncate::before_write_batch",
        "off",
    )
    .unwrap();

    let mut store: RocksdbLogStore<TestTypeConfig, Flat> =
        RocksdbLogStore::open(db, LOG_CF, META_CF, Flat).unwrap();
    let state = store.get_log_state().await.unwrap();
    assert_eq!(
        state.last_log_id.as_ref().map(|id| id.index),
        Some(3),
        "panic at truncate::before_write_batch must fire before db.write_opt; \
         expected the log tail intact (last_log_id index = 3) but got {:?}",
        state.last_log_id,
    );
}

/// `tsoracle_openraft_toolkit::log_store::truncate::after_write_before_sync`
/// fires after the rocksdb write returns. A `return` action makes
/// `truncate_after` produce `Err(io::Error)` while the WriteBatch has already
/// been applied — the truncation is durable even though the call reported
/// failure. After reopening, `last_log_id` reflects the truncation (index = 1).
/// If a regression moves the failpoint to before the write, the truncation is
/// lost and `last_log_id` stays 3, failing this test.
#[tokio::test]
async fn return_at_truncate_after_write_before_sync_persists_truncation() {
    let _serial = FAILPOINT_TEST_SERIAL.lock().await;
    let _scenario = tsoracle_failpoint::fail::FailScenario::setup();

    let dir = TempDir::new().unwrap();
    let db = open_db(&dir);
    seed_three_entries(&db).await;

    let mut store: RocksdbLogStore<TestTypeConfig, Flat> =
        RocksdbLogStore::open(Arc::clone(&db), LOG_CF, META_CF, Flat).unwrap();
    tsoracle_failpoint::fail::cfg(
        "tsoracle_openraft_toolkit::log_store::truncate::after_write_before_sync",
        "return",
    )
    .unwrap();
    let result = store.truncate_after(Some(log_id_at(1))).await;
    tsoracle_failpoint::fail::cfg(
        "tsoracle_openraft_toolkit::log_store::truncate::after_write_before_sync",
        "off",
    )
    .unwrap();
    assert!(
        result.is_err(),
        "expected return action to surface as Err from truncate_after, got {result:?}"
    );

    drop(store);
    let mut store: RocksdbLogStore<TestTypeConfig, Flat> =
        RocksdbLogStore::open(db, LOG_CF, META_CF, Flat).unwrap();
    let state = store.get_log_state().await.unwrap();
    assert_eq!(
        state.last_log_id.as_ref().map(|id| id.index),
        Some(1),
        "return at truncate::after_write_before_sync must fire after db.write_opt; \
         expected the truncation persisted (last_log_id index = 1) but got {:?}",
        state.last_log_id,
    );
}

/// `tsoracle_openraft_toolkit::log_store::purge::before_write_batch` fires
/// immediately before `db.write_opt(batch, ...)` in `purge`. A `panic` action
/// terminates the task before the deletions and the `LastPurged` marker reach
/// RocksDB; after reopening, nothing must be purged (`last_purged_log_id` is
/// `None`). If a regression moves the failpoint to after the write, the purge
/// lands and `last_purged_log_id` becomes `Some(_)`, failing this test.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn panic_at_purge_before_write_batch_leaves_log_intact() {
    let _serial = FAILPOINT_TEST_SERIAL.lock().await;
    let _scenario = tsoracle_failpoint::fail::FailScenario::setup();

    let dir = TempDir::new().unwrap();
    let db = open_db(&dir);
    seed_three_entries(&db).await;

    tsoracle_failpoint::fail::cfg(
        "tsoracle_openraft_toolkit::log_store::purge::before_write_batch",
        "panic",
    )
    .unwrap();

    let writer_db = Arc::clone(&db);
    let join = tokio::spawn(async move {
        let mut store: RocksdbLogStore<TestTypeConfig, Flat> =
            RocksdbLogStore::open(writer_db, LOG_CF, META_CF, Flat).unwrap();
        store.purge(log_id_at(2)).await
    });
    let join_err = join
        .await
        .expect_err("expected the panic action to surface as a JoinError");
    assert!(
        join_err.is_panic(),
        "expected JoinError::is_panic(), got {join_err:?}"
    );

    tsoracle_failpoint::fail::cfg(
        "tsoracle_openraft_toolkit::log_store::purge::before_write_batch",
        "off",
    )
    .unwrap();

    let mut store: RocksdbLogStore<TestTypeConfig, Flat> =
        RocksdbLogStore::open(db, LOG_CF, META_CF, Flat).unwrap();
    let state = store.get_log_state().await.unwrap();
    assert!(
        state.last_purged_log_id.is_none(),
        "panic at purge::before_write_batch must fire before db.write_opt; \
         expected nothing purged but get_log_state returned last_purged_log_id = {:?}",
        state.last_purged_log_id,
    );
}

/// `tsoracle_openraft_toolkit::log_store::purge::after_write_before_sync` fires
/// after the rocksdb write returns. A `return` action makes `purge` produce
/// `Err(io::Error)` while the WriteBatch has already been applied — the purge
/// is durable even though the call reported failure. After reopening,
/// `last_purged_log_id` reflects the purge (index = 2). If a regression moves
/// the failpoint to before the write, the purge is lost and
/// `last_purged_log_id` stays `None`, failing this test.
#[tokio::test]
async fn return_at_purge_after_write_before_sync_persists_purge() {
    let _serial = FAILPOINT_TEST_SERIAL.lock().await;
    let _scenario = tsoracle_failpoint::fail::FailScenario::setup();

    let dir = TempDir::new().unwrap();
    let db = open_db(&dir);
    seed_three_entries(&db).await;

    let mut store: RocksdbLogStore<TestTypeConfig, Flat> =
        RocksdbLogStore::open(Arc::clone(&db), LOG_CF, META_CF, Flat).unwrap();
    tsoracle_failpoint::fail::cfg(
        "tsoracle_openraft_toolkit::log_store::purge::after_write_before_sync",
        "return",
    )
    .unwrap();
    let result = store.purge(log_id_at(2)).await;
    tsoracle_failpoint::fail::cfg(
        "tsoracle_openraft_toolkit::log_store::purge::after_write_before_sync",
        "off",
    )
    .unwrap();
    assert!(
        result.is_err(),
        "expected return action to surface as Err from purge, got {result:?}"
    );

    drop(store);
    let mut store: RocksdbLogStore<TestTypeConfig, Flat> =
        RocksdbLogStore::open(db, LOG_CF, META_CF, Flat).unwrap();
    let state = store.get_log_state().await.unwrap();
    assert_eq!(
        state.last_purged_log_id.as_ref().map(|id| id.index),
        Some(2),
        "return at purge::after_write_before_sync must fire after db.write_opt; \
         expected the purge persisted (last_purged_log_id index = 2) but got {:?}",
        state.last_purged_log_id,
    );
}
