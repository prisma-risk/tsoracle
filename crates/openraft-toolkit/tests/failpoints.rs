#![cfg(feature = "failpoints")]

use std::sync::Arc;

use openraft::entry::RaftEntry;
use openraft::storage::{IOFlushed, RaftLogStorage};
use openraft::{Entry, LogId};
use openraft_toolkit::{Flat, RocksdbLogStore};
use rocksdb::{ColumnFamilyDescriptor, DB, Options};
use tempfile::TempDir;

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

fn blank_entry_at(index: u64) -> Entry<TestLeaderId, common::TestAppData, u64, common::TestPeer> {
    Entry::new_blank(LogId::new(
        TestLeaderId {
            term: 1,
            node_id: 1,
        },
        index,
    ))
}

/// `openraft_toolkit::log_store::before_write_batch` fires immediately before
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
    let _scenario = fail::FailScenario::setup();

    let dir = TempDir::new().unwrap();
    let db = open_db(&dir);

    fail::cfg("openraft_toolkit::log_store::before_write_batch", "panic").unwrap();

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

    fail::cfg("openraft_toolkit::log_store::before_write_batch", "off").unwrap();

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

/// `openraft_toolkit::log_store::after_write_before_sync` fires after the
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
    let _scenario = fail::FailScenario::setup();

    let dir = TempDir::new().unwrap();
    let db = open_db(&dir);
    let mut store: RocksdbLogStore<TestTypeConfig, Flat> =
        RocksdbLogStore::open(Arc::clone(&db), LOG_CF, META_CF, Flat).unwrap();

    fail::cfg(
        "openraft_toolkit::log_store::after_write_before_sync",
        "return",
    )
    .unwrap();
    let result = store
        .append(std::iter::once(blank_entry_at(42)), IOFlushed::noop())
        .await;
    fail::cfg(
        "openraft_toolkit::log_store::after_write_before_sync",
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
