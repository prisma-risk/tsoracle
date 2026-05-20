#![cfg(feature = "rocksdb-log-store")]

use std::sync::Arc;

use openraft::storage::{RaftLogReader, RaftLogStorage};
use openraft::{LogId, Vote};
use openraft_toolkit::{Flat, KeySpace, MetaLabel, RocksdbLogStore};
use rocksdb::{ColumnFamilyDescriptor, DB, Options};
use tempfile::TempDir;

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
    let _store: RocksdbLogStore<TestTypeConfig, Flat> =
        RocksdbLogStore::open(db, LOG_CF, META_CF, Flat).unwrap();
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
        matches!(err, openraft_toolkit::RocksdbLogStoreError::MissingColumnFamily(ref s) if s == LOG_CF)
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
// the `bincode::deserialize` error arm in `meta::read` which is unreachable
// from any legitimate API call sequence.
#[tokio::test]
async fn read_vote_surfaces_decode_error_on_corrupted_meta() {
    let dir = TempDir::new().unwrap();
    let db = open_db(&dir);

    // Inject garbage bytes under the `Flat` keyspace's Vote key directly via the
    // shared `Arc<DB>` — the public store API has no "write raw bytes" door.
    let key = Flat.meta_key(MetaLabel::Vote);
    let cf = db.cf_handle(META_CF).unwrap();
    db.put_cf(&cf, &key, b"not a valid bincode-encoded vote")
        .unwrap();

    let mut store: RocksdbLogStore<TestTypeConfig, Flat> =
        RocksdbLogStore::open(Arc::clone(&db), LOG_CF, META_CF, Flat).unwrap();

    let err = store
        .read_vote()
        .await
        .expect_err("read_vote should propagate the decode failure");
    // The exact message comes from bincode; we just want to confirm an error
    // path actually fires rather than asserting a brittle substring.
    let _ = err.to_string();
}
