#![cfg(feature = "rocksdb-log-store")]

use std::sync::Arc;

use openraft_toolkit::{Flat, RocksdbLogStore};
use rocksdb::{ColumnFamilyDescriptor, DB, Options};
use tempfile::TempDir;

mod common;
use common::TestTypeConfig;

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
