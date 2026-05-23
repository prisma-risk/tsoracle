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

//! Basic RocksDB storage round-trip tests: append + read, prefix replacement,
//! and persistence across DB close / reopen.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use common::{TEST_CF, TestCommand, open_rocksdb_in_tempdir};
use omnipaxos::storage::Storage;
use rocksdb::{ColumnFamilyDescriptor, DB, Options};
use tempfile::TempDir;
use tsoracle_paxos_toolkit::storage::RocksdbStorage;

#[test]
fn append_get_round_trip_over_rocksdb() {
    let (_dir, database) = open_rocksdb_in_tempdir(TEST_CF);
    let mut storage: RocksdbStorage<TestCommand> =
        RocksdbStorage::open_in(database, TEST_CF).expect("open_in");
    assert_eq!(storage.get_log_len().unwrap(), 0);
    let len = storage
        .append_entries(vec![TestCommand(1), TestCommand(2), TestCommand(3)])
        .unwrap();
    assert_eq!(len, 3);
    let suffix = storage.get_suffix(0).unwrap();
    assert_eq!(suffix.len(), 3);
    assert_eq!(suffix[0].0, 1);
    assert_eq!(suffix[2].0, 3);
}

#[test]
fn append_on_prefix_truncates_then_appends_over_rocksdb() {
    let (_dir, database) = open_rocksdb_in_tempdir(TEST_CF);
    let mut storage: RocksdbStorage<TestCommand> =
        RocksdbStorage::open_in(database, TEST_CF).expect("open_in");
    storage
        .append_entries(vec![TestCommand(1), TestCommand(2), TestCommand(3)])
        .unwrap();
    storage
        .append_on_prefix(1, vec![TestCommand(9), TestCommand(8)])
        .unwrap();
    let suffix = storage.get_suffix(0).unwrap();
    assert_eq!(suffix.len(), 3);
    assert_eq!(suffix[0].0, 1);
    assert_eq!(suffix[1].0, 9);
    assert_eq!(suffix[2].0, 8);
}

#[test]
fn reopen_preserves_log_across_db_close() {
    let dir = TempDir::new().unwrap();
    {
        let mut storage = open_rocksdb_storage_in(&dir, TEST_CF);
        storage
            .append_entries(vec![TestCommand(7), TestCommand(8)])
            .unwrap();
    }
    {
        let storage = open_rocksdb_storage_in(&dir, TEST_CF);
        let suffix = storage.get_suffix(0).unwrap();
        assert_eq!(suffix.len(), 2);
        assert_eq!(suffix[0].0, 7);
        assert_eq!(suffix[1].0, 8);
    }
}

fn open_rocksdb_storage_in(dir: &TempDir, cf_name: &str) -> RocksdbStorage<TestCommand> {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    let cf = ColumnFamilyDescriptor::new(cf_name, Options::default());
    let database = Arc::new(DB::open_cf_descriptors(&opts, dir.path(), vec![cf]).expect("open db"));
    RocksdbStorage::open_in(database, cf_name).expect("open_in")
}
