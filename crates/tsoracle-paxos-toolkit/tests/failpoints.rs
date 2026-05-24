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

//! Verify the RocksDB storage honors injected failpoints.

#![cfg(all(feature = "failpoints", feature = "rocksdb-storage"))]

#[path = "common/mod.rs"]
mod common;

use common::{TEST_CF, TestCommand, open_rocksdb_in_tempdir};
use omnipaxos::storage::Storage;
use tsoracle_paxos_toolkit::storage::RocksdbStorage;

#[test]
fn append_entry_panics_when_failpoint_armed() {
    let (_dir, database) = open_rocksdb_in_tempdir(TEST_CF);
    let mut storage: RocksdbStorage<TestCommand> =
        RocksdbStorage::open_in(database, TEST_CF).expect("open_in");

    tsoracle_failpoint::fail::cfg("paxos_toolkit::storage::append_entry", "panic").unwrap();

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        storage.append_entry(TestCommand(1))
    }));

    tsoracle_failpoint::fail::remove("paxos_toolkit::storage::append_entry");
    assert!(result.is_err(), "expected panic from failpoint");
}
