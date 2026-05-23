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

//! Conformance harness: every scenario runs against both `MemStorage` and
//! `RocksdbStorage`. Any divergence is a bug in one of the impls.

#[path = "common/mod.rs"]
mod common;

use common::{TEST_CF, TestCommand, TestSnapshot, open_rocksdb_in_tempdir};
use omnipaxos::ClusterConfig;
use omnipaxos::ballot_leader_election::Ballot;
use omnipaxos::storage::{StopSign, Storage};
use tsoracle_paxos_toolkit::storage::RocksdbStorage;
use tsoracle_paxos_toolkit::test_fakes::mem_storage::MemStorage;

fn ballot(round: u32, pid: u64) -> Ballot {
    Ballot {
        config_id: 1,
        n: round,
        priority: 0,
        pid,
    }
}

fn run_scenario<S: Storage<TestCommand>>(mut storage: S) {
    // (1) Empty defaults
    assert_eq!(storage.get_log_len().unwrap(), 0);
    assert_eq!(storage.get_decided_idx().unwrap(), 0);
    assert_eq!(storage.get_compacted_idx().unwrap(), 0);
    assert!(storage.get_promise().unwrap().is_none());
    assert!(storage.get_accepted_round().unwrap().is_none());
    assert!(storage.get_snapshot().unwrap().is_none());
    assert!(storage.get_stopsign().unwrap().is_none());

    // (2) Append + read
    let new_len = storage
        .append_entries(vec![TestCommand(1), TestCommand(2), TestCommand(3)])
        .unwrap();
    assert_eq!(new_len, 3);
    assert_eq!(storage.get_log_len().unwrap(), 3);

    // (3) Half-open range
    let mid = storage.get_entries(1, 3).unwrap();
    assert_eq!(mid.len(), 2);
    assert_eq!(mid[0].0, 2);
    assert_eq!(mid[1].0, 3);

    // (4) Empty ranges
    assert!(storage.get_entries(5, 5).unwrap().is_empty());
    assert!(storage.get_entries(10, 20).unwrap().is_empty());

    // (5) append_on_prefix replaces tail
    storage
        .append_on_prefix(1, vec![TestCommand(9), TestCommand(8)])
        .unwrap();
    let after_replace = storage.get_suffix(0).unwrap();
    assert_eq!(after_replace.len(), 3);
    assert_eq!(after_replace[1].0, 9);

    // (6) Promise
    let promise = ballot(5, 2);
    storage.set_promise(promise).unwrap();
    let got = storage.get_promise().unwrap().expect("present");
    assert_eq!(got.n, 5);
    assert_eq!(got.pid, 2);

    // (7) Accepted round
    let accepted = ballot(7, 3);
    storage.set_accepted_round(accepted).unwrap();
    assert_eq!(storage.get_accepted_round().unwrap().expect("present").n, 7);

    // (8) Decided idx
    storage.set_decided_idx(2).unwrap();
    assert_eq!(storage.get_decided_idx().unwrap(), 2);

    // (9) Compacted idx
    storage.set_compacted_idx(1).unwrap();
    assert_eq!(storage.get_compacted_idx().unwrap(), 1);

    // (10) Trim + gap contract: any subsequent get_entries range that
    //      crosses the trimmed region MUST return empty Vec, never a
    //      partial prefix.
    storage.trim(1).unwrap();
    let after_trim = storage.get_suffix(0).unwrap();
    assert_eq!(after_trim.len(), 2, "suffix walks present entries");
    let gapped = storage.get_entries(0, 3).unwrap();
    assert!(
        gapped.is_empty(),
        "get_entries crossing a gap must yield empty, got {} items",
        gapped.len(),
    );
    let present = storage.get_entries(1, 3).unwrap();
    assert_eq!(present.len(), 2);

    // (11) Snapshot
    storage.set_snapshot(Some(TestSnapshot(42))).unwrap();
    assert_eq!(storage.get_snapshot().unwrap().expect("present").0, 42);
    storage.set_snapshot(None).unwrap();
    assert!(storage.get_snapshot().unwrap().is_none());

    // (12) Stopsign
    let stopsign = StopSign::with(
        ClusterConfig {
            configuration_id: 2,
            nodes: vec![1, 2, 3, 4, 5],
            flexible_quorum: None,
        },
        None,
    );
    storage.set_stopsign(Some(stopsign)).unwrap();
    let got = storage.get_stopsign().unwrap().expect("present");
    assert_eq!(got.next_config.configuration_id, 2);
    storage.set_stopsign(None).unwrap();
    assert!(storage.get_stopsign().unwrap().is_none());
}

#[test]
fn mem_storage_conforms() {
    run_scenario(MemStorage::<TestCommand>::new());
}

#[test]
fn rocksdb_storage_conforms() {
    let (_dir, database) = open_rocksdb_in_tempdir(TEST_CF);
    let storage: RocksdbStorage<TestCommand> =
        RocksdbStorage::open_in(database, TEST_CF).expect("open_in");
    run_scenario(storage);
}
