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

/// Exercises how the next absolute write index moves under operations that
/// do not simply grow the log: `append_on_prefix` that shrinks the tail, and
/// an unpaired full `trim`. Both impls must agree on where the following
/// append lands. These are the corners a cached `next_idx` is most likely to
/// get wrong (a naive cache only ever increments), so the cross-impl check
/// pins the cache to the source-of-truth model `MemStorage` embodies.
fn run_index_tracking_scenario<S: Storage<TestCommand>>(mut storage: S) {
    // append_on_prefix that shrinks the log must move `next` *backward*: the
    // next append continues from the new, lower tail rather than the old one.
    storage
        .append_entries(vec![
            TestCommand(1),
            TestCommand(2),
            TestCommand(3),
            TestCommand(4),
            TestCommand(5),
        ])
        .unwrap();
    storage
        .append_on_prefix(2, vec![TestCommand(20), TestCommand(30)])
        .unwrap();
    // Log now holds absolute indices 0,1,2,3 -> next == 4.
    let len = storage.append_entry(TestCommand(40)).unwrap();
    assert_eq!(len, 5, "append continues at idx 4; physical len = 5");
    let at_four = storage.get_entries(4, 5).unwrap();
    assert_eq!(at_four.len(), 1);
    assert_eq!(
        at_four[0].0, 40,
        "new entry lands at absolute idx 4, no gap"
    );
    let all = storage.get_suffix(0).unwrap();
    assert_eq!(all.len(), 5);
    assert_eq!(all[2].0, 20);
    assert_eq!(all[4].0, 40);
}

/// An unpaired full `trim` (every key removed, `compacted_idx` left at 0)
/// must reset the next write index to `compacted_idx`, so the following
/// append re-fills from idx 0 rather than leaving a phantom gap.
fn run_unpaired_full_trim_scenario<S: Storage<TestCommand>>(mut storage: S) {
    storage
        .append_entries(vec![TestCommand(1), TestCommand(2), TestCommand(3)])
        .unwrap();
    storage.trim(3).unwrap();
    assert_eq!(storage.get_log_len().unwrap(), 0, "every key trimmed");
    let len = storage.append_entry(TestCommand(9)).unwrap();
    assert_eq!(len, 1);
    let at_zero = storage.get_entries(0, 1).unwrap();
    assert_eq!(at_zero.len(), 1);
    assert_eq!(
        at_zero[0].0, 9,
        "after a full unpaired trim, append resumes at idx 0"
    );
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

#[test]
fn mem_storage_tracks_index_under_shrink() {
    run_index_tracking_scenario(MemStorage::<TestCommand>::new());
}

#[test]
fn rocksdb_storage_tracks_index_under_shrink() {
    let (_dir, database) = open_rocksdb_in_tempdir(TEST_CF);
    let storage: RocksdbStorage<TestCommand> =
        RocksdbStorage::open_in(database, TEST_CF).expect("open_in");
    run_index_tracking_scenario(storage);
}

#[test]
fn mem_storage_resets_index_after_unpaired_full_trim() {
    run_unpaired_full_trim_scenario(MemStorage::<TestCommand>::new());
}

#[test]
fn rocksdb_storage_resets_index_after_unpaired_full_trim() {
    let (_dir, database) = open_rocksdb_in_tempdir(TEST_CF);
    let storage: RocksdbStorage<TestCommand> =
        RocksdbStorage::open_in(database, TEST_CF).expect("open_in");
    run_unpaired_full_trim_scenario(storage);
}
