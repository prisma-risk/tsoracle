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

//! Verify the RocksDB storage honors injected failpoints.

#![cfg(all(feature = "failpoints", feature = "rocksdb-storage"))]

#[path = "common/mod.rs"]
mod common;

use common::{TEST_CF, TestCommand, open_rocksdb_in_tempdir, open_rocksdb_storage};
use omnipaxos::storage::Storage;
use tempfile::TempDir;
use tsoracle_paxos_toolkit::storage::RocksdbStorage;

/// Failpoints live in the process-global `fail` registry, but cargo runs a
/// binary's tests concurrently. Serialize every failpoint test in this binary
/// so one test's armed failpoint can never leak into another's storage write.
/// `parking_lot::Mutex` is non-poisoning, so a failing test cannot cascade
/// spurious failures into its siblings.
static FAILPOINT_TEST_SERIAL: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

#[test]
fn append_entry_panics_when_failpoint_armed() {
    let _serial = FAILPOINT_TEST_SERIAL.lock();
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

#[test]
fn lost_compacted_index_after_trim_recovers_forward_only() {
    let _serial = FAILPOINT_TEST_SERIAL.lock();
    // Realistic crash: trim persists, then the set_compacted_idx write is
    // lost (the WAL tail is truncated past trim). On reopen the compacted
    // offset is stale-low, but recovery must stay forward-only — surviving
    // entries remain readable at their absolute indices, no phantom entry
    // appears at a low index, and the next append lands past the survivors
    // rather than overwriting one.
    let dir = TempDir::new().unwrap();
    {
        let mut storage = open_rocksdb_storage(&dir, TEST_CF);
        storage
            .append_entries(vec![
                TestCommand(1),
                TestCommand(2),
                TestCommand(3),
                TestCommand(4),
            ])
            .unwrap();
        // trim persists (deletes physical keys 0,1).
        storage.trim(2).unwrap();
        // The paired compacted-index update is lost in the crash.
        tsoracle_failpoint::fail::cfg("paxos_toolkit::storage::async_write", "return").unwrap();
        let lost = storage.set_compacted_idx(2);
        tsoracle_failpoint::fail::remove("paxos_toolkit::storage::async_write");
        assert!(
            lost.is_err(),
            "the failpoint must drop the compacted-index write"
        );
    }

    let mut storage = open_rocksdb_storage(&dir, TEST_CF);
    assert_eq!(
        storage.get_compacted_idx().unwrap(),
        0,
        "the lost compacted-index update leaves the offset stale-low",
    );
    // The trimmed-but-uncompacted entries are still readable at their
    // absolute indices; the trimmed-away keys 0,1 do NOT reappear.
    let suffix = storage.get_suffix(0).unwrap();
    assert_eq!(suffix.len(), 2, "only the untrimmed entries survive");
    assert_eq!(suffix[0].0, 3);
    assert_eq!(suffix[1].0, 4);

    // Forward-only: the next append lands at idx 4 (past the survivors at
    // 2,3), never overwriting a survivor or planting a phantom at idx 0.
    storage.append_entry(TestCommand(99)).unwrap();
    let single = storage.get_entries(4, 5).unwrap();
    assert_eq!(single.len(), 1);
    assert_eq!(single[0].0, 99);
    let suffix = storage.get_suffix(2).unwrap();
    assert_eq!(suffix.len(), 3);
    assert_eq!(suffix[2].0, 99);
}

/// Counter value for `name` whose label set contains `op = <expected_op>`.
#[cfg(feature = "metrics")]
fn counter_with_op(
    snapshot: &[(
        metrics_util::CompositeKey,
        Option<metrics::Unit>,
        Option<metrics::SharedString>,
        metrics_util::debugging::DebugValue,
    )],
    name: &str,
    expected_op: &str,
) -> u64 {
    use metrics_util::MetricKind;
    use metrics_util::debugging::DebugValue;
    for (composite, _unit, _desc, value) in snapshot {
        if composite.kind() != MetricKind::Counter || composite.key().name() != name {
            continue;
        }
        let has_op = composite
            .key()
            .labels()
            .any(|label| label.key() == "op" && label.value() == expected_op);
        if has_op && let DebugValue::Counter(n) = value {
            return *n;
        }
    }
    0
}

/// A failing async (non-synced) write must increment
/// `tsoracle.paxos.storage.async_write_failures.total`, labelled with the
/// storage operation that failed. The `async_write` failpoint forces the
/// `batch_async` body to return an error; a thread-local recorder captures
/// the emitted counter so the assertion observes real emission, not a mock.
#[cfg(feature = "metrics")]
#[test]
fn failed_async_write_increments_labelled_failure_counter() {
    let _serial = FAILPOINT_TEST_SERIAL.lock();
    use metrics_util::debugging::DebuggingRecorder;

    let dir = TempDir::new().unwrap();
    let mut storage = open_rocksdb_storage(&dir, TEST_CF);

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();

    tsoracle_failpoint::fail::cfg("paxos_toolkit::storage::async_write", "return").unwrap();
    let result = metrics::with_local_recorder(&recorder, || storage.set_compacted_idx(5));
    tsoracle_failpoint::fail::remove("paxos_toolkit::storage::async_write");

    assert!(
        result.is_err(),
        "armed failpoint must surface a write error"
    );

    let snapshot = snapshotter.snapshot().into_vec();
    assert_eq!(
        counter_with_op(
            &snapshot,
            "tsoracle.paxos.storage.async_write_failures.total",
            "set_compacted_idx",
        ),
        1,
        "a failed set_compacted_idx must increment the op-labelled failure counter once",
    );
}
