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
use std::sync::mpsc::sync_channel;
use std::time::Duration;
use tsoracle_consensus::{ConsensusDriver, ConsensusError};
use tsoracle_core::Epoch;
use tsoracle_driver_file::FileDriver;

/// Body-level serialization: the `fail` registry is process-global; even with
/// `FailScenario::setup` snapshotting between tests, multiple test bodies
/// sharing the same registered name will interleave their configurations.
/// Mirrors the old `PERSIST_TEST_SERIAL` pattern.
static FAILPOINT_TEST_SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Regression guard for the AtomicU64 refactor: `load_high_water` must not
/// park behind an in-flight `persist_high_water`. Uses `tsoracle_failpoint::fail::cfg_callback`
/// to install a callback that signals arrival and then blocks until released,
/// preserving the deterministic handshake the original `SLOW_WRITE_HOOK`
/// provided. Plain `pause` would block the writer but give the reader no
/// signal that the writer has reached the failpoint, so the timeout-bounded
/// read could race-pass before the writer arrives.
///
/// Failure mode if the regression returns: `load_high_water` blocks on the
/// same sync mutex held by the writer. The `tokio::time::timeout` below
/// catches *async* regressions cleanly, but a sync-mutex regression hangs
/// the task off-runtime — the timer driver never gets to poll the timeout.
/// In that case the test hangs until CI's per-test timeout fires.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn load_is_not_blocked_by_in_flight_persist() {
    let _serial = FAILPOINT_TEST_SERIAL.lock().await;
    let _scenario = tsoracle_failpoint::fail::FailScenario::setup();

    let dir = tempfile::tempdir().unwrap();
    let driver = FileDriver::open_or_init(dir.path()).unwrap();

    let (entered_tx, entered_rx) = sync_channel::<()>(1);
    let (release_tx, release_rx) = sync_channel::<()>(1);
    let release_rx = std::sync::Mutex::new(release_rx);

    tsoracle_failpoint::fail::cfg_callback("file_driver::write_blocked", move || {
        entered_tx.send(()).unwrap();
        release_rx.lock().unwrap().recv().unwrap();
    })
    .unwrap();

    let writer = {
        let driver = Arc::clone(&driver);
        tokio::spawn(async move { driver.persist_high_water(100, Epoch::ZERO).await })
    };

    // Deterministic wait: the writer has reached the failpoint when this
    // channel send arrives. Running the sync recv in spawn_blocking keeps
    // it off a runtime worker thread.
    tokio::task::spawn_blocking(move || entered_rx.recv().unwrap())
        .await
        .unwrap();

    // Writer is parked. A reader must not wait on it. The 500ms timeout
    // only catches async-blocking regressions; sync-mutex regressions
    // (the original bug) show up as a hang.
    let read = tokio::time::timeout(Duration::from_millis(500), driver.load_high_water())
        .await
        .expect("load_high_water was blocked by an in-flight persist");
    assert_eq!(
        read.unwrap(),
        0,
        "writer has not published yet; reader should see the prior value"
    );

    release_tx.send(()).unwrap();
    let persisted = writer.await.unwrap().unwrap();
    assert_eq!(persisted, 100);
    assert_eq!(driver.load_high_water().await.unwrap(), 100);
}

/// `file_driver::before_write` fires at the top of `write_record`, before
/// the tmpfile is opened. A `return(io)` action makes `write_record`
/// produce `Err(FileDriverError::Io(_))`; `persist_high_water` wraps that
/// into `ConsensusError::PermanentDriver`. After the failed persist, no
/// state file (or stale state file) is left behind — reopen sees the
/// prior high-water.
#[tokio::test]
async fn reopen_after_crash_before_write_returns_prior_high_water() {
    let _serial = FAILPOINT_TEST_SERIAL.lock().await;
    let _scenario = tsoracle_failpoint::fail::FailScenario::setup();

    let dir = tempfile::tempdir().unwrap();
    {
        let driver = FileDriver::open_or_init(dir.path()).unwrap();
        driver.persist_high_water(100, Epoch::ZERO).await.unwrap();

        tsoracle_failpoint::fail::cfg("file_driver::before_write", "return(io)").unwrap();
        let result = driver.persist_high_water(200, Epoch::ZERO).await;
        assert!(
            matches!(result, Err(ConsensusError::PermanentDriver(_))),
            "expected PermanentDriver, got {result:?}"
        );
        tsoracle_failpoint::fail::cfg("file_driver::before_write", "off").unwrap();
    }

    let reopened = FileDriver::open_or_init(dir.path()).unwrap();
    assert_eq!(
        reopened.load_high_water().await.unwrap(),
        100,
        "reopen should see prior high-water; the failed persist must not have rewritten state"
    );
}

/// `file_driver::after_tmp_fsync_before_rename` fires after the tmpfile
/// fsync but before `fs::rename`. A `return(io)` action causes the rename
/// to be skipped; the tmpfile may exist on disk but the canonical `state`
/// file is unchanged. Reopen reads `state` and ignores `state.tmp`.
#[tokio::test]
async fn reopen_after_crash_between_tmp_fsync_and_rename_returns_prior_high_water() {
    let _serial = FAILPOINT_TEST_SERIAL.lock().await;
    let _scenario = tsoracle_failpoint::fail::FailScenario::setup();

    let dir = tempfile::tempdir().unwrap();
    {
        let driver = FileDriver::open_or_init(dir.path()).unwrap();
        driver.persist_high_water(100, Epoch::ZERO).await.unwrap();

        tsoracle_failpoint::fail::cfg("file_driver::after_tmp_fsync_before_rename", "return(io)")
            .unwrap();
        let result = driver.persist_high_water(200, Epoch::ZERO).await;
        assert!(
            matches!(result, Err(ConsensusError::PermanentDriver(_))),
            "expected PermanentDriver, got {result:?}"
        );
        tsoracle_failpoint::fail::cfg("file_driver::after_tmp_fsync_before_rename", "off").unwrap();
    }

    let reopened = FileDriver::open_or_init(dir.path()).unwrap();
    assert_eq!(reopened.load_high_water().await.unwrap(), 100);
}

/// `file_driver::after_rename_before_dir_fsync` fires after `fs::rename`
/// but before the directory fsync. A `panic` action terminates the
/// `spawn_blocking` worker; `persist_high_water` surfaces it as
/// `PermanentDriver`. Reopen sees the new high-water in practice (the
/// rename's metadata is in the kernel page cache; we have not crashed
/// the OS), but the assertion is loosened to "monotonic either way" —
/// the test asserts the loaded value is >= 100 and <= 200, because a
/// future change to the underlying filesystem semantics shouldn't break
/// this test.
#[tokio::test]
async fn reopen_after_crash_between_rename_and_dir_fsync_is_monotonic() {
    let _serial = FAILPOINT_TEST_SERIAL.lock().await;
    let _scenario = tsoracle_failpoint::fail::FailScenario::setup();

    let dir = tempfile::tempdir().unwrap();
    {
        let driver = FileDriver::open_or_init(dir.path()).unwrap();
        driver.persist_high_water(100, Epoch::ZERO).await.unwrap();

        tsoracle_failpoint::fail::cfg("file_driver::after_rename_before_dir_fsync", "panic")
            .unwrap();
        let result = driver.persist_high_water(200, Epoch::ZERO).await;
        assert!(
            matches!(result, Err(ConsensusError::PermanentDriver(_))),
            "spawn_blocking panic should surface as PermanentDriver, got {result:?}"
        );
        tsoracle_failpoint::fail::cfg("file_driver::after_rename_before_dir_fsync", "off").unwrap();
    }

    let reopened = FileDriver::open_or_init(dir.path()).unwrap();
    let loaded = reopened.load_high_water().await.unwrap();
    assert!(
        (100..=200).contains(&loaded),
        "expected loaded value in 100..=200, got {loaded}"
    );
}
