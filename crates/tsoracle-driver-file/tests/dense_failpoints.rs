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

use tsoracle_consensus::ConsensusDriver;
use tsoracle_core::{Epoch, SeqKey};
use tsoracle_driver_file::FileDriver;
use tsoracle_failpoint::fail;

/// Body-level serialization: the `fail` registry is process-global; even with
/// `FailScenario::setup` snapshotting between tests, multiple test bodies
/// sharing the same registered name will interleave their configurations.
static FAILPOINT_TEST_SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn key(s: &str) -> SeqKey {
    SeqKey::try_new(s).unwrap()
}

// A crash BEFORE the dense fsync must leave the counter unchanged: the failed
// advance returns an error, no block is handed out, and after "recovery"
// (reopen) the next advance reuses the same start. No gap, no duplicate.
#[tokio::test]
async fn crash_before_fsync_does_not_advance() {
    let _serial = FAILPOINT_TEST_SERIAL.lock().await;
    let _scenario = fail::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();

    {
        let d = FileDriver::open_or_init(dir.path()).unwrap();
        d.advance_dense(&key("orders"), 5, Epoch(1)).await.unwrap(); // [0,5), durable

        fail::cfg("file_driver::dense::before_write", "return").unwrap();
        let err = d.advance_dense(&key("orders"), 3, Epoch(1)).await;
        assert!(
            err.is_err(),
            "advance must fail when the write is interrupted"
        );
        fail::cfg("file_driver::dense::before_write", "off").unwrap();
    }

    // Reopen: the interrupted advance left no trace. Next start is 5, not 8.
    let d2 = FileDriver::open_or_init(dir.path()).unwrap();
    assert_eq!(
        d2.advance_dense(&key("orders"), 1, Epoch(1)).await.unwrap(),
        5
    );
}

// A crash AFTER the tmp fsync but BEFORE the rename is equivalent to "before":
// the live `dense` file is untouched, so recovery sees the prior state.
#[tokio::test]
async fn crash_after_tmp_fsync_before_rename_does_not_advance() {
    let _serial = FAILPOINT_TEST_SERIAL.lock().await;
    let _scenario = fail::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();

    {
        let d = FileDriver::open_or_init(dir.path()).unwrap();
        d.advance_dense(&key("orders"), 5, Epoch(1)).await.unwrap();

        fail::cfg(
            "file_driver::dense::after_tmp_fsync_before_rename",
            "return",
        )
        .unwrap();
        let err = d.advance_dense(&key("orders"), 3, Epoch(1)).await;
        assert!(err.is_err());
        fail::cfg("file_driver::dense::after_tmp_fsync_before_rename", "off").unwrap();
    }

    let d2 = FileDriver::open_or_init(dir.path()).unwrap();
    assert_eq!(
        d2.advance_dense(&key("orders"), 1, Epoch(1)).await.unwrap(),
        5
    );
}
