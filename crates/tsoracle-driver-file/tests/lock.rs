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

//! Single-writer enforcement via flock on the `LOCK` sentinel file.
//!
//! These tests pin the contract that `FileDriver::open_or_init` rejects a
//! second concurrent open against the same directory, and that graceful
//! Drop releases the lock. The kernel-cleanup-on-abort case lives in its
//! own `lock_child_abort` binary — see that file for the fork-inheritance
//! rationale.

use tempfile::tempdir;
use tsoracle_driver_file::{FileDriver, FileDriverError};

#[test]
fn open_then_open_same_dir_fails() {
    let dir = tempdir().unwrap();
    let _first = FileDriver::open_or_init(dir.path()).expect("first open succeeds");
    let err = FileDriver::open_or_init(dir.path())
        .expect_err("second concurrent open must fail at lock acquisition");
    match err {
        FileDriverError::AlreadyLocked { path, .. } => {
            assert_eq!(
                path,
                dir.path().join("LOCK"),
                "AlreadyLocked must report the sentinel path, not the state path"
            );
        }
        other => panic!("expected AlreadyLocked, got {other:?}"),
    }
}

#[test]
fn open_after_drop_succeeds() {
    let dir = tempdir().unwrap();
    let first = FileDriver::open_or_init(dir.path()).expect("first open succeeds");
    drop(first);
    let _second = FileDriver::open_or_init(dir.path())
        .expect("second open after drop must succeed — drop releases the flock");
}
