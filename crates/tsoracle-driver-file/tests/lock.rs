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

//! Single-writer enforcement via flock on the `LOCK` sentinel file.
//!
//! These tests pin the contract that `FileDriver::open_or_init` rejects a
//! second concurrent open against the same directory, and that the OS-level
//! lock is released both on graceful drop and on hard process abort.

use std::process::Command;
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

/// Child-process pattern: when `TSORACLE_LOCK_TEST_ABORT_DIR` is set, this
/// test acts as the child — it opens the directory and `process::abort`s
/// without unwinding. The kernel releases the flock on process death, so
/// the parent must be able to reopen the same directory without seeing
/// `AlreadyLocked`. Proves we don't depend on graceful Drop running.
#[test]
fn open_after_child_abort_succeeds() {
    if let Ok(child_dir) = std::env::var("TSORACLE_LOCK_TEST_ABORT_DIR") {
        let _driver = FileDriver::open_or_init(&child_dir).expect("child: open");
        std::process::abort();
    }

    let dir = tempdir().unwrap();
    let exe = std::env::current_exe().expect("current_exe");
    let status = Command::new(&exe)
        .args(["--exact", "--nocapture", "open_after_child_abort_succeeds"])
        .env("TSORACLE_LOCK_TEST_ABORT_DIR", dir.path())
        .status()
        .expect("spawn child");
    assert!(
        !status.success(),
        "child process should have aborted, got status {status:?}"
    );

    let _reopen = FileDriver::open_or_init(dir.path())
        .expect("reopen after child abort must succeed — kernel released the flock");
}
