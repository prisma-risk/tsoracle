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

//! Verifies the kernel releases the flock on hard process death (no Drop).
//!
//! Isolated into its own integration-test binary so that its `Command::spawn`
//! cannot fork-inherit file descriptors held by sibling lock tests in the same
//! binary. `flock(2)` ties locks to the open file description; an inherited fd
//! in a child between `fork` and `exec` keeps the OFD alive (and the lock
//! held) from a concurrent test's perspective, even after that test drops its
//! `FileDriver`. Putting this test alone in its own binary removes the
//! sibling-fd surface area entirely.

use std::process::Command;
use tempfile::tempdir;
use tsoracle_driver_file::FileDriver;

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
