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

#![cfg(all(feature = "file", feature = "single-node"))]

use std::fs;
use std::path::Path;
use std::process::Command;

use tsoracle_consensus::ConsensusDriver;
use tsoracle_core::Epoch;
use tsoracle_driver_file::FileDriver;

mod support;
use support::{populate, snapshot};

fn command(dir: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_tsoracle"));
    command
        .args(["export", "file", "--state-dir"])
        .arg(dir)
        .arg("--compact");
    command
}

#[tokio::test]
async fn exports_json_without_mutating_state_and_refuses_native_owner() {
    let dir = tempfile::tempdir().unwrap();
    let driver = FileDriver::open_or_init(dir.path()).unwrap();
    populate(driver.as_ref()).await;
    let before = snapshot(dir.path());
    let locked = command(dir.path()).output().unwrap();
    assert!(!locked.status.success());
    assert!(locked.stdout.is_empty());
    assert_eq!(snapshot(dir.path()), before);
    drop(driver);
    let output = command(dir.path()).output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let receipt: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(receipt["schema_version"], 1);
    assert_eq!(receipt["committed_reservation_high_water_physical_ms"], 500);
    assert_eq!(receipt["dense_next_unallocated"]["orders"], 7);
    assert_eq!(
        receipt["maximum_lease_expires_at_unix_ms"],
        serde_json::Value::Null
    );
    assert_eq!(receipt["files"].as_array().unwrap().len(), 4);
    assert_eq!(snapshot(dir.path()), before);
    let pretty = Command::new(env!("CARGO_BIN_EXE_tsoracle"))
        .args(["export", "file", "--state-dir"])
        .arg(dir.path())
        .output()
        .unwrap();
    assert!(pretty.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&pretty.stdout).unwrap(),
        receipt
    );
}

#[test]
fn missing_directory_produces_no_receipt_or_files() {
    let parent = tempfile::tempdir().unwrap();
    let missing = parent.path().join("missing");
    let output = command(&missing).output().unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(!missing.exists());
}

#[tokio::test]
async fn incomplete_and_corrupt_state_produce_no_receipt_or_mutation() {
    let dir = tempfile::tempdir().unwrap();
    let driver = FileDriver::open_or_init(dir.path()).unwrap();
    driver.persist_high_water(500, Epoch::ZERO).await.unwrap();
    drop(driver);
    fs::remove_file(dir.path().join("state")).unwrap();
    let before = snapshot(dir.path());
    let output = command(dir.path()).output().unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert_eq!(snapshot(dir.path()), before);
    let driver = FileDriver::open_or_init(dir.path()).unwrap();
    populate(driver.as_ref()).await;
    drop(driver);
    fs::write(dir.path().join("leases"), b"corrupt").unwrap();
    let before = snapshot(dir.path());
    let output = command(dir.path()).output().unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert_eq!(snapshot(dir.path()), before);
}

#[tokio::test]
async fn timestamp_only_and_seeded_exports_record_absent_files() {
    for seeded in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        if seeded {
            FileDriver::init_seeded(dir.path(), 500).unwrap();
        } else {
            let driver = FileDriver::open_or_init(dir.path()).unwrap();
            driver.persist_high_water(500, Epoch::ZERO).await.unwrap();
        }
        let before = snapshot(dir.path());
        let output = command(dir.path()).output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let receipt: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert!(
            receipt["dense_next_unallocated"]
                .as_object()
                .unwrap()
                .is_empty()
        );
        assert!(receipt["leases"].as_array().unwrap().is_empty());
        for entry in receipt["files"].as_array().unwrap() {
            if entry["path"] == "dense" || entry["path"] == "leases" {
                assert_eq!(entry["present"], false);
                assert!(entry["sha256"].is_null());
                assert!(entry["size_bytes"].is_null());
            }
        }
        assert_eq!(snapshot(dir.path()), before);
    }
}

#[test]
fn export_help_explains_json_and_compact_output() {
    let output = Command::new(env!("CARGO_BIN_EXE_tsoracle"))
        .args(["export", "file", "--help"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).unwrap();
    assert!(help.contains("--compact"));
    assert!(help.contains("indented JSON"));
    let output = Command::new(env!("CARGO_BIN_EXE_tsoracle"))
        .args(["export", "--help"])
        .output()
        .unwrap();
    assert!(
        String::from_utf8(output.stdout)
            .unwrap()
            .contains("stopped file driver's state as JSON")
    );
}
