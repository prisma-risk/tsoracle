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

use std::process::Command;

#[cfg(feature = "file")]
fn output() -> std::process::Output {
    let dir = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_tsoracle"))
        .arg("family-compat")
        .current_dir(dir.path())
        .env("RUST_LOG", "invalid filter[")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    output
}

#[cfg(feature = "file")]
#[test]
fn file_manifest_pins_format_three_shape_without_graph_commands() {
    let output = output();
    let manifest = cg_family_compat::FamilyCompat::parse(&output.stdout).unwrap();
    let expected = serde_json::json!({
        "format": 3,
        "service": "tsoracle",
        "version": env!("CARGO_PKG_VERSION"),
        "contracts": { "cggraph_grpc": {} },
        "service_contracts": {
            "wire_protocol": { "name": "tsoracle.v1.TsoService", "version": 1 },
            "receipt_schema": { "name": "tsoracle.stopped-file-state", "version": 1 }
        },
        "join_ruleset": 1
    });
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
        expected
    );
    assert_eq!(serde_json::to_value(manifest).unwrap(), expected);
}

#[cfg(feature = "file")]
#[test]
fn manifest_is_deterministic_without_configuration_or_state() {
    assert_eq!(output().stdout, output().stdout);
}

#[test]
fn manifest_command_refuses_extra_arguments() {
    let output = Command::new(env!("CARGO_BIN_EXE_tsoracle"))
        .args(["family-compat", "unexpected"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
}

#[cfg(feature = "file")]
#[tokio::test]
async fn declared_receipt_matches_the_linked_file_exporter() {
    use tsoracle_consensus::ConsensusDriver;
    use tsoracle_core::{Epoch, SeqKey};
    use tsoracle_driver_file::{FileDriver, StoppedFileState};

    let dir = tempfile::tempdir().unwrap();
    let driver = FileDriver::open_or_init(dir.path()).unwrap();
    driver.persist_high_water(500, Epoch::ZERO).await.unwrap();
    driver
        .advance_dense(&SeqKey::try_new("orders").unwrap(), 7, Epoch::ZERO)
        .await
        .unwrap();
    driver.persist_leases(&[], Epoch::ZERO).await.unwrap();
    drop(driver);
    let state = StoppedFileState::open(dir.path()).unwrap();
    let receipt = state.receipt().unwrap();
    let manifest = cg_family_compat::FamilyCompat::parse(&output().stdout).unwrap();
    let declared = manifest.service_contracts.unwrap().receipt_schema.unwrap();
    assert_eq!(declared.name(), receipt.schema);
    assert_eq!(declared.version(), receipt.schema_version);
}

#[cfg(feature = "file")]
#[test]
fn wire_declaration_matches_the_served_proto_package_and_service() {
    let proto = include_str!("../../tsoracle-proto/proto/tsoracle/v1/tso.proto");
    assert!(proto.lines().any(|line| line == "package tsoracle.v1;"));
    assert!(proto.lines().any(|line| line == "service TsoService {"));
    let manifest = cg_family_compat::FamilyCompat::parse(&output().stdout).unwrap();
    let declared = manifest.service_contracts.unwrap().wire_protocol.unwrap();
    assert_eq!(declared.name(), "tsoracle.v1.TsoService");
    assert_eq!(declared.version(), 1);
}

#[cfg(not(feature = "file"))]
#[test]
fn builds_without_file_refuse_the_file_manifest() {
    let output = Command::new(env!("CARGO_BIN_EXE_tsoracle"))
        .arg("family-compat")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("does not include the file driver"));
}
