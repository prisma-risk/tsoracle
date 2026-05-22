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

//! Regenerates the fuzz seed corpus for the two postcard-decoder targets
//! sourced from this crate (`log_entry_decode` and `snapshot_payload_decode`),
//! deriving valid-message bytes from the canonical postcard encoder.
//!
//! Run with:
//!
//!     cargo test -p tsoracle-driver-openraft --test generate_fuzz_seeds -- --ignored
//!
//! Idempotent. Re-run after any change to `HighWaterCommand` or
//! `HighWaterStateMachineSnapshot` to refresh the seeds.

use std::fs;
use std::path::PathBuf;

use openraft::type_config::alias::StoredMembershipOf;
use tsoracle_driver_openraft::{HighWaterCommand, HighWaterStateMachineSnapshot, TypeConfig};

fn fuzz_corpus_dir(target: &str) -> PathBuf {
    PathBuf::from("../..").join("fuzz/corpus").join(target)
}

fn write_seed(target: &str, name: &str, bytes: &[u8]) {
    let dir = fuzz_corpus_dir(target);
    fs::create_dir_all(&dir).expect("create seed corpus dir");
    fs::write(dir.join(name), bytes).expect("write seed file");
}

#[test]
#[ignore = "regenerates fuzz seed corpus from the source of truth"]
fn generate_log_entry_decode_seeds() {
    let target = "log_entry_decode";
    // Empty input — exercises the postcard "unexpected EOF" branch.
    write_seed(target, "seed_empty", &[]);
    // Three boundary `Bump` targets mirror the existing hand-written
    // roundtrip tests in `log_entry.rs::tests`.
    for (name, target_value) in [
        ("seed_bump_zero", 0u64),
        ("seed_bump_one", 1u64),
        ("seed_bump_max", u64::MAX),
    ] {
        let bytes = postcard::to_stdvec(&HighWaterCommand::Bump {
            target: target_value,
        })
        .expect("serialize HighWaterCommand");
        write_seed(target, name, &bytes);
    }
}

#[test]
#[ignore = "regenerates fuzz seed corpus from the source of truth"]
fn generate_snapshot_payload_decode_seeds() {
    let target = "snapshot_payload_decode";
    // Empty input — exercises the postcard "unexpected EOF" branch.
    write_seed(target, "seed_empty", &[]);
    // Minimal valid payload: default membership, no last_applied, value 0.
    // Matches the state-machine constructor's initial core (see
    // `state_machine.rs::with_store`).
    let minimal = HighWaterStateMachineSnapshot {
        current_value: 0,
        last_applied: None,
        last_membership: StoredMembershipOf::<TypeConfig>::default(),
    };
    write_seed(
        target,
        "seed_minimal",
        &postcard::to_stdvec(&minimal).expect("serialize minimal snapshot"),
    );
    // Boundary `current_value`: u64::MAX — flags any postcard varint edge
    // case in the leading field of the payload.
    let max_value = HighWaterStateMachineSnapshot {
        current_value: u64::MAX,
        last_applied: None,
        last_membership: StoredMembershipOf::<TypeConfig>::default(),
    };
    write_seed(
        target,
        "seed_value_max",
        &postcard::to_stdvec(&max_value).expect("serialize max-value snapshot"),
    );
}
