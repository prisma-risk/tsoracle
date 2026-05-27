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

//! Regenerates the fuzz seed corpus for the two paxos postcard-decoder targets
//! sourced from this crate (`paxos_log_entry_decode` and
//! `paxos_snapshot_decode`), deriving valid-message bytes from the canonical
//! postcard encoder.
//!
//! Run with:
//!
//!     cargo test -p tsoracle-driver-paxos --test generate_fuzz_seeds -- --ignored
//!
//! Idempotent. Re-run after any change to `HighWaterCommand` or
//! `HighWaterSnapshot` to refresh the seeds.

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use tsoracle_consensus::AdvancePayload;
use tsoracle_driver_paxos::{HighWaterCommand, HighWaterSnapshot};

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
fn generate_paxos_log_entry_decode_seeds() {
    let target = "paxos_log_entry_decode";
    // Empty input — exercises the postcard "unexpected EOF" branch.
    write_seed(target, "seed_empty", &[]);
    // Boundary `Advance` targets mirror the snapshot-fold unit tests in
    // `log_entry.rs::tests`.
    for (name, at_least) in [
        ("seed_advance_zero", 0u64),
        ("seed_advance_one", 1u64),
        ("seed_advance_max", u64::MAX),
    ] {
        let bytes = postcard::to_stdvec(&HighWaterCommand::Advance(AdvancePayload { at_least }))
            .expect("serialize Advance");
        write_seed(target, name, &bytes);
    }
    // `Barrier` is the second variant — its discriminant plus the two-field
    // body is the other shape the decoder must accept.
    let barrier = postcard::to_stdvec(&HighWaterCommand::Barrier {
        node: u64::MAX,
        seq: 1,
    })
    .expect("serialize Barrier");
    write_seed(target, "seed_barrier", &barrier);
}

#[test]
#[ignore = "regenerates fuzz seed corpus from the source of truth"]
fn generate_paxos_snapshot_decode_seeds() {
    let target = "paxos_snapshot_decode";
    // Empty input — exercises the postcard "unexpected EOF" branch.
    write_seed(target, "seed_empty", &[]);
    // Minimal valid payload: value 0, empty barrier ledger. Matches a
    // freshly-created snapshot over an empty range (see
    // `HighWaterSnapshot::create(&[])`).
    let minimal = HighWaterSnapshot::default();
    write_seed(
        target,
        "seed_minimal",
        &postcard::to_stdvec(&minimal).expect("serialize minimal snapshot"),
    );
    // Boundary `value`: u64::MAX — flags any postcard varint edge case in the
    // leading field.
    let max_value = HighWaterSnapshot {
        value: u64::MAX,
        applied_barriers: HashMap::new(),
    };
    write_seed(
        target,
        "seed_value_max",
        &postcard::to_stdvec(&max_value).expect("serialize max-value snapshot"),
    );
    // Populated `applied_barriers` map — drives the variable-length
    // `HashMap<u64, u64>` length prefix and several entries, the field most
    // likely to expose a postcard allocation/length edge case.
    let with_barriers = HighWaterSnapshot {
        value: 100,
        applied_barriers: HashMap::from([(1u64, 5u64), (2, 9), (u64::MAX, u64::MAX)]),
    };
    write_seed(
        target,
        "seed_with_barriers",
        &postcard::to_stdvec(&with_barriers).expect("serialize barrier-ledger snapshot"),
    );
}
