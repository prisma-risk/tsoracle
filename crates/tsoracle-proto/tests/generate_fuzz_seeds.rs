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

//! Regenerates the fuzz seed corpus for the three prost-decoder targets
//! (GetTsRequest, GetTsResponse, LeaderHint), deriving valid-message
//! bytes from the canonical prost encoder.
//!
//! Run with:
//!
//!     cargo test -p tsoracle-proto --test generate_fuzz_seeds -- --ignored
//!
//! Idempotent. Re-run after any schema change to refresh seeds.

use prost::Message;
use std::fs;
use std::path::PathBuf;
use tsoracle_proto::v1::{EpochWire, GetTsRequest, GetTsResponse, LeaderHint};

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
fn generate_get_ts_request_seeds() {
    let target = "proto_get_ts_request_decode";
    write_seed(target, "seed_empty", &[]);
    write_seed(
        target,
        "seed_count_0",
        &GetTsRequest { count: 0 }.encode_to_vec(),
    );
    write_seed(
        target,
        "seed_count_1",
        &GetTsRequest { count: 1 }.encode_to_vec(),
    );
    write_seed(
        target,
        "seed_count_max",
        &GetTsRequest { count: u32::MAX }.encode_to_vec(),
    );
}

#[test]
#[ignore = "regenerates fuzz seed corpus from the source of truth"]
fn generate_get_ts_response_seeds() {
    let target = "proto_get_ts_response_decode";
    write_seed(target, "seed_empty", &[]);
    write_seed(
        target,
        "seed_default",
        &GetTsResponse {
            physical_ms: 0,
            logical_start: 0,
            count: 0,
            epoch_hi: 0,
            epoch_lo: 0,
        }
        .encode_to_vec(),
    );
    write_seed(
        target,
        "seed_typical",
        &GetTsResponse {
            physical_ms: 1_700_000_000_000,
            logical_start: 0,
            count: 100,
            epoch_hi: 0,
            epoch_lo: 1,
        }
        .encode_to_vec(),
    );
}

#[test]
#[ignore = "regenerates fuzz seed corpus from the source of truth"]
fn generate_leader_hint_seeds() {
    let target = "proto_leader_hint_decode";
    write_seed(target, "seed_empty", &[]);
    write_seed(
        target,
        "seed_no_epoch",
        &LeaderHint {
            leader_endpoint: None,
            leader_epoch: None,
        }
        .encode_to_vec(),
    );
    write_seed(
        target,
        "seed_typical",
        &LeaderHint {
            leader_endpoint: Some("127.0.0.1:50551".into()),
            leader_epoch: Some(EpochWire { hi: 0, lo: 1 }),
        }
        .encode_to_vec(),
    );
    write_seed(
        target,
        "seed_long_endpoint",
        &LeaderHint {
            leader_endpoint: Some("a".repeat(512)),
            leader_epoch: Some(EpochWire {
                hi: u64::MAX,
                lo: u64::MAX,
            }),
        }
        .encode_to_vec(),
    );
}
