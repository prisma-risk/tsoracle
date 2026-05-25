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

//! Regenerates the fuzz seed corpus for `paxos_meta_ballot_decode`, deriving
//! valid ballot bytes from the canonical
//! [`tsoracle_paxos_toolkit::storage::meta::encode_ballot`] entry point.
//!
//! Run with:
//!
//!     cargo test -p tsoracle-paxos-toolkit --test generate_fuzz_seeds -- --ignored
//!
//! Idempotent. Re-run after any change to the meta ballot codec.

use std::fs;
use std::path::PathBuf;

use omnipaxos::ballot_leader_election::Ballot;
use tsoracle_paxos_toolkit::storage::meta::encode_ballot;

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
fn generate_paxos_meta_ballot_decode_seeds() {
    let target = "paxos_meta_ballot_decode";
    // Empty input — exercises the postcard "unexpected EOF" branch reached
    // through `decode_ballot`.
    write_seed(target, "seed_empty", &[]);
    // Default ballot: all fields zero. Matches the bottom ballot omnipaxos
    // persists before any leader is elected.
    write_seed(
        target,
        "seed_default",
        &encode_ballot(&Ballot::default()).expect("encode default ballot"),
    );
    // Boundary ballot: every field at its type maximum. `config_id`, `n`, and
    // `priority` are u32; `pid` is u64 — covering both varint widths in one
    // seed.
    let max = Ballot {
        config_id: u32::MAX,
        n: u32::MAX,
        priority: u32::MAX,
        pid: u64::MAX,
    };
    write_seed(
        target,
        "seed_field_max",
        &encode_ballot(&max).expect("encode max-field ballot"),
    );
}
