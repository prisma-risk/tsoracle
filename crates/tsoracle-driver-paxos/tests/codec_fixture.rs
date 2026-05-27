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

//! Pins the v1 on-disk frame of a paxos `HighWaterCommand` log entry: the
//! exact bytes `RocksdbStorage` persists per entry. A layout change trips this
//! and forces a `SCHEMA_VERSION` bump.

use tsoracle_consensus::AdvancePayload;
use tsoracle_driver_paxos::HighWaterCommand;
use tsoracle_paxos_toolkit::codec::{SCHEMA_VERSION, encode};

#[test]
fn log_entry_pins_v1_layout() {
    // Body [0,5] = HighWaterCommand::Advance variant tag (0) + the wrapped
    // AdvancePayload, which postcard encodes as a bare `at_least` varint (5) —
    // byte-for-byte identical to the pre-newtype struct-variant layout.
    let cmd = HighWaterCommand::Advance(AdvancePayload { at_least: 5 });
    let framed = encode(SCHEMA_VERSION, &cmd).expect("encode");
    assert_eq!(framed, vec![1, 0, 5]);
}
