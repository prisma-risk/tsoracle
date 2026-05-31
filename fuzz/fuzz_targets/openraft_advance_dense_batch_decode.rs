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

#![no_main]
use libfuzzer_sys::fuzz_target;
use tsoracle_driver_openraft::OpenraftEntry;
use tsoracle_openraft_toolkit::decode;

// The AdvanceDenseBatch command is an attacker-reachable wire/disk format once
// write version 6 is activated. Decoding arbitrary bytes must never panic —
// every malformed input (bad variant index, truncated vec length, empty or
// oversized embedded SeqKey, trailing bytes) must surface as a decode error,
// never an unwrap/slice panic. This mirrors `openraft_dense_command_decode`'s
// entry portion exactly: `decode` is an EXACT-version decoder, not the
// log-store readable-range gate, so this asserts only the no-panic property —
// it deliberately makes NO out-of-range-rejection assertion (the range gate
// lives in the log store's `decode_entry_record`, covered by a unit test).
fuzz_target!(|data: &[u8]| {
    if let Some((&version, body)) = data.split_first() {
        let _ = decode::<OpenraftEntry>(version, body);
    }
});
