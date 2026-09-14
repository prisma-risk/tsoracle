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
use tsoracle_driver_openraft::{HighWaterCommand, HighWaterStateMachineSnapshot, OpenraftEntry};
use tsoracle_openraft_toolkit::decode;

// The SetLeases command and the lease-bearing snapshot are attacker-reachable wire and disk formats once write version 7 is activated. Decoding arbitrary bytes must never panic: a bad variant index, a truncated vec length, an empty or oversized holder, an unordered or duplicate lease id, or trailing bytes must all surface as decode errors.
//
// Three decoders share one input. The first byte picks an exact log-entry version for `decode::<OpenraftEntry>`, as in `openraft_advance_dense_batch_decode`. The whole input is also decoded as a bare `HighWaterCommand` behind a forced SetLeases variant index, so the fuzzer spends its effort inside the lease payload instead of rediscovering the index, and as a v7 snapshot payload, whose trailing field is the lease set. Only the no-panic property is asserted.
fuzz_target!(|data: &[u8]| {
    if let Some((&version, body)) = data.split_first() {
        let _ = decode::<OpenraftEntry>(version, body);
    }
    let mut command = Vec::with_capacity(data.len() + 1);
    command.push(4u8);
    command.extend_from_slice(data);
    let _ = postcard::from_bytes::<HighWaterCommand>(&command);
    let _ = postcard::from_bytes::<HighWaterStateMachineSnapshot>(data);
});
