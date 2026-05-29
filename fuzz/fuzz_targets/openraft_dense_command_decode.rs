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
use tsoracle_driver_openraft::{HighWaterStateMachineSnapshot, OpenraftEntry};
use tsoracle_openraft_toolkit::{DENSE_WRITE_VERSION, decode};

fuzz_target!(|data: &[u8]| {
    // Dense snapshot payload at the dense write version (new BTreeMap field).
    let _ = decode::<HighWaterStateMachineSnapshot>(DENSE_WRITE_VERSION, data);
    // Full openraft entry decode (now includes the AdvanceDense command variant).
    let _ = decode::<OpenraftEntry>(DENSE_WRITE_VERSION, data);
});
