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
use tsoracle_driver_paxos::HighWaterSnapshot;

// Adversarial-bytes safety for the OmniPaxos snapshot decoder. A
// `HighWaterSnapshot` is transferred between nodes during snapshot catch-up
// and persisted to the RocksDB meta column, so it is decoded from bytes a
// recovering or lagging node did not produce itself. Its `applied_barriers`
// `HashMap<u64, u64>` is variable-length — the field most likely to expose a
// postcard length/allocation edge case, which makes it worth a target
// distinct from `paxos_log_entry_decode`.
fuzz_target!(|data: &[u8]| {
    let _ = postcard::from_bytes::<HighWaterSnapshot>(data);
});
