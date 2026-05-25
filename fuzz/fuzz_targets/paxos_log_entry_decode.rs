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
use tsoracle_driver_paxos::HighWaterCommand;

// Adversarial-bytes safety for the OmniPaxos log entry decoder. Every
// `HighWaterCommand` (`Advance` / `Barrier`) rides a replicated entry: bytes
// arrive over the peer transport and are read back from the RocksDB log
// column on recovery, so a panic here crashes a follower or a restarting
// node. The paxos counterpart of `log_entry_decode` (which covers the
// openraft `HighWaterCommand`).
fuzz_target!(|data: &[u8]| {
    let _ = postcard::from_bytes::<HighWaterCommand>(data);
});
