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
use tsoracle_driver_openraft::HighWaterStateMachineSnapshot;

// Adversarial-bytes safety for `install_snapshot` and snapshot-store loads.
// Snapshot bytes arrive from a leader over the openraft snapshot-install RPC
// (decoded at `state_machine.rs::install_snapshot`) and from disk via
// `SnapshotStore::load` (envelope unwrapped at `with_store`). The inner blob
// is a postcard-serialized `HighWaterStateMachineSnapshot` whose
// `last_membership` field is a length-prefixed openraft `StoredMembership` —
// exactly the kind of nested length-prefixed structure that rewards fuzzing.
fuzz_target!(|data: &[u8]| {
    let _ = postcard::from_bytes::<HighWaterStateMachineSnapshot>(data);
});
