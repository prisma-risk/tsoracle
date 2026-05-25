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
use tsoracle_paxos_toolkit::storage::meta::decode_ballot;

// Adversarial-bytes safety for the toolkit's meta-column ballot decoder. The
// promised/accepted `Ballot` is the one omnipaxos-shaped value the toolkit
// reads back as postcard from disk on every recovery (see
// `storage/meta.rs::decode_ballot`); a panic in that decode crashes startup.
// This is the paxos toolkit's counterpart of `toolkit_codec_decode`. The paxos
// `codec` now frames every record as [version | postcard(body)]; driving the
// decoder through the concrete `Ballot` the toolkit reads back on recovery
// exercises both the version gate and the postcard body.
fuzz_target!(|data: &[u8]| {
    let _ = decode_ballot(data);
});
