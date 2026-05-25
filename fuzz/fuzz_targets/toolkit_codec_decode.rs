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
use tsoracle_openraft_toolkit::decode;
use tsoracle_driver_openraft::HighWaterApplied;

// Adversarial-bytes safety for the `[version_byte | postcard(body)]` envelope
// used for openraft RPC payloads and storage records (see
// `tsoracle-openraft-toolkit/src/codec.rs`). Splits coverage between the framing
// (empty input, version-mismatch branch) and the postcard body decode by
// driving both `decode::<HighWaterApplied>(0, data)` (expected-version path)
// and `decode::<HighWaterApplied>(data[0], data)` (escape from the version
// gate so the postcard decoder sees the whole body for any first byte).
//
// `HighWaterApplied` is intentionally tiny — it makes the framing logic the
// dominant source of new coverage relative to the existing
// `snapshot_payload_decode` target, which already saturates postcard on a
// large serde shape.
fuzz_target!(|data: &[u8]| {
    let _ = decode::<HighWaterApplied>(0, data);
    if let Some(&first) = data.first() {
        let _ = decode::<HighWaterApplied>(first, data);
    }
});
