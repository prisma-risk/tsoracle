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
use tsoracle_codec::decode_framed;
use tsoracle_driver_openraft::{HighWaterStateMachineSnapshot, OpenraftEntry};
use tsoracle_openraft_toolkit::{MAX_READABLE_VERSION, MIN_READABLE_VERSION, decode};

fuzz_target!(|data: &[u8]| {
    // Treat `data` as a version-framed snapshot record `[version | body]` and
    // decode it through the real multi-version reader. `decode_framed` reads
    // the leading byte and rejects anything outside the readable range
    // [MIN_READABLE_VERSION, MAX_READABLE_VERSION] with `VersionUnsupported`
    // before touching the body — the "old reader rejects a too-new record at
    // the version gate rather than misdecoding" property (#583). The leading
    // byte must therefore never produce a misdecode or a panic when it is out
    // of range. `HighWaterStateMachineSnapshot` is the `VersionedCodec` type
    // that carries the dense v5 layout; `OpenraftEntry`'s range gate lives in
    // the log store's `decode_entry_record` (covered by a unit test) and is
    // exercised here only for no-panic coverage via the single-version reader.
    let snap = decode_framed::<HighWaterStateMachineSnapshot>(
        MIN_READABLE_VERSION,
        MAX_READABLE_VERSION,
        data,
    );
    if let Some(&leading) = data.first() {
        if leading < MIN_READABLE_VERSION || leading > MAX_READABLE_VERSION {
            assert!(
                snap.is_err(),
                "out-of-range framed version {leading} must be rejected, not misdecoded"
            );
        }
    }

    // No-panic coverage of the entry decoder (single-version exact-match path;
    // no range assertion because `decode` matches an exact version rather than
    // gating a range).
    if let Some((&version, body)) = data.split_first() {
        let _ = decode::<OpenraftEntry>(version, body);
    }
});
