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
use tsoracle_paxos_toolkit::codec::{SCHEMA_VERSION, decode, encode};

// Reverse-roundtrip property for the version-framed paxos snapshot codec. The
// `HighWaterSnapshot` is persisted to the RocksDB meta column and transferred
// during snapshot catch-up as `[SCHEMA_VERSION | postcard(..)]` (the paxos
// storage layer encodes it with `encode_postcard` and reads it back with
// `decode_postcard`, both of which route through this codec). Beyond "does not
// panic" (covered by `paxos_snapshot_decode`), we assert the codec is a stable
// canonical form: decode -> encode -> decode reproduces the same value.
//
// The assertion is value equality on the fields, deliberately NOT byte
// equality of the re-encodings:
//   - `applied_barriers` is a `HashMap`, whose iteration order is randomized
//     per instance, so two equal maps can serialize to different byte orders;
//     `HashMap`'s `==` compares them as sets, which is order-independent.
//   - `postcard::from_bytes` ignores trailing bytes, so `re-encode == input`
//     would false-positive on a valid prefix followed by junk.
fuzz_target!(|data: &[u8]| {
    let Ok(value) = decode::<HighWaterSnapshot>(SCHEMA_VERSION, data) else {
        return;
    };
    let reencoded =
        encode(SCHEMA_VERSION, &value).expect("re-encoding a decoded value must succeed");
    let roundtripped: HighWaterSnapshot =
        decode(SCHEMA_VERSION, &reencoded).expect("re-encoded bytes must decode");
    assert_eq!(
        value.value, roundtripped.value,
        "codec changed the high-water value across decode -> encode -> decode"
    );
    assert_eq!(
        value.applied_barriers, roundtripped.applied_barriers,
        "codec changed the applied-barriers ledger across decode -> encode -> decode"
    );
});
