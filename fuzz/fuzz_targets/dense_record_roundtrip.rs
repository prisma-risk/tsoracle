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

use std::collections::BTreeMap;

use libfuzzer_sys::fuzz_target;
use tsoracle_driver_file::dense_record;

// Two round-trip properties for the dense-sequence record codec, mirroring
// `record_roundtrip` for the high-water record.
fuzz_target!(|data: &[u8]| {
    // Reverse: `decode` is strict (it rejects trailing bytes), so any input that
    // decodes successfully must re-encode to the exact same bytes. Catches any
    // asymmetry between the encoder and decoder (field order, length framing,
    // checksum placement).
    if let Ok((map, cap)) = dense_record::decode(data) {
        let re_encoded = dense_record::encode(&map, cap);
        assert_eq!(re_encoded, data);
    }

    // Forward: a map derived from the input must round-trip through
    // encode -> decode unchanged. Keys are synthesized as short, valid UTF-8
    // (`k0`, `k1`, ...) so the encoder's key-length framing is exercised without
    // needing the `arbitrary` crate.
    if data.len() >= 8 {
        let cap = u64::from_le_bytes(data[..8].try_into().unwrap());
        let mut map = BTreeMap::new();
        for (i, chunk) in data[8..].chunks(8).enumerate().take(64) {
            let mut buf = [0u8; 8];
            buf[..chunk.len()].copy_from_slice(chunk);
            map.insert(format!("k{i}"), u64::from_le_bytes(buf));
        }
        let (decoded, decoded_cap) = dense_record::decode(&dense_record::encode(&map, cap))
            .expect("an encoded dense record must always decode");
        assert_eq!(decoded, map);
        assert_eq!(decoded_cap, cap);
    }
});
