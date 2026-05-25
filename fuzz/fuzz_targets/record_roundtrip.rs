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
use tsoracle_driver_file::record::{decode, encode};

fuzz_target!(|data: &[u8]| {
    // Forward direction: any u64 round-trips through encode/decode unchanged.
    if data.len() >= 8 {
        let value = u64::from_le_bytes(data[..8].try_into().unwrap());
        let encoded = encode(value);
        let decoded = decode(&encoded).expect("encoded record must decode");
        assert_eq!(decoded, value);
    }
    // Reverse direction: any bytes that decode successfully must re-encode
    // to the exact input. Under the strict-trailing-bytes contract from
    // the preceding RecordError::TrailingBytes change, decode only accepts
    // inputs of length RECORD_LEN, so full equality (not just prefix
    // equality) is the correct property.
    if let Ok(decoded_value) = decode(data) {
        let re_encoded = encode(decoded_value);
        assert_eq!(&re_encoded[..], data);
    }
});
