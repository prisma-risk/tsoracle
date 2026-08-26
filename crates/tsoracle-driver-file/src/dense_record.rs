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

//! Versioned, checksummed on-disk record for the dense-sequence map.
//! Layout (little-endian): "TSOD" | u8 version | u64 cap | u32 key_count |
//! key_count × (u16 key_len | key bytes | u64 counter) | u32 crc32c.

use std::collections::BTreeMap;

use crate::framed_record::{self, FrameError};

pub const MAGIC: &[u8; 4] = b"TSOD";
pub const VERSION: u8 = 1;
const MIN_LEN: usize = framed_record::HEADER_LEN + 8 + 4 + framed_record::CRC_LEN;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum DenseRecordError {
    #[error("dense record too short: {0} bytes")]
    TooShort(usize),
    #[error("bad magic: {0:?}")]
    BadMagic([u8; 4]),
    #[error("unsupported dense record version {0}")]
    UnsupportedVersion(u8),
    #[error("checksum mismatch: stored {stored:#010x}, computed {computed:#010x}")]
    ChecksumMismatch { stored: u32, computed: u32 },
    #[error("malformed dense record body")]
    Malformed,
    #[error("non-utf8 key in dense record")]
    NonUtf8Key,
}

impl From<FrameError> for DenseRecordError {
    fn from(error: FrameError) -> Self {
        match error {
            FrameError::TooShort(len) => DenseRecordError::TooShort(len),
            FrameError::BadMagic(magic) => DenseRecordError::BadMagic(magic),
            FrameError::UnsupportedVersion(version) => {
                DenseRecordError::UnsupportedVersion(version)
            }
            FrameError::ChecksumMismatch { stored, computed } => {
                DenseRecordError::ChecksumMismatch { stored, computed }
            }
            FrameError::Malformed => DenseRecordError::Malformed,
        }
    }
}

pub fn encode(map: &BTreeMap<String, u64>, cap: u64) -> Vec<u8> {
    debug_assert!(map.len() <= u32::MAX as usize, "key_count exceeds u32");
    framed_record::encode(MAGIC, VERSION, 8 + 4 + map.len() * 24, |buf| {
        buf.extend_from_slice(&cap.to_le_bytes());
        buf.extend_from_slice(&(map.len() as u32).to_le_bytes());
        for (key, counter) in map {
            let kb = key.as_bytes();
            // key length is bounded by MAX_SEQ_KEY_LEN (<= u16::MAX) at the API edge.
            debug_assert!(kb.len() <= u16::MAX as usize, "key length exceeds u16");
            buf.extend_from_slice(&(kb.len() as u16).to_le_bytes());
            buf.extend_from_slice(kb);
            buf.extend_from_slice(&counter.to_le_bytes());
        }
    })
}

pub fn decode(bytes: &[u8]) -> Result<(BTreeMap<String, u64>, u64), DenseRecordError> {
    let body =
        framed_record::decode(bytes, MAGIC, VERSION, MIN_LEN).map_err(DenseRecordError::from)?;
    let cap = u64::from_le_bytes(
        body[0..8]
            .try_into()
            .map_err(|_| DenseRecordError::Malformed)?,
    );
    let key_count = u32::from_le_bytes(
        body[8..12]
            .try_into()
            .map_err(|_| DenseRecordError::Malformed)?,
    );
    let mut pos = 12;
    let mut map = BTreeMap::new();
    let mut prev_key: Option<String> = None;
    for _ in 0..key_count {
        if pos + 2 > body.len() {
            return Err(DenseRecordError::Malformed);
        }
        let klen = u16::from_le_bytes(
            body[pos..pos + 2]
                .try_into()
                .map_err(|_| DenseRecordError::Malformed)?,
        ) as usize;
        pos += 2;
        if pos + klen + 8 > body.len() {
            return Err(DenseRecordError::Malformed);
        }
        let key = std::str::from_utf8(&body[pos..pos + klen])
            .map_err(|_| DenseRecordError::NonUtf8Key)?
            .to_string();
        pos += klen;
        let counter = u64::from_le_bytes(
            body[pos..pos + 8]
                .try_into()
                .map_err(|_| DenseRecordError::Malformed)?,
        );
        pos += 8;
        // Keys must appear in strictly ascending order. `encode` emits them in
        // `BTreeMap` order (sorted, unique), so any record this codec produced
        // satisfies this. Enforcing it on decode (a) rejects duplicate keys,
        // which `BTreeMap::insert` would otherwise silently coalesce — dropping
        // a counter and decoding to a different map than the bytes describe —
        // and (b) rejects non-canonical orderings, making decode canonical so
        // that re-encoding a decoded record reproduces the exact input bytes
        // (the strict round-trip the fuzz target asserts).
        if let Some(prev) = &prev_key
            && key <= *prev
        {
            return Err(DenseRecordError::Malformed);
        }
        prev_key = Some(key.clone());
        map.insert(key, counter);
    }
    if pos != body.len() {
        return Err(DenseRecordError::Malformed);
    }
    Ok((map, cap))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> BTreeMap<String, u64> {
        let mut m = BTreeMap::new();
        m.insert("orders".to_string(), 42);
        m.insert("users".to_string(), 7);
        m
    }

    #[test]
    fn round_trips() {
        let m = sample();
        let bytes = encode(&m, 10_000);
        let (decoded, cap) = decode(&bytes).unwrap();
        assert_eq!(decoded, m);
        assert_eq!(cap, 10_000);
    }

    #[test]
    fn empty_map_round_trips() {
        let m = BTreeMap::new();
        let bytes = encode(&m, 500);
        let (decoded, cap) = decode(&bytes).unwrap();
        assert!(decoded.is_empty());
        assert_eq!(cap, 500);
    }

    #[test]
    fn detects_corruption() {
        let mut bytes = encode(&sample(), 10_000);
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff; // corrupt the crc
        assert!(matches!(
            decode(&bytes),
            Err(DenseRecordError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn rejects_bad_magic() {
        let mut bytes = encode(&sample(), 1);
        bytes[0] = b'X';
        // crc will also fail, but magic is checked first.
        assert!(matches!(decode(&bytes), Err(DenseRecordError::BadMagic(_))));
    }

    #[test]
    fn rejects_unsupported_version() {
        let mut bytes = encode(&sample(), 1);
        bytes[4] = 0; // flip version to 0
        // crc will now mismatch too, but version is checked before crc.
        assert!(matches!(
            decode(&bytes),
            Err(DenseRecordError::UnsupportedVersion(0))
        ));
    }

    #[test]
    fn rejects_non_utf8_key() {
        let mut body = Vec::new();
        body.extend_from_slice(MAGIC);
        body.push(VERSION);
        body.extend_from_slice(&1u64.to_le_bytes()); // cap
        body.extend_from_slice(&1u32.to_le_bytes()); // key_count = 1
        body.extend_from_slice(&2u16.to_le_bytes()); // key_len = 2
        body.extend_from_slice(&[0xff, 0xfe]); // invalid utf8 key bytes
        body.extend_from_slice(&7u64.to_le_bytes()); // counter
        let crc = crc32c::crc32c(&body);
        body.extend_from_slice(&crc.to_le_bytes());
        assert_eq!(decode(&body), Err(DenseRecordError::NonUtf8Key));
    }

    #[test]
    fn rejects_short() {
        assert!(matches!(
            decode(&[0u8; 3]),
            Err(DenseRecordError::TooShort(3))
        ));
    }

    /// Build a record with an arbitrary, possibly non-canonical key list and a
    /// valid CRC — so the body survives the checksum gate and the structural
    /// (ordering) checks are what the test exercises.
    fn encode_raw(cap: u64, entries: &[(&str, u64)]) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(MAGIC);
        body.push(VERSION);
        body.extend_from_slice(&cap.to_le_bytes());
        body.extend_from_slice(&(entries.len() as u32).to_le_bytes());
        for (k, c) in entries {
            body.extend_from_slice(&(k.len() as u16).to_le_bytes());
            body.extend_from_slice(k.as_bytes());
            body.extend_from_slice(&c.to_le_bytes());
        }
        let crc = crc32c::crc32c(&body);
        body.extend_from_slice(&crc.to_le_bytes());
        body
    }

    #[test]
    fn rejects_duplicate_keys() {
        // A valid-CRC record listing the same key twice would silently drop one
        // counter under `BTreeMap::insert`, decoding to a different map than the
        // bytes describe. Reject it.
        let bytes = encode_raw(10_000, &[("orders", 1), ("orders", 2)]);
        assert_eq!(decode(&bytes), Err(DenseRecordError::Malformed));
    }

    #[test]
    fn rejects_out_of_order_keys() {
        // The encoder emits keys in ascending (BTreeMap) order, so a descending
        // pair is non-canonical: decoding it and re-encoding would reorder the
        // bytes, violating the strict round-trip the codec guarantees.
        // ("users" > "orders", so listing them in this order is descending.)
        let bytes = encode_raw(10_000, &[("users", 7), ("orders", 42)]);
        assert_eq!(decode(&bytes), Err(DenseRecordError::Malformed));
    }

    #[test]
    fn rejects_truncated_key_entry() {
        // A record that claims a key but is cut off mid-entry must be Malformed,
        // not silently accepted. Start from a valid single-key record, then lop
        // off the trailing counter bytes (and fix the CRC so the truncation —
        // not the checksum — is what trips the decoder).
        let mut body = Vec::new();
        body.extend_from_slice(MAGIC);
        body.push(VERSION);
        body.extend_from_slice(&1u64.to_le_bytes()); // cap
        body.extend_from_slice(&1u32.to_le_bytes()); // key_count = 1
        body.extend_from_slice(&3u16.to_le_bytes()); // key_len = 3
        body.extend_from_slice(b"abc"); // key
        // Counter (8 bytes) omitted entirely → the entry runs past body end.
        let crc = crc32c::crc32c(&body);
        body.extend_from_slice(&crc.to_le_bytes());
        assert_eq!(decode(&body), Err(DenseRecordError::Malformed));
    }

    #[test]
    fn accepts_canonical_ascending_keys() {
        let bytes = encode_raw(10_000, &[("orders", 42), ("users", 7)]);
        let (map, cap) = decode(&bytes).unwrap();
        assert_eq!(cap, 10_000);
        assert_eq!(map.get("orders"), Some(&42));
        assert_eq!(map.get("users"), Some(&7));
        // A canonical record re-encodes to the exact input (strict round-trip).
        assert_eq!(encode(&map, cap), bytes);
    }
}
