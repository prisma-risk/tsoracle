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

pub const MAGIC: &[u8; 4] = b"TSOD";
pub const VERSION: u8 = 1;

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

pub fn encode(map: &BTreeMap<String, u64>, cap: u64) -> Vec<u8> {
    debug_assert!(map.len() <= u32::MAX as usize, "key_count exceeds u32");
    let mut buf = Vec::with_capacity(4 + 1 + 8 + 4 + map.len() * 24 + 4);
    buf.extend_from_slice(MAGIC);
    buf.push(VERSION);
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
    let crc = crc32c::crc32c(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());
    buf
}

pub fn decode(bytes: &[u8]) -> Result<(BTreeMap<String, u64>, u64), DenseRecordError> {
    // minimum: magic(4) + version(1) + cap(8) + key_count(4) + crc(4) = 21
    if bytes.len() < 21 {
        return Err(DenseRecordError::TooShort(bytes.len()));
    }
    if &bytes[0..4] != MAGIC {
        return Err(DenseRecordError::BadMagic([
            bytes[0], bytes[1], bytes[2], bytes[3],
        ]));
    }
    if bytes[4] != VERSION {
        return Err(DenseRecordError::UnsupportedVersion(bytes[4]));
    }
    let body = &bytes[..bytes.len() - 4];
    let stored = u32::from_le_bytes(
        bytes[bytes.len() - 4..]
            .try_into()
            .map_err(|_| DenseRecordError::Malformed)?,
    );
    let computed = crc32c::crc32c(body);
    if stored != computed {
        return Err(DenseRecordError::ChecksumMismatch { stored, computed });
    }
    let cap = u64::from_le_bytes(
        bytes[5..13]
            .try_into()
            .map_err(|_| DenseRecordError::Malformed)?,
    );
    let key_count = u32::from_le_bytes(
        bytes[13..17]
            .try_into()
            .map_err(|_| DenseRecordError::Malformed)?,
    );
    let mut pos = 17;
    let mut map = BTreeMap::new();
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
}
