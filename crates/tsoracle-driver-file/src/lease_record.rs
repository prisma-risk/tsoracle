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

//! Versioned, checksummed on-disk record for the lease set.
//! Layout (little-endian): "TSOO" | u8 version | u32 record_count |
//! record_count × (u64 lease_id | u16 holder_len | holder bytes |
//! u64 holder_epoch | u64 ttl_ms | u64 ts_upper_bound | u64 expires_at_ms |
//! u8 superseded) | u32 crc32c.

use tsoracle_core::LeaseRecord;

use crate::framed_record::{self, FrameError};

pub const MAGIC: &[u8; 4] = b"TSOO";
pub const VERSION: u8 = 1;
const MIN_LEN: usize = framed_record::HEADER_LEN + 4 + framed_record::CRC_LEN;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum LeaseRecordError {
    #[error("lease record too short: {0} bytes")]
    TooShort(usize),
    #[error("bad magic: {0:?}")]
    BadMagic([u8; 4]),
    #[error("unsupported lease record version {0}")]
    UnsupportedVersion(u8),
    #[error("checksum mismatch: stored {stored:#010x}, computed {computed:#010x}")]
    ChecksumMismatch { stored: u32, computed: u32 },
    #[error("malformed lease record body")]
    Malformed,
}

impl From<FrameError> for LeaseRecordError {
    fn from(error: FrameError) -> Self {
        match error {
            FrameError::TooShort(len) => LeaseRecordError::TooShort(len),
            FrameError::BadMagic(magic) => LeaseRecordError::BadMagic(magic),
            FrameError::UnsupportedVersion(version) => {
                LeaseRecordError::UnsupportedVersion(version)
            }
            FrameError::ChecksumMismatch { stored, computed } => {
                LeaseRecordError::ChecksumMismatch { stored, computed }
            }
            FrameError::Malformed => LeaseRecordError::Malformed,
        }
    }
}

pub fn encode(records: &[LeaseRecord]) -> Vec<u8> {
    debug_assert!(
        records.len() <= u32::MAX as usize,
        "record_count exceeds u32"
    );
    let mut records = records.to_vec();
    records.sort_by_key(|r| r.lease_id);

    let holder_bytes: usize = records.iter().map(|r| r.holder.len()).sum();
    framed_record::encode(
        MAGIC,
        VERSION,
        4 + records.len() * 43 + holder_bytes,
        |buf| {
            buf.extend_from_slice(&(records.len() as u32).to_le_bytes());
            for record in &records {
                debug_assert!(
                    record.holder.len() <= u16::MAX as usize,
                    "holder length exceeds u16"
                );
                buf.extend_from_slice(&record.lease_id.to_le_bytes());
                buf.extend_from_slice(&(record.holder.len() as u16).to_le_bytes());
                buf.extend_from_slice(&record.holder);
                buf.extend_from_slice(&record.holder_epoch.to_le_bytes());
                buf.extend_from_slice(&record.ttl_ms.to_le_bytes());
                buf.extend_from_slice(&record.ts_upper_bound.to_le_bytes());
                buf.extend_from_slice(&record.expires_at_ms.to_le_bytes());
                buf.push(u8::from(record.superseded));
            }
        },
    )
}

pub fn decode(bytes: &[u8]) -> Result<Vec<LeaseRecord>, LeaseRecordError> {
    let body =
        framed_record::decode(bytes, MAGIC, VERSION, MIN_LEN).map_err(LeaseRecordError::from)?;
    let record_count = u32::from_le_bytes(
        body[0..4]
            .try_into()
            .map_err(|_| LeaseRecordError::Malformed)?,
    );
    let mut pos = 4;
    let mut records = Vec::with_capacity(record_count as usize);
    let mut prev_id = None;
    for _ in 0..record_count {
        if pos + 10 > body.len() {
            return Err(LeaseRecordError::Malformed);
        }
        let lease_id = u64::from_le_bytes(
            body[pos..pos + 8]
                .try_into()
                .map_err(|_| LeaseRecordError::Malformed)?,
        );
        pos += 8;
        if let Some(prev) = prev_id
            && lease_id <= prev
        {
            return Err(LeaseRecordError::Malformed);
        }
        prev_id = Some(lease_id);

        let holder_len = u16::from_le_bytes(
            body[pos..pos + 2]
                .try_into()
                .map_err(|_| LeaseRecordError::Malformed)?,
        ) as usize;
        pos += 2;
        if pos + holder_len + 33 > body.len() {
            return Err(LeaseRecordError::Malformed);
        }
        let holder = body[pos..pos + holder_len].to_vec();
        pos += holder_len;
        let holder_epoch = read_u64(body, &mut pos)?;
        let ttl_ms = read_u64(body, &mut pos)?;
        let ts_upper_bound = read_u64(body, &mut pos)?;
        let expires_at_ms = read_u64(body, &mut pos)?;
        let superseded = match body.get(pos) {
            Some(0) => false,
            Some(1) => true,
            Some(_) | None => return Err(LeaseRecordError::Malformed),
        };
        pos += 1;
        records.push(LeaseRecord {
            lease_id,
            holder,
            holder_epoch,
            ttl_ms,
            ts_upper_bound,
            expires_at_ms,
            superseded,
        });
    }
    if pos != body.len() {
        return Err(LeaseRecordError::Malformed);
    }
    Ok(records)
}

fn read_u64(body: &[u8], pos: &mut usize) -> Result<u64, LeaseRecordError> {
    if *pos + 8 > body.len() {
        return Err(LeaseRecordError::Malformed);
    }
    let value = u64::from_le_bytes(
        body[*pos..*pos + 8]
            .try_into()
            .map_err(|_| LeaseRecordError::Malformed)?,
    );
    *pos += 8;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(
        lease_id: u64,
        holder: &[u8],
        holder_epoch: u64,
        ttl_ms: u64,
        ts_upper_bound: u64,
        expires_at_ms: u64,
        superseded: bool,
    ) -> LeaseRecord {
        LeaseRecord {
            lease_id,
            holder: holder.to_vec(),
            holder_epoch,
            ttl_ms,
            ts_upper_bound,
            expires_at_ms,
            superseded,
        }
    }

    #[test]
    fn empty_set_round_trips() {
        let records = Vec::new();
        assert_eq!(decode(&encode(&records)).unwrap(), records);
    }

    #[test]
    fn two_records_round_trip_preserving_fields() {
        let records = vec![
            rec(2, b"holder-b", 7, 10_000, 2_000, 12_000, true),
            rec(1, b"holder-a", 3, 5_000, 1_000, 6_000, false),
        ];
        let mut expected = records.clone();
        expected.sort_by_key(|r| r.lease_id);
        assert_eq!(decode(&encode(&records)).unwrap(), expected);
    }

    #[test]
    fn rejects_bad_magic() {
        let mut bytes = encode(&[rec(1, b"h", 1, 5_000, 10, 5_010, false)]);
        bytes[0] = b'X';
        assert!(matches!(decode(&bytes), Err(LeaseRecordError::BadMagic(_))));
    }

    #[test]
    fn rejects_wrong_version() {
        let mut bytes = encode(&[rec(1, b"h", 1, 5_000, 10, 5_010, false)]);
        bytes[4] = 0;
        assert_eq!(decode(&bytes), Err(LeaseRecordError::UnsupportedVersion(0)));
    }

    #[test]
    fn rejects_flipped_crc_byte() {
        let mut bytes = encode(&[rec(1, b"h", 1, 5_000, 10, 5_010, false)]);
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        assert!(matches!(
            decode(&bytes),
            Err(LeaseRecordError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn rejects_truncated_body() {
        let mut bytes = encode(&[rec(1, b"h", 1, 5_000, 10, 5_010, false)]);
        bytes.truncate(bytes.len() - 2);
        assert!(matches!(
            decode(&bytes),
            Err(LeaseRecordError::ChecksumMismatch { .. }) | Err(LeaseRecordError::Malformed)
        ));
    }
}
