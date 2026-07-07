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

//! Shared framing for versioned, CRC-protected durable records.

pub const HEADER_LEN: usize = 5;
pub const CRC_LEN: usize = 4;

#[derive(Debug, PartialEq, Eq)]
pub enum FrameError {
    TooShort(usize),
    BadMagic([u8; 4]),
    UnsupportedVersion(u8),
    ChecksumMismatch { stored: u32, computed: u32 },
    Malformed,
}

pub fn encode<F>(
    magic: &[u8; 4],
    version: u8,
    payload_capacity: usize,
    encode_payload: F,
) -> Vec<u8>
where
    F: FnOnce(&mut Vec<u8>),
{
    let mut buf = Vec::with_capacity(HEADER_LEN + payload_capacity + CRC_LEN);
    buf.extend_from_slice(magic);
    buf.push(version);
    encode_payload(&mut buf);
    let crc = crc32c::crc32c(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());
    buf
}

pub fn decode<'a>(
    bytes: &'a [u8],
    magic: &[u8; 4],
    version: u8,
    minimum_len: usize,
) -> Result<&'a [u8], FrameError> {
    if bytes.len() < minimum_len {
        return Err(FrameError::TooShort(bytes.len()));
    }
    if &bytes[0..4] != magic {
        return Err(FrameError::BadMagic([
            bytes[0], bytes[1], bytes[2], bytes[3],
        ]));
    }
    if bytes[4] != version {
        return Err(FrameError::UnsupportedVersion(bytes[4]));
    }

    let body = &bytes[..bytes.len() - CRC_LEN];
    let stored = u32::from_le_bytes(
        bytes[bytes.len() - CRC_LEN..]
            .try_into()
            .map_err(|_| FrameError::Malformed)?,
    );
    let computed = crc32c::crc32c(body);
    if stored != computed {
        return Err(FrameError::ChecksumMismatch { stored, computed });
    }

    Ok(&body[HEADER_LEN..])
}
