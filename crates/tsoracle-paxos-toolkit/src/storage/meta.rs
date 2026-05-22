//
//  ░▀█▀░█▀▀░█▀█░█▀▄░█▀█░█▀▀░█░░░█▀▀
//  ░░█░░▀▀█░█░█░█▀▄░█▀█░█░░░█░░░█▀▀
//  ░░▀░░▀▀▀░▀▀▀░▀░▀░▀░▀░▀▀▀░▀▀▀░▀▀▀
//
//  tsoracle — Distributed Timestamp Oracle
//
//  Copyright (c) 2026 Prisma Risk
//  Licensed under the Apache License, Version 2.0
//  https://github.com/prisma-risk/tsoracle
//

//! Typed serializers for the meta column. Ballot, decided_idx, etc. are
//! persisted as a fixed-shape byte string so the Storage impl never sees raw
//! `Vec<u8>` for fields with known shape.
//!
//! u64 fields use little-endian 8-byte encoding (faster than postcard for
//! a fixed-width type, and iteration order doesn't matter for meta keys —
//! they're singletons keyed by name). Ballot / Snapshot / StopSign use
//! postcard for forward compatibility with omnipaxos releases that add
//! fields.

use omnipaxos::ballot_leader_election::Ballot;
use serde::{Serialize, de::DeserializeOwned};

use crate::codec::{CodecError, decode as codec_decode, encode as codec_encode};

#[derive(Debug, thiserror::Error)]
pub enum MetaError {
    #[error(transparent)]
    Codec(#[from] CodecError),
    #[error("u64 field has wrong length: expected 8 bytes, got {0}")]
    WrongU64Length(usize),
}

#[must_use]
pub fn encode_u64(value: u64) -> Vec<u8> {
    value.to_le_bytes().to_vec()
}

pub fn decode_u64(bytes: &[u8]) -> Result<u64, MetaError> {
    let arr: [u8; 8] = bytes
        .try_into()
        .map_err(|_| MetaError::WrongU64Length(bytes.len()))?;
    Ok(u64::from_le_bytes(arr))
}

#[must_use = "encoded ballot bytes must be persisted or the encode call had no effect"]
pub fn encode_ballot(ballot: &Ballot) -> Result<Vec<u8>, MetaError> {
    Ok(codec_encode(ballot)?)
}

#[must_use = "decoded ballot must be inspected; discarding it discards the read"]
pub fn decode_ballot(bytes: &[u8]) -> Result<Ballot, MetaError> {
    Ok(codec_decode(bytes)?)
}

#[must_use = "encoded bytes must be persisted or the encode call had no effect"]
pub fn encode_postcard<T: Serialize>(value: &T) -> Result<Vec<u8>, MetaError> {
    Ok(codec_encode(value)?)
}

#[must_use = "decoded value must be inspected; discarding it discards the read"]
pub fn decode_postcard<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, MetaError> {
    Ok(codec_decode(bytes)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use omnipaxos::ballot_leader_election::Ballot;

    #[test]
    fn ballot_round_trip() {
        let original = Ballot {
            config_id: 1,
            n: 42,
            priority: 0,
            pid: 7,
        };
        let encoded = encode_ballot(&original).expect("encode");
        let decoded = decode_ballot(&encoded).expect("decode");
        assert_eq!(original.config_id, decoded.config_id);
        assert_eq!(original.n, decoded.n);
        assert_eq!(original.priority, decoded.priority);
        assert_eq!(original.pid, decoded.pid);
    }

    #[test]
    fn decided_idx_round_trip() {
        let original: u64 = 0x1234_5678_9ABC_DEF0;
        let encoded = encode_u64(original);
        assert_eq!(encoded.len(), 8);
        let decoded = decode_u64(&encoded).expect("decode");
        assert_eq!(decoded, original);
    }

    #[test]
    fn decode_u64_rejects_wrong_length() {
        assert!(decode_u64(&[0u8; 7]).is_err());
        assert!(decode_u64(&[0u8; 9]).is_err());
    }
}
