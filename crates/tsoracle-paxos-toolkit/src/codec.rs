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

//! Postcard codec wrapper used by every other module that persists OmniPaxos
//! state. Centralizing it gives one place to swap formats later (e.g., to
//! `bincode`) and one place to attach diagnostic context to encode errors.

use serde::{Serialize, de::DeserializeOwned};

#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("encode failed: {0}")]
    Encode(#[source] postcard::Error),
    #[error("decode failed: {0}")]
    Decode(#[source] postcard::Error),
}

pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, CodecError> {
    postcard::to_stdvec(value).map_err(CodecError::Encode)
}

pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, CodecError> {
    postcard::from_bytes(bytes).map_err(CodecError::Decode)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Sample {
        idx: u64,
        name: String,
    }

    #[test]
    fn encode_decode_roundtrip() {
        let original = Sample {
            idx: 42,
            name: "paxos".into(),
        };
        let bytes = encode(&original).expect("encode");
        let decoded: Sample = decode(&bytes).expect("decode");
        assert_eq!(original, decoded);
    }

    #[test]
    fn decode_rejects_truncated_input() {
        // Use a payload long enough that bytes.len() / 2 cuts through an
        // interior field boundary rather than the trivially-small minimum.
        let original = Sample {
            idx: u64::MAX,
            name: "hello-world-paxos-storage-roundtrip".into(),
        };
        let bytes = encode(&original).expect("encode");
        assert!(bytes.len() >= 16, "payload should be non-trivial");
        let truncated = &bytes[..bytes.len() / 2];
        assert!(decode::<Sample>(truncated).is_err());
    }
}
