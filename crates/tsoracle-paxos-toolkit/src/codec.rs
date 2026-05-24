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

//! Version-prefixed postcard codec used by every module that persists
//! OmniPaxos state. Every payload is encoded as `[version_byte |
//! postcard(value)]`; the leading byte lets the on-disk format evolve without
//! a silent misdecode — a stale reader hits [`CodecError::Version`] instead of
//! parsing old bytes against a new struct layout. Centralizing it also gives
//! one place to swap formats later and to attach diagnostic context to errors.

use serde::{Serialize, de::DeserializeOwned};

#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("payload empty")]
    Empty,
    #[error("version mismatch: expected {expected}, got {actual}")]
    Version { expected: u8, actual: u8 },
    #[error("encode failed: {0}")]
    Encode(#[source] postcard::Error),
    #[error("decode failed: {0}")]
    Decode(#[source] postcard::Error),
}

/// On-disk schema version stamped as the leading byte of every framed ballot,
/// stopsign, snapshot, and log entry. Bump when a persisted struct's postcard
/// layout changes incompatibly so a stale reader fails loudly.
pub const SCHEMA_VERSION: u8 = 1;

/// Encode `value` as `[version | postcard(value)]`.
pub fn encode<T: Serialize>(version: u8, value: &T) -> Result<Vec<u8>, CodecError> {
    let body = postcard::to_stdvec(value).map_err(CodecError::Encode)?;
    let mut out = Vec::with_capacity(1 + body.len());
    out.push(version);
    out.extend_from_slice(&body);
    Ok(out)
}

/// Decode a payload produced by [`encode`], rejecting a version mismatch.
pub fn decode<T: DeserializeOwned>(expected_version: u8, bytes: &[u8]) -> Result<T, CodecError> {
    let (first, rest) = bytes.split_first().ok_or(CodecError::Empty)?;
    if *first != expected_version {
        return Err(CodecError::Version {
            expected: expected_version,
            actual: *first,
        });
    }
    postcard::from_bytes(rest).map_err(CodecError::Decode)
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
        let bytes = encode(SCHEMA_VERSION, &original).expect("encode");
        assert_eq!(bytes[0], SCHEMA_VERSION);
        let decoded: Sample = decode(SCHEMA_VERSION, &bytes).expect("decode");
        assert_eq!(original, decoded);
    }

    #[test]
    fn decode_rejects_wrong_version() {
        let bytes = encode(
            2,
            &Sample {
                idx: 1,
                name: "x".into(),
            },
        )
        .expect("encode");
        let err = decode::<Sample>(1, &bytes).expect_err("must reject");
        assert!(matches!(
            err,
            CodecError::Version {
                expected: 1,
                actual: 2
            }
        ));
    }

    #[test]
    fn decode_rejects_empty() {
        let err = decode::<Sample>(1, &[]).expect_err("must reject");
        assert!(matches!(err, CodecError::Empty));
    }

    #[test]
    fn decode_rejects_truncated_input() {
        let original = Sample {
            idx: u64::MAX,
            name: "hello-world-paxos-storage-roundtrip".into(),
        };
        let bytes = encode(SCHEMA_VERSION, &original).expect("encode");
        assert!(bytes.len() >= 16, "payload should be non-trivial");
        let truncated = &bytes[..bytes.len() / 2];
        assert!(matches!(
            decode::<Sample>(SCHEMA_VERSION, truncated),
            Err(CodecError::Decode(_))
        ));
    }

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn encode_decode_roundtrip_any(
            version in any::<u8>(),
            idx in any::<u64>(),
            name in any::<String>(),
        ) {
            let s = Sample { idx, name };
            let bytes = encode(version, &s).unwrap();
            prop_assert_eq!(bytes[0], version);
            let back: Sample = decode(version, &bytes).unwrap();
            prop_assert_eq!(s, back);
        }

        #[test]
        fn decode_rejects_any_version_mismatch(
            encoded in any::<u8>(),
            expected in any::<u8>(),
            idx in any::<u64>(),
            name in any::<String>(),
        ) {
            prop_assume!(encoded != expected);
            let bytes = encode(encoded, &Sample { idx, name }).unwrap();
            match decode::<Sample>(expected, &bytes) {
                Err(CodecError::Version { expected: e, actual: a }) => {
                    prop_assert_eq!(e, expected);
                    prop_assert_eq!(a, encoded);
                }
                other => prop_assert!(false, "expected Version mismatch; got {other:?}"),
            }
        }
    }
}
