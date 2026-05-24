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

#![doc = include_str!("../README.md")]

use serde::{Serialize, de::DeserializeOwned};

/// Failure modes of the version-prefixed postcard codec.
///
/// `Encode` and `Decode` are kept distinct so a caller can tell which
/// direction failed; both carry the underlying [`postcard::Error`] as the
/// error source rather than via a `From` conversion, so a stray `?` on a
/// `postcard` result never silently becomes a `CodecError`.
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    /// The payload had no leading version byte (it was empty).
    #[error("payload empty")]
    Empty,
    /// The leading version byte did not match the version the reader expected.
    /// A stale reader hits this instead of misdecoding old bytes against a new
    /// struct layout.
    #[error("version mismatch: expected {expected}, got {actual}")]
    Version { expected: u8, actual: u8 },
    /// `postcard` failed to serialize the value.
    #[error("encode failed: {0}")]
    Encode(#[source] postcard::Error),
    /// `postcard` failed to deserialize the framed body.
    #[error("decode failed: {0}")]
    Decode(#[source] postcard::Error),
}

/// Encode `value` as `[version | postcard(value)]`.
///
/// The leading byte lets the on-disk/wire format evolve without a silent
/// misdecode: see [`decode`], which rejects a foreign version. The `version`
/// is a parameter rather than a constant so each consumer owns its own schema
/// version and can evolve it independently.
pub fn encode<T: Serialize>(version: u8, value: &T) -> Result<Vec<u8>, CodecError> {
    let body = postcard::to_stdvec(value).map_err(CodecError::Encode)?;
    let mut out = Vec::with_capacity(1 + body.len());
    out.push(version);
    out.extend_from_slice(&body);
    Ok(out)
}

/// Decode a payload produced by [`encode`], rejecting a version mismatch.
///
/// Returns [`CodecError::Version`] when the leading byte differs from
/// `expected_version` — a stale reader fails loudly instead of parsing old
/// bytes against a new struct layout.
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
            name: "tsoracle".into(),
        };
        let bytes = encode(1, &original).expect("encode");
        assert_eq!(bytes[0], 1);
        let decoded: Sample = decode(1, &bytes).expect("decode");
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
            name: "hello-world-storage-roundtrip".into(),
        };
        let bytes = encode(1, &original).expect("encode");
        assert!(bytes.len() >= 16, "payload should be non-trivial");
        let truncated = &bytes[..bytes.len() / 2];
        assert!(matches!(
            decode::<Sample>(1, truncated),
            Err(CodecError::Decode(_))
        ));
    }

    use proptest::prelude::*;

    proptest! {
        // Roundtrip: encode then decode at the same version returns the
        // original value, for any (version, payload).
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

        // Version-mismatch detection: decoding with the wrong expected version
        // returns CodecError::Version carrying the exact values, for any pair
        // (encoded, expected) where the two differ.
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
