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

//! Binary wire codec for openraft RPC payloads and storage records.
//!
//! Every payload is encoded as `[version_byte | postcard(value)]`. The leading
//! byte lets us evolve the wire format without an explicit migration when both
//! sides of an upgrade run mixed versions briefly.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// On-disk/wire schema version stamped as the leading byte of every framed
/// snapshot, log entry, and storage record produced by this toolkit. Bump
/// when a persisted struct's postcard layout changes in a
/// backward-incompatible way (field reorder/insert/remove); a stale reader
/// then fails loudly with [`CodecError::Version`] instead of misdecoding.
pub const SCHEMA_VERSION: u8 = 1;

#[derive(Debug, Error)]
pub enum CodecError {
    #[error("payload empty")]
    Empty,
    #[error("version mismatch: expected {expected}, got {actual}")]
    Version { expected: u8, actual: u8 },
    #[error("decode failed: {0}")]
    Decode(#[from] postcard::Error),
}

/// Encode `value` as `[version | postcard(value)]`.
pub fn encode<T: Serialize>(version: u8, value: &T) -> Result<Vec<u8>, CodecError> {
    let body = postcard::to_stdvec(value)?;
    let mut out = Vec::with_capacity(1 + body.len());
    out.push(version);
    out.extend_from_slice(&body);
    Ok(out)
}

/// Decode a payload previously produced by [`encode`], rejecting any version mismatch.
pub fn decode<T: for<'de> Deserialize<'de>>(
    expected_version: u8,
    bytes: &[u8],
) -> Result<T, CodecError> {
    let (first, rest) = bytes.split_first().ok_or(CodecError::Empty)?;
    if *first != expected_version {
        return Err(CodecError::Version {
            expected: expected_version,
            actual: *first,
        });
    }
    Ok(postcard::from_bytes(rest)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Payload {
        a: u64,
        b: String,
    }

    #[test]
    fn roundtrip_preserves_payload() {
        let p = Payload {
            a: 42,
            b: "hello".into(),
        };
        let bytes = encode(1, &p).unwrap();
        assert_eq!(bytes[0], 1);
        let back: Payload = decode(1, &bytes).unwrap();
        assert_eq!(p, back);
    }

    #[test]
    fn decode_rejects_wrong_version() {
        let p = Payload {
            a: 1,
            b: "x".into(),
        };
        let bytes = encode(2, &p).unwrap();
        let err = decode::<Payload>(1, &bytes).unwrap_err();
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
        let err = decode::<Payload>(1, &[]).unwrap_err();
        assert!(matches!(err, CodecError::Empty));
    }

    use proptest::prelude::*;

    proptest! {
        // Roundtrip: encode then decode at the same version must return the
        // original value, for any (version, payload). The Payload's two fields
        // give us coverage of an integer + an arbitrary UTF-8 string body.
        #[test]
        fn encode_decode_roundtrip(
            version in any::<u8>(),
            a in any::<u64>(),
            b in any::<String>(),
        ) {
            let p = Payload { a, b };
            let bytes = encode(version, &p).unwrap();
            prop_assert_eq!(bytes[0], version);
            let back: Payload = decode(version, &bytes).unwrap();
            prop_assert_eq!(p, back);
        }

        // Version-mismatch detection: decoding with the wrong expected version
        // must return CodecError::Version{ expected, actual } carrying the
        // exact values, for any pair (encoded_version, expected_version) where
        // the two differ.
        #[test]
        fn decode_rejects_any_version_mismatch(
            encoded in any::<u8>(),
            expected in any::<u8>(),
            a in any::<u64>(),
            b in any::<String>(),
        ) {
            prop_assume!(encoded != expected);
            let bytes = encode(encoded, &Payload { a, b }).unwrap();
            match decode::<Payload>(expected, &bytes) {
                Err(CodecError::Version { expected: e, actual: c }) => {
                    prop_assert_eq!(e, expected);
                    prop_assert_eq!(c, encoded);
                }
                other => prop_assert!(
                    false,
                    "expected Version mismatch error; got {other:?}",
                ),
            }
        }
    }
}
