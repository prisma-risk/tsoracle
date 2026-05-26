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

#![doc = include_str!("../README.md")]

use std::io;

use serde::{Serialize, de::DeserializeOwned};

mod versioned;
pub use versioned::{
    VersionedCodec, decode_framed, decode_postcard_exact, encode_framed, encode_postcard,
};

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
    /// The body decoded successfully but `extra` bytes remained unconsumed.
    /// For a versioned on-disk format this signals corruption — e.g. a partial
    /// overwrite that left stale tail bytes — rather than a clean record.
    #[error("trailing bytes: {extra} unconsumed after a valid body")]
    TrailingBytes { extra: usize },
    /// The leading version byte was outside the reader's supported range
    /// `[min, max]`. Distinct from [`CodecError::Version`] (which a single-version
    /// reader returns); a multi-version reader uses this to fail loudly on a
    /// version it has no parser for.
    #[error("version unsupported: {actual} outside readable range [{min}, {max}]")]
    VersionUnsupported { min: u8, max: u8, actual: u8 },
    /// Down-conversion was asked to encode state not representable at `version`
    /// (a feature-gate violation: a field/behavior introduced at a later version
    /// is set while encoding an older layout). Fail-loud rather than silently
    /// dropping the value.
    #[error("value not representable at version {version}")]
    NotRepresentable { version: u8 },
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
/// bytes against a new struct layout. Returns [`CodecError::TrailingBytes`]
/// when the body decodes but leaves surplus bytes unconsumed: for a versioned
/// format that exists to catch drift, garbage appended to a valid record is a
/// corruption signal, not something to silently discard.
pub fn decode<T: DeserializeOwned>(expected_version: u8, bytes: &[u8]) -> Result<T, CodecError> {
    let (first, rest) = bytes.split_first().ok_or(CodecError::Empty)?;
    if *first != expected_version {
        return Err(CodecError::Version {
            expected: expected_version,
            actual: *first,
        });
    }
    let (value, remainder) = postcard::take_from_bytes(rest).map_err(CodecError::Decode)?;
    if !remainder.is_empty() {
        return Err(CodecError::TrailingBytes {
            extra: remainder.len(),
        });
    }
    Ok(value)
}

/// Map a [`CodecError`] to an [`io::Error`], tagging the message with `context`
/// so the failing boundary is identifiable.
///
/// A [`CodecError::Version`] mismatch becomes [`io::ErrorKind::InvalidData`]: a
/// foreign on-disk/wire format is a structured, distinguishable condition — a
/// caller can react to "this record is from another schema version" specifically
/// rather than treating it as a generic decode failure. Every other variant maps
/// through [`io::Error::other`].
///
/// Consumers whose trait surface speaks `io::Error` — the openraft
/// `RaftLogStorage` / `RaftStateMachine` implementations — share this one mapping
/// so the version-mismatch-to-`InvalidData` contract can't drift between them.
/// (The paxos toolkit surfaces [`CodecError`] through its own `thiserror` error
/// enum and so does not use this.)
pub fn codec_io_error(context: &str, err: CodecError) -> io::Error {
    match err {
        foreign_version @ (CodecError::Version { .. } | CodecError::VersionUnsupported { .. }) => {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{context}: {foreign_version}"),
            )
        }
        other => io::Error::other(format!("{context}: {other}")),
    }
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

    #[test]
    fn decode_rejects_trailing_bytes() {
        let original = Sample {
            idx: 7,
            name: "trailing".into(),
        };
        let mut bytes = encode(1, &original).expect("encode");
        // Simulate a partial overwrite that left stale tail bytes behind: a
        // valid body followed by garbage postcard never consumes.
        bytes.extend_from_slice(&[0xAB, 0xCD, 0xEF]);
        assert!(matches!(
            decode::<Sample>(1, &bytes),
            Err(CodecError::TrailingBytes { extra: 3 })
        ));
    }

    #[test]
    fn codec_io_error_maps_version_mismatch_to_invalid_data() {
        // A record stamped at version 2 read against version 1 is a version
        // mismatch — the boundary `codec_io_error` must surface as `InvalidData`.
        let v2_bytes = encode(
            2,
            &Sample {
                idx: 1,
                name: "x".into(),
            },
        )
        .unwrap();
        let err = decode::<Sample>(1, &v2_bytes).expect_err("must reject");
        assert!(matches!(err, CodecError::Version { .. }));
        let io_err = codec_io_error("vote decode", err);
        assert_eq!(io_err.kind(), io::ErrorKind::InvalidData);
        assert!(
            io_err.to_string().starts_with("vote decode: "),
            "context must prefix the message, got {io_err}"
        );
    }

    #[test]
    fn codec_io_error_maps_other_variants_to_other_kind() {
        // `Empty` is not a version mismatch, so it must not masquerade as the
        // `InvalidData` reserved for a foreign schema version.
        let io_err = codec_io_error("vote decode", CodecError::Empty);
        assert_ne!(io_err.kind(), io::ErrorKind::InvalidData);
        assert!(io_err.to_string().starts_with("vote decode: "));
    }

    #[test]
    fn codec_io_error_maps_version_unsupported_to_invalid_data() {
        // A multi-version reader's foreign-version rejection must carry the same
        // `InvalidData` contract as the single-version `Version` mismatch, so the
        // openraft storage layer can still detect a foreign schema version.
        let err = CodecError::VersionUnsupported {
            min: 3,
            max: 3,
            actual: 7,
        };
        let io_err = codec_io_error("log decode", err);
        assert_eq!(io_err.kind(), io::ErrorKind::InvalidData);
        assert!(io_err.to_string().starts_with("log decode: "));
    }

    #[test]
    fn codec_io_error_maps_not_representable_to_other_kind() {
        // An encode-side feature-gate violation is not a foreign-format read, so
        // it must not masquerade as the `InvalidData` reserved for that case.
        let io_err = codec_io_error(
            "snapshot encode",
            CodecError::NotRepresentable { version: 1 },
        );
        assert_ne!(io_err.kind(), io::ErrorKind::InvalidData);
        assert!(io_err.to_string().starts_with("snapshot encode: "));
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
