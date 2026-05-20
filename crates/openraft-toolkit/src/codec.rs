//! Binary wire codec for openraft RPC payloads and storage records.
//!
//! Every payload is encoded as `[version_byte | bincode(value)]`. The leading
//! byte lets us evolve the wire format without an explicit migration when both
//! sides of an upgrade run mixed versions briefly.

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CodecError {
    #[error("payload empty")]
    Empty,
    #[error("version mismatch: expected {expected}, got {actual}")]
    Version { expected: u8, actual: u8 },
    #[error("decode failed: {0}")]
    Decode(#[from] bincode::Error),
}

/// Encode `value` as `[version | bincode(value)]`.
pub fn encode<T: Serialize>(version: u8, value: &T) -> Result<Vec<u8>, CodecError> {
    let body = bincode::serialize(value)?;
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
    Ok(bincode::deserialize(rest)?)
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
}
