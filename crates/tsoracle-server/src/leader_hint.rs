//! Encoding and decoding the `tsoracle-leader-hint-bin` binary metadata trailer.

use prost::Message;
use tonic::Status;
use tonic::metadata::errors::InvalidMetadataKey;
use tonic::metadata::{BinaryMetadataKey, BinaryMetadataValue, MetadataKey};
use tsoracle_proto::v1::LeaderHint;

pub const KEY: &str = "tsoracle-leader-hint-bin";

/// Confirms [`KEY`] is a valid binary metadata key.
///
/// Called from [`ServerBuilder::build`](crate::server::ServerBuilder::build)
/// so an invalid `KEY` (only reachable via developer error) surfaces as a
/// startup failure rather than as a silent runtime degradation in which the
/// `tsoracle-leader-hint-bin` trailer is dropped from every NOT_LEADER
/// response.
pub fn validate_key() -> Result<(), InvalidMetadataKey> {
    BinaryMetadataKey::from_bytes(KEY.as_bytes()).map(|_| ())
}

pub fn not_leader_status(hint: LeaderHint) -> Status {
    with_leader_hint(Status::failed_precondition("not leader"), hint, KEY)
}

/// Inserts a [`LeaderHint`] trailer keyed by `key_str` and returns `status`.
///
/// If `key_str` is not a valid binary metadata key, logs at `error!` and
/// returns `status` without the trailer. The client's retry path tolerates a
/// missing trailer (it round-robins over configured endpoints instead of
/// jumping directly to the hinted leader), so a corrupted key degrades
/// latency rather than correctness. [`validate_key`] additionally gates this
/// at startup so the no-trailer fallback only ever runs in a worst-case
/// developer-error scenario.
fn with_leader_hint(mut status: Status, hint: LeaderHint, key_str: &str) -> Status {
    match BinaryMetadataKey::from_bytes(key_str.as_bytes()) {
        Ok(key) => {
            let bytes = hint.encode_to_vec();
            let value = BinaryMetadataValue::from_bytes(&bytes);
            status.metadata_mut().insert_bin(key, value);
        }
        Err(_error) => {
            #[cfg(feature = "tracing")]
            tracing::error!(
                key = key_str,
                error = %_error,
                "leader-hint metadata key invalid; omitting trailer from NOT_LEADER response"
            );
        }
    }
    status
}

pub fn decode_leader_hint(status: &Status) -> Option<LeaderHint> {
    let key = MetadataKey::from_bytes(KEY.as_bytes()).ok()?;
    let value = status.metadata().get_bin(key)?;
    let bytes = value.to_bytes().ok()?;
    LeaderHint::decode(bytes.as_ref()).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let hint = LeaderHint {
            leader_endpoint: Some("10.0.0.7:50551".into()),
            leader_epoch: Some(42),
        };
        let status = not_leader_status(hint.clone());
        let decoded = decode_leader_hint(&status).expect("present");
        assert_eq!(decoded.leader_endpoint, hint.leader_endpoint);
        assert_eq!(decoded.leader_epoch, hint.leader_epoch);
    }

    #[test]
    fn validate_key_accepts_real_key() {
        validate_key().expect("KEY is a valid binary metadata key");
    }

    #[test]
    fn invalid_key_omits_trailer_but_preserves_status() {
        let hint = LeaderHint {
            leader_endpoint: Some("10.0.0.7:50551".into()),
            leader_epoch: Some(42),
        };
        // Spaces and uppercase make this key invalid for HTTP header
        // construction; it also lacks the required `-bin` suffix.
        let status = with_leader_hint(
            Status::failed_precondition("not leader"),
            hint,
            "Invalid Key",
        );
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert!(decode_leader_hint(&status).is_none());
    }
}
