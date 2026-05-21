//! Encoding and decoding the `tsoracle-leader-hint-bin` binary metadata trailer.

use prost::Message;
use tonic::Status;
use tonic::metadata::{BinaryMetadataValue, MetadataKey};
use tsoracle_proto::v1::LeaderHint;

pub const KEY: &str = "tsoracle-leader-hint-bin";

pub fn not_leader_status(hint: LeaderHint) -> Status {
    let mut status = Status::failed_precondition("not leader");
    let bytes = hint.encode_to_vec();
    let value = BinaryMetadataValue::from_bytes(&bytes);
    #[expect(
        clippy::expect_used,
        reason = "`KEY` is a `const &'static str` of valid ASCII; `MetadataKey::from_bytes` cannot fail here. Tracked by #5."
    )]
    let key = MetadataKey::from_bytes(KEY.as_bytes()).expect("valid key");
    status.metadata_mut().insert_bin(key, value);
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
}
