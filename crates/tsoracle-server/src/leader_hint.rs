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

//! Encoding and decoding the `tsoracle-leader-hint-bin` binary metadata trailer.

use prost::Message;
use tonic::Status;
use tonic::metadata::{BinaryMetadataKey, BinaryMetadataValue};
use tsoracle_proto::v1::{LEADER_HINT_TRAILER_KEY, LeaderHint, LeaderHintLookup};

pub fn not_leader_status(reporter: &crate::reporter::Reporter, hint: LeaderHint) -> Status {
    // Single chokepoint: every NOT_LEADER rejection in the service layer
    // routes through this constructor, so the counter increments exactly
    // once per rejection regardless of which call site detected it.
    reporter.not_leader.increment(1);
    with_leader_hint(
        Status::failed_precondition("not leader"),
        hint,
        LEADER_HINT_TRAILER_KEY,
    )
}

/// Inserts a [`LeaderHint`] trailer keyed by `key_str` and returns `status`.
///
/// If `key_str` is not a valid binary metadata key, logs at `error!` and
/// returns `status` without the trailer. The client's retry path tolerates a
/// missing trailer (it round-robins over configured endpoints instead of
/// jumping directly to the hinted leader), so a corrupted key degrades
/// latency rather than correctness. [`LEADER_HINT_TRAILER_KEY`] is a
/// `const &'static str`, so this fallback only ever runs in a worst-case
/// developer-error scenario; the `validate_key_accepts_real_key` unit test
/// proves the real key parses on every build.
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

/// Server-side `Option` view over the shared
/// [`tsoracle_proto::v1::decode_leader_hint`] classifier: both "no trailer"
/// (`Absent`) and "garbage trailer" (`Malformed`) collapse to `None`, since the
/// server's only decode caller is its own test surface, which has no use for
/// the wire-protocol-bug distinction the client tracks.
pub fn decode_leader_hint(status: &Status) -> Option<LeaderHint> {
    match tsoracle_proto::v1::decode_leader_hint(status) {
        LeaderHintLookup::Decoded(hint) => Some(hint),
        LeaderHintLookup::Absent | LeaderHintLookup::Malformed => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tsoracle_proto::v1::EpochWire;

    #[test]
    fn roundtrip() {
        let hint = LeaderHint {
            leader_endpoint: Some("10.0.0.7:50551".into()),
            leader_epoch: Some(EpochWire { hi: 0, lo: 42 }),
        };
        let status = not_leader_status(&crate::reporter::Reporter::for_tests(), hint.clone());
        let decoded = decode_leader_hint(&status).expect("present");
        assert_eq!(decoded.leader_endpoint, hint.leader_endpoint);
        assert_eq!(decoded.leader_epoch, hint.leader_epoch);
    }

    #[test]
    fn validate_key_accepts_real_key() {
        // The build-time guard that LEADER_HINT_TRAILER_KEY is a valid binary
        // metadata key. If a future edit breaks the const, this test fails
        // rather than letting `with_leader_hint` silently drop the trailer.
        BinaryMetadataKey::from_bytes(LEADER_HINT_TRAILER_KEY.as_bytes())
            .expect("LEADER_HINT_TRAILER_KEY is a valid binary metadata key");
    }

    #[test]
    fn invalid_key_omits_trailer_but_preserves_status() {
        let hint = LeaderHint {
            leader_endpoint: Some("10.0.0.7:50551".into()),
            leader_epoch: Some(EpochWire { hi: 0, lo: 42 }),
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
