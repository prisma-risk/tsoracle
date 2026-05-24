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
#![allow(clippy::all)]

pub mod v1 {
    tonic::include_proto!("tsoracle.v1");

    use prost::Message;
    use tonic::Status;
    use tonic::metadata::MetadataKey;

    /// Binary metadata trailer key carrying the [`LeaderHint`] payload on a
    /// `NOT_LEADER` (`FAILED_PRECONDITION`) response.
    ///
    /// This is the single source of truth for the wire contract: the server
    /// inserts the trailer under this key and the client decodes it from the
    /// same key. If the two ever disagreed, the client would silently stop
    /// following hints and degrade to round-robin, so both sides reference
    /// this one constant rather than re-spelling the string.
    pub const LEADER_HINT_TRAILER_KEY: &str = "tsoracle-leader-hint-bin";

    /// Outcome of inspecting a `Status`'s trailers for the leader-hint payload.
    ///
    /// The client's retry loop treats the three cases differently: `Absent` is
    /// the normal "this peer doesn't know who the leader is" signal and stays
    /// silent; `Malformed` is a wire-protocol bug worth a warning + counter;
    /// `Decoded` is the followable redirect. The server's decode path collapses
    /// this to `Option<LeaderHint>` via a thin adapter, but the richer shape is
    /// kept here so that distinction is not lost for callers that need it.
    pub enum LeaderHintLookup {
        Absent,
        Decoded(LeaderHint),
        Malformed,
    }

    /// Decode the [`LeaderHint`] trailer keyed by [`LEADER_HINT_TRAILER_KEY`]
    /// from `status`, classifying the three outcomes (see [`LeaderHintLookup`]).
    pub fn decode_leader_hint(status: &Status) -> LeaderHintLookup {
        let Ok(key) = MetadataKey::from_bytes(LEADER_HINT_TRAILER_KEY.as_bytes()) else {
            return LeaderHintLookup::Absent;
        };
        let Some(value) = status.metadata().get_bin(key) else {
            return LeaderHintLookup::Absent;
        };
        let Ok(bytes) = value.to_bytes() else {
            return LeaderHintLookup::Malformed;
        };
        match LeaderHint::decode(bytes.as_ref()) {
            Ok(hint) => LeaderHintLookup::Decoded(hint),
            Err(_) => LeaderHintLookup::Malformed,
        }
    }
}

/// Encoded `FileDescriptorSet` for every proto compiled by this crate.
/// Feed to `tonic_reflection::server::Builder::register_encoded_file_descriptor_set`
/// to expose gRPC server reflection (`grpcurl`, `evans`, Postman) without
/// shipping the `.proto` files to clients.
#[cfg(feature = "reflection")]
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/tsoracle_descriptor.bin"));

#[cfg(test)]
mod tests {
    use super::v1::*;
    use prost::Message;
    use tonic::Status;
    use tonic::metadata::{BinaryMetadataKey, BinaryMetadataValue};

    #[test]
    fn message_types_exist() {
        let _ = GetTsRequest { count: 1 };
        let _ = GetTsResponse {
            physical_ms: 0,
            logical_start: 0,
            count: 0,
            epoch_hi: 0,
            epoch_lo: 0,
        };
        let _ = LeaderHint {
            leader_endpoint: Some("127.0.0.1:50551".into()),
            leader_epoch: Some(EpochWire { hi: 0, lo: 1 }),
        };
    }

    /// The leader epoch travels as a single nested `EpochWire`, so presence
    /// implies *both* halves: a hint either carries a complete epoch or none
    /// at all. The old two-`optional`-`uint64` shape could encode a corrupt
    /// half-populated epoch; that state is no longer representable.
    #[test]
    fn leader_epoch_round_trips_as_a_single_unit() {
        // Both halves carried — and a non-zero hi guards against a hi/lo swap
        // that an all-low-bits value could not detect.
        let with_epoch = LeaderHint {
            leader_endpoint: Some("127.0.0.1:50551".into()),
            leader_epoch: Some(EpochWire { hi: 1, lo: 3 }),
        };
        let decoded = LeaderHint::decode(with_epoch.encode_to_vec().as_ref()).unwrap();
        assert_eq!(decoded.leader_epoch, Some(EpochWire { hi: 1, lo: 3 }));

        // No epoch at all — the only other representable state.
        let without_epoch = LeaderHint {
            leader_endpoint: Some("127.0.0.1:50551".into()),
            leader_epoch: None,
        };
        let decoded = LeaderHint::decode(without_epoch.encode_to_vec().as_ref()).unwrap();
        assert_eq!(decoded.leader_epoch, None);
    }

    /// A `Status` without a `tsoracle-leader-hint-bin` trailer must decode to
    /// `Absent` — this is the steady-state case (every response other than
    /// NOT_LEADER, plus NOT_LEADER from a server that has no known leader) and
    /// must not surface as `Malformed`, which would cause the client's retry
    /// loop to count it against the wire-protocol-bug bucket.
    #[test]
    fn decode_leader_hint_returns_absent_when_no_trailer_present() {
        let status = Status::failed_precondition("not leader");
        assert!(matches!(
            decode_leader_hint(&status),
            LeaderHintLookup::Absent
        ));
    }

    /// A `Status` with a `tsoracle-leader-hint-bin` trailer whose payload is
    /// not a valid `LeaderHint` protobuf must surface as `Malformed` — the
    /// distinction from `Absent` is what lets the client's retry loop count
    /// wire-protocol bugs separately from "this peer doesn't know the leader."
    #[test]
    fn decode_leader_hint_returns_malformed_on_bad_protobuf() {
        let mut status = Status::failed_precondition("not leader");
        let key = BinaryMetadataKey::from_bytes(LEADER_HINT_TRAILER_KEY.as_bytes())
            .expect("LEADER_HINT_TRAILER_KEY must be a valid binary metadata key");
        // Bytes that are not a valid `LeaderHint` proto. A wire-tag-shaped run
        // of `0xff` makes the decoder enter varint parsing and then fail.
        let value = BinaryMetadataValue::from_bytes(&[0xff, 0xff, 0xff, 0xff]);
        status.metadata_mut().insert_bin(key, value);
        assert!(matches!(
            decode_leader_hint(&status),
            LeaderHintLookup::Malformed
        ));
    }

    /// A well-formed trailer round-trips through `encode` ↔ `decode` and
    /// surfaces as `Decoded(hint)` with the original payload preserved. The
    /// server's `not_leader_status` is the producer; both sides must agree on
    /// the wire shape or NOT_LEADER redirects will silently degrade.
    #[test]
    fn decode_leader_hint_decodes_well_formed_trailer() {
        let mut status = Status::failed_precondition("not leader");
        let key = BinaryMetadataKey::from_bytes(LEADER_HINT_TRAILER_KEY.as_bytes())
            .expect("LEADER_HINT_TRAILER_KEY must be a valid binary metadata key");
        let hint = LeaderHint {
            leader_endpoint: Some("10.0.0.7:50551".into()),
            leader_epoch: Some(EpochWire { hi: 0, lo: 42 }),
        };
        let value = BinaryMetadataValue::from_bytes(&hint.encode_to_vec());
        status.metadata_mut().insert_bin(key, value);

        match decode_leader_hint(&status) {
            LeaderHintLookup::Decoded(decoded) => {
                assert_eq!(decoded.leader_endpoint, hint.leader_endpoint);
                assert_eq!(decoded.leader_epoch, hint.leader_epoch);
            }
            other => panic!(
                "expected Decoded(_), got something else: {}",
                match other {
                    LeaderHintLookup::Absent => "Absent",
                    LeaderHintLookup::Malformed => "Malformed",
                    LeaderHintLookup::Decoded(_) => unreachable!(),
                }
            ),
        }
    }
}
