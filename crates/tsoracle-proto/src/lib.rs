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
}
