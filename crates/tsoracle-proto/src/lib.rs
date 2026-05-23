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

    #[test]
    fn message_types_exist() {
        let _ = GetTsRequest { count: 1 };
        let _ = GetTsResponse {
            physical_ms: 0,
            logical_start: 0,
            count: 0,
            epoch: 0,
        };
        let _ = LeaderHint {
            leader_endpoint: Some("127.0.0.1:50551".into()),
            leader_epoch: Some(1),
        };
    }
}
