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

use cg_family_compat::{
    Contracts, FamilyCompat, GrpcSurfaces, ReceiptSchema, SERVICE_CONTRACT_FORMAT,
    ServiceContracts, WireProtocol,
};

pub(crate) fn manifest() -> FamilyCompat {
    FamilyCompat {
        format: SERVICE_CONTRACT_FORMAT,
        service: "tsoracle".into(),
        version: env!("CARGO_PKG_VERSION").into(),
        capability: None,
        frame: None,
        snapshot: None,
        storage_layout: None,
        cggraph_family: None,
        commands: None,
        entries: None,
        lifecycle: None,
        lir: None,
        hir: None,
        view: None,
        config_seed: None,
        contracts: Some(Contracts {
            version_token_bytes: None,
            cggraph_proto_major: None,
            cgcompute_worker_proto_major: None,
            cggraph_grpc: Some(GrpcSurfaces {
                client: None,
                admin: None,
                read: None,
                raft_peer: None,
            }),
            log_codec_fingerprint: None,
            snapshot_codec_fingerprint: None,
            cggraph_read_levels: None,
            cggraph_handshake: None,
        }),
        service_contracts: Some(ServiceContracts {
            wire_protocol: Some(WireProtocol::TsoService { version: 1 }),
            receipt_schema: Some(ReceiptSchema::StoppedFileState { version: 1 }),
        }),
        join_ruleset: 1,
    }
    .sealed()
}
