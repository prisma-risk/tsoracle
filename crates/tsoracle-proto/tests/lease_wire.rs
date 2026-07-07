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

//! Golden wire vectors for the lease proto contract.

use prost::Message;
use tsoracle_proto::v1::{AcquireLeaseRequest, AcquireLeaseResponse, EpochWire};

#[test]
fn acquire_lease_request_golden_bytes() {
    let req = AcquireLeaseRequest {
        holder: vec![0xAB, 0xCD],
        holder_epoch: 7,
        ttl_ms: 20_000,
    };
    let bytes = req.encode_to_vec();
    assert_eq!(
        bytes,
        vec![
            0x0a, 0x02, 0xab, 0xcd, // field 1 (holder: bytes), len 2
            0x10, 0x07, // field 2 (holder_epoch: varint) = 7
            0x18, 0xa0, 0x9c, 0x01, // field 3 (ttl_ms: varint) = 20000
        ],
    );
    assert_eq!(AcquireLeaseRequest::decode(&bytes[..]).unwrap(), req);
}

#[test]
fn acquire_lease_response_golden_bytes() {
    let resp = AcquireLeaseResponse {
        lease_id: 5,
        ts_upper_bound: 1_000,
        expires_at_ms: 2_000,
        epoch: Some(EpochWire { hi: 0, lo: 9 }),
    };
    let bytes = resp.encode_to_vec();
    assert_eq!(
        bytes,
        vec![
            0x08, 0x05, // field 1 (lease_id) = 5
            0x10, 0xe8, 0x07, // field 2 (ts_upper_bound) = 1000
            0x18, 0xd0, 0x0f, // field 3 (expires_at_ms) = 2000
            0x22, 0x02, 0x10, 0x09, // field 4 (epoch: EpochWire{lo:9})
        ],
    );
    assert_eq!(AcquireLeaseResponse::decode(&bytes[..]).unwrap(), resp);
}
