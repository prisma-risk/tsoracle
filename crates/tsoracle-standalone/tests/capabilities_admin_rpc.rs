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

//! Handler-layer test for `ReportCapabilities`. Drives `AdminServiceImpl`
//! directly with a fake `MembershipAdmin` to pin two contracts: an
//! `Unsupported` trait error becomes gRPC `UNIMPLEMENTED`, and a
//! `CapabilityReport` maps to the proto message with reachable rows carrying
//! the three version fields and unreachable rows carrying the detail.

#![cfg(all(feature = "openraft", feature = "test-support"))]

use std::sync::Arc;

use async_trait::async_trait;
use tonic::{Code, Request};
use tsoracle_standalone::admin::{
    AdminError, CapabilityReport, CapabilityState, MemberCapability, MemberEntry, MemberRole,
    MembershipAdmin, MembershipView, NewMember,
};
use tsoracle_standalone::admin_proto::ReportCapabilitiesRequest;
use tsoracle_standalone::admin_proto::membership_admin_server::MembershipAdmin as GrpcAdmin;

/// Fake admin that returns a preconfigured `report_capabilities` outcome and
/// `Unsupported` for everything else.
struct CapabilityAdmin {
    result: tokio::sync::Mutex<Option<Result<CapabilityReport, AdminError>>>,
}

impl CapabilityAdmin {
    fn new(result: Result<CapabilityReport, AdminError>) -> Self {
        Self {
            result: tokio::sync::Mutex::new(Some(result)),
        }
    }
}

#[async_trait]
impl MembershipAdmin for CapabilityAdmin {
    async fn list_members(&self) -> Result<MembershipView, AdminError> {
        Err(AdminError::Unsupported)
    }

    async fn add_learner(&self, _: NewMember) -> Result<(), AdminError> {
        Err(AdminError::Unsupported)
    }

    async fn promote(&self, _: u64) -> Result<(), AdminError> {
        Err(AdminError::Unsupported)
    }

    async fn remove(&self, _: u64) -> Result<(), AdminError> {
        Err(AdminError::Unsupported)
    }

    async fn activate_format(&self, _: u8) -> Result<(), AdminError> {
        Err(AdminError::Unsupported)
    }

    async fn report_capabilities(&self) -> Result<CapabilityReport, AdminError> {
        self.result
            .lock()
            .await
            .take()
            .expect("report_capabilities called more than once")
    }
}

fn handler(admin: CapabilityAdmin) -> impl GrpcAdmin {
    tsoracle_standalone::admin::test_support::admin_service(Arc::new(admin))
}

fn entry(id: u64) -> MemberEntry {
    MemberEntry {
        id,
        role: MemberRole::Voter,
        raft_addr: format!("h{id}:1"),
        service_endpoint: format!("h{id}:2"),
        admin_endpoint: format!("h{id}:3"),
    }
}

#[tokio::test]
async fn unsupported_maps_to_unimplemented_status() {
    let handler = handler(CapabilityAdmin::new(Err(AdminError::Unsupported)));
    let status = handler
        .report_capabilities(Request::new(ReportCapabilitiesRequest {}))
        .await
        .expect_err("Unsupported must surface as a gRPC Status");
    assert_eq!(status.code(), Code::Unimplemented);
}

#[tokio::test]
async fn reachable_and_unreachable_rows_map_to_proto() {
    let report = CapabilityReport {
        leader: Some(1),
        members: vec![
            MemberCapability {
                member: entry(1),
                caps: CapabilityState::Reported {
                    min_readable: 4,
                    max_readable: 6,
                    active_write: 4,
                },
            },
            MemberCapability {
                member: entry(2),
                caps: CapabilityState::Unreachable {
                    detail: "connection refused".to_string(),
                },
            },
        ],
    };
    let handler = handler(CapabilityAdmin::new(Ok(report)));
    let proto = handler
        .report_capabilities(Request::new(ReportCapabilitiesRequest {}))
        .await
        .expect("ok")
        .into_inner();

    assert!(proto.has_leader);
    assert_eq!(proto.leader, 1);
    assert_eq!(proto.members.len(), 2);

    let reachable = &proto.members[0];
    assert!(reachable.reachable);
    assert_eq!(reachable.min_readable_version, 4);
    assert_eq!(reachable.max_readable_version, 6);
    assert_eq!(reachable.active_write_version, 4);
    assert!(reachable.unreachable_detail.is_empty());

    let unreachable = &proto.members[1];
    assert!(!unreachable.reachable);
    assert_eq!(unreachable.min_readable_version, 0);
    assert_eq!(unreachable.max_readable_version, 0);
    assert_eq!(unreachable.active_write_version, 0);
    assert_eq!(unreachable.unreachable_detail, "connection refused");
}
