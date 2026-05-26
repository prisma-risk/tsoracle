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

//! Handler-layer mapping test for `ActivateFormat`. Drives `AdminServiceImpl`
//! directly with a fake `MembershipAdmin` that returns each `AdminError`
//! variant in turn, asserting the resulting `ChangeResponse` carries the
//! correct `AdminErrorKind`. Complements the trait-level mapping tests in
//! `src/admin/openraft.rs` (`FormatActivationError -> AdminError`); this
//! file pins the next hop (`AdminError -> AdminErrorKind` over the wire).

#![cfg(all(feature = "openraft", feature = "test-support"))]

use std::sync::Arc;

use async_trait::async_trait;
use tonic::Request;
use tsoracle_standalone::admin::{AdminError, MembershipAdmin, MembershipView, NewMember};
use tsoracle_standalone::admin_proto::membership_admin_server::MembershipAdmin as GrpcAdmin;
use tsoracle_standalone::admin_proto::{ActivateFormatRequest, AdminErrorKind};

/// Fake admin returning a one-shot configured outcome for `activate_format`.
/// Other membership ops are unused by these tests — they return Unsupported.
struct ProgrammableAdmin {
    result: tokio::sync::Mutex<Option<Result<(), AdminError>>>,
}

impl ProgrammableAdmin {
    fn new(result: Result<(), AdminError>) -> Self {
        Self {
            result: tokio::sync::Mutex::new(Some(result)),
        }
    }
}

#[async_trait]
impl MembershipAdmin for ProgrammableAdmin {
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
    async fn activate_format(&self, _target: u8) -> Result<(), AdminError> {
        self.result
            .lock()
            .await
            .take()
            .expect("activate_format called more than once")
    }
}

fn handler(admin: ProgrammableAdmin) -> impl GrpcAdmin {
    tsoracle_standalone::admin::test_support::admin_service(Arc::new(admin))
}

async fn call_activate(
    handler: &impl GrpcAdmin,
    target: u32,
) -> tsoracle_standalone::admin_proto::ChangeResponse {
    handler
        .activate_format(Request::new(ActivateFormatRequest { target }))
        .await
        .expect("handler returned Status err on a value-domain mapping path")
        .into_inner()
}

#[tokio::test]
async fn ok_maps_to_ok() {
    let h = handler(ProgrammableAdmin::new(Ok(())));
    let resp = call_activate(&h, 5).await;
    assert!(resp.ok);
    assert_eq!(resp.error, AdminErrorKind::Unspecified as i32);
}

#[tokio::test]
async fn members_below_target_maps_to_dedicated_kind() {
    let h = handler(ProgrammableAdmin::new(Err(
        AdminError::MembersBelowTarget {
            target: 5,
            incapable: vec![(1, 4), (3, 4)],
        },
    )));
    let resp = call_activate(&h, 5).await;
    assert!(!resp.ok);
    assert_eq!(resp.error, AdminErrorKind::MembersBelowTarget as i32);
    assert!(resp.message.contains("members below target"));
}

#[tokio::test]
async fn target_out_of_range_maps_to_dedicated_kind() {
    let h = handler(ProgrammableAdmin::new(Err(AdminError::TargetOutOfRange {
        target: 99,
        min: 4,
        max: 5,
    })));
    let resp = call_activate(&h, 99).await;
    assert!(!resp.ok);
    assert_eq!(resp.error, AdminErrorKind::TargetOutOfRange as i32);
}

#[tokio::test]
async fn membership_changed_maps_to_dedicated_kind() {
    let h = handler(ProgrammableAdmin::new(Err(
        AdminError::MembershipChangedSinceGate { target: 5 },
    )));
    let resp = call_activate(&h, 5).await;
    assert!(!resp.ok);
    assert_eq!(resp.error, AdminErrorKind::MembershipChanged as i32);
}

#[tokio::test]
async fn not_leader_maps_to_not_leader_without_endpoint() {
    let h = handler(ProgrammableAdmin::new(Err(AdminError::NotLeader {
        leader_admin_endpoint: None,
    })));
    let resp = call_activate(&h, 5).await;
    assert!(!resp.ok);
    assert_eq!(resp.error, AdminErrorKind::NotLeader as i32);
    // Activation NotLeader never carries an endpoint
    // (`FormatActivationError::NotLeader` is a unit variant — see
    // `admin/openraft.rs` map_activation_error). The shell relies on this.
    assert!(resp.leader_admin_endpoint.is_empty());
}

#[tokio::test]
async fn target_out_of_u8_range_rejected_with_invalid_argument() {
    // The handler should reject before ever calling the admin trait. Park an
    // Unsupported in the slot purely as a safe fallback for the take() guard
    // — if the handler regressed and called through, it would return
    // Ok(Response { error: Unsupported }) and expect_err() below would panic
    // immediately, making the regression unambiguous.
    let h = handler(ProgrammableAdmin::new(Err(AdminError::Unsupported)));
    let err = h
        .activate_format(Request::new(ActivateFormatRequest { target: 300 }))
        .await
        .expect_err("expected Status err for out-of-u8-range target");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}
