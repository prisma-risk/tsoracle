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

//! gRPC `AdminService`: adapts `Arc<dyn MembershipAdmin>` onto the generated
//! `tsoracle.admin.v1` server, mapping `AdminError` to `ChangeResponse`.

use std::sync::Arc;

use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};

use crate::admin::{AdminError, MembershipAdmin, NewMember};
use crate::admin_proto::membership_admin_server::{
    MembershipAdmin as GrpcAdmin, MembershipAdminServer,
};
use crate::admin_proto::{
    ActivateFormatRequest, AddLearnerRequest, AdminErrorKind, CapabilityReport, ChangeResponse,
    ListMembersRequest, MemberCapabilities, MemberEntry, MemberRole, MembershipView,
    PromoteRequest, RemoveNodeRequest, ReportCapabilitiesRequest,
};

/// Admin RPCs carry only ids and host:port strings, so a tight decode/encode
/// cap is well above any legitimate message and bounds what an unauthenticated
/// client on the admin port can make the server buffer (tonic defaults to 4
/// MiB). Mirrors the peer server's explicit message-size hardening.
const MAX_ADMIN_MESSAGE_BYTES: usize = 64 * 1024;

pub(crate) struct AdminServiceImpl {
    admin: Arc<dyn MembershipAdmin>,
}

impl AdminServiceImpl {
    pub(crate) fn new(admin: Arc<dyn MembershipAdmin>) -> Self {
        Self { admin }
    }
}

fn change_ok() -> ChangeResponse {
    ChangeResponse {
        ok: true,
        error: AdminErrorKind::Unspecified as i32,
        leader_admin_endpoint: String::new(),
        message: String::new(),
    }
}

fn change_err(err: AdminError) -> ChangeResponse {
    let (kind, leader, message) = match err {
        AdminError::NotLeader {
            leader_admin_endpoint,
        } => (
            AdminErrorKind::NotLeader,
            leader_admin_endpoint.unwrap_or_default(),
            "not the leader".to_string(),
        ),
        AdminError::Unsupported => (
            AdminErrorKind::Unsupported,
            String::new(),
            "unsupported".into(),
        ),
        AdminError::NotMember(id) => (
            AdminErrorKind::NotMember,
            String::new(),
            format!("node {id} not a member"),
        ),
        AdminError::NotCaughtUp(id) => (
            AdminErrorKind::NotCaughtUp,
            String::new(),
            format!("node {id} not caught up"),
        ),
        AdminError::WouldLoseQuorum => (
            AdminErrorKind::WouldLoseQuorum,
            String::new(),
            "would lose quorum".into(),
        ),
        AdminError::Timeout => (AdminErrorKind::Timeout, String::new(), "timed out".into()),
        AdminError::MembersBelowTarget { target, incapable } => (
            AdminErrorKind::MembersBelowTarget,
            String::new(),
            format!(
                "format activation to target {target} blocked: members below target: {incapable:?}"
            ),
        ),
        AdminError::TargetOutOfRange { target, min, max } => (
            AdminErrorKind::TargetOutOfRange,
            String::new(),
            format!("format activation: target {target} outside readable range [{min}, {max}]"),
        ),
        AdminError::MembershipChangedSinceGate { target } => (
            AdminErrorKind::MembershipChanged,
            String::new(),
            format!("format activation to target {target} no-op: membership changed since gate"),
        ),
        AdminError::Driver(detail) => (AdminErrorKind::Driver, String::new(), detail),
    };
    ChangeResponse {
        ok: false,
        error: kind as i32,
        leader_admin_endpoint: leader,
        message,
    }
}

fn change_response(result: Result<(), AdminError>) -> ChangeResponse {
    match result {
        Ok(()) => change_ok(),
        Err(err) => change_err(err),
    }
}

#[tonic::async_trait]
impl GrpcAdmin for AdminServiceImpl {
    async fn list_members(
        &self,
        _req: Request<ListMembersRequest>,
    ) -> Result<Response<MembershipView>, Status> {
        let view = self
            .admin
            .list_members()
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        Ok(Response::new(MembershipView {
            members: view
                .members
                .into_iter()
                .map(|member| MemberEntry {
                    id: member.id,
                    role: match member.role {
                        crate::admin::MemberRole::Voter => MemberRole::Voter as i32,
                        crate::admin::MemberRole::Learner => MemberRole::Learner as i32,
                    },
                    raft_addr: member.raft_addr,
                    service_endpoint: member.service_endpoint,
                    admin_endpoint: member.admin_endpoint,
                })
                .collect(),
            has_leader: view.leader.is_some(),
            leader: view.leader.unwrap_or_default(),
        }))
    }

    async fn add_learner(
        &self,
        req: Request<AddLearnerRequest>,
    ) -> Result<Response<ChangeResponse>, Status> {
        let request = req.into_inner();
        let result = self
            .admin
            .add_learner(NewMember {
                id: request.id,
                raft_addr: request.raft_addr,
                service_endpoint: request.service_endpoint,
                admin_endpoint: request.admin_endpoint,
            })
            .await;
        Ok(Response::new(change_response(result)))
    }

    async fn promote(
        &self,
        req: Request<PromoteRequest>,
    ) -> Result<Response<ChangeResponse>, Status> {
        Ok(Response::new(change_response(
            self.admin.promote(req.into_inner().id).await,
        )))
    }

    async fn remove_node(
        &self,
        req: Request<RemoveNodeRequest>,
    ) -> Result<Response<ChangeResponse>, Status> {
        Ok(Response::new(change_response(
            self.admin.remove(req.into_inner().id).await,
        )))
    }

    async fn activate_format(
        &self,
        req: Request<ActivateFormatRequest>,
    ) -> Result<Response<ChangeResponse>, Status> {
        let target_u32 = req.into_inner().target;
        // proto3 has no uint8; validate that the value fits u8 before
        // forwarding to the driver layer. Out-of-u8-range is an obvious
        // client bug — bail with INVALID_ARGUMENT rather than passing a
        // truncated value into the activation gate.
        let Ok(target) = u8::try_from(target_u32) else {
            return Err(Status::invalid_argument(format!(
                "target {target_u32} does not fit in u8 (0..=255)"
            )));
        };
        Ok(Response::new(change_response(
            self.admin.activate_format(target).await,
        )))
    }

    async fn report_capabilities(
        &self,
        _req: Request<ReportCapabilitiesRequest>,
    ) -> Result<Response<CapabilityReport>, Status> {
        let report = self
            .admin
            .report_capabilities()
            .await
            .map_err(|err| match err {
                AdminError::Unsupported => {
                    Status::unimplemented("format capabilities are not supported by this driver")
                }
                other => Status::internal(other.to_string()),
            })?;
        let members = report
            .members
            .into_iter()
            .map(|entry| {
                let (reachable, min_readable, max_readable, active_write, detail) = match entry.caps
                {
                    crate::admin::CapabilityState::Reported {
                        min_readable,
                        max_readable,
                        active_write,
                    } => (
                        true,
                        u32::from(min_readable),
                        u32::from(max_readable),
                        u32::from(active_write),
                        String::new(),
                    ),
                    crate::admin::CapabilityState::Unreachable { detail } => {
                        (false, 0, 0, 0, detail)
                    }
                };
                MemberCapabilities {
                    id: entry.member.id,
                    role: match entry.member.role {
                        crate::admin::MemberRole::Voter => MemberRole::Voter as i32,
                        crate::admin::MemberRole::Learner => MemberRole::Learner as i32,
                    },
                    raft_addr: entry.member.raft_addr,
                    service_endpoint: entry.member.service_endpoint,
                    admin_endpoint: entry.member.admin_endpoint,
                    reachable,
                    min_readable_version: min_readable,
                    max_readable_version: max_readable,
                    active_write_version: active_write,
                    unreachable_detail: detail,
                }
            })
            .collect();
        Ok(Response::new(CapabilityReport {
            members,
            has_leader: report.leader.is_some(),
            leader: report.leader.unwrap_or_default(),
        }))
    }
}

/// Spawn the admin gRPC server on `listener` under transport supervision.
/// The caller is responsible for the `TcpListener::bind` (the openraft driver
/// build path does this immediately before calling us). Keeping the bind in
/// the caller lets test-only entry points hand in a `TcpListener` that has
/// been held continuously since `lease_port()` — eliminating the close/rebind
/// race window where another `bind(:0)` could snatch the freshly-freed
/// ephemeral port. When `admin_tls` is `Some`, the server requires a client
/// certificate signed by the configured admin CA (mTLS). An unexpected exit
/// of the spawned server trips `fatal` so the node fails fast instead of
/// running on without its admin surface. Returns the supervised transport
/// handle plus the actual bound `SocketAddr` (resolves :0 to the OS-picked
/// port) for caller-side observability — stored on
/// `Standalone::admin_listen_addr`.
pub(crate) async fn serve_admin(
    admin: Arc<dyn MembershipAdmin>,
    listener: tokio::net::TcpListener,
    admin_tls: Option<crate::admin_tls::AdminTlsMaterial>,
    fatal: crate::FatalSignal,
) -> Result<(crate::TransportHandle, std::net::SocketAddr), std::io::Error> {
    let bound = listener.local_addr()?;
    let service = MembershipAdminServer::new(AdminServiceImpl::new(admin))
        .max_decoding_message_size(MAX_ADMIN_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_ADMIN_MESSAGE_BYTES);
    let mut builder = tonic::transport::Server::builder();
    if let Some(material) = admin_tls {
        // tls_config() on a pre-validated ServerTlsConfig cannot fail in
        // practice (admin_tls.rs already dry-ran the build), but tonic's
        // signature returns Result so we surface any residual error as an
        // io::Error rather than panic.
        builder = builder.tls_config(material.server).map_err(|err| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, err.to_string())
        })?;
    }
    let handle = crate::TransportHandle::spawn_supervised("admin server", fatal, move |shutdown| {
        builder
            .add_service(service)
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown)
    });
    Ok((handle, bound))
}
