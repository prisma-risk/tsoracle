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

//! gRPC `AdminService`: adapts `Arc<dyn MembershipAdmin>` onto the generated
//! `tsoracle.admin.v1` server, mapping `AdminError` to `ChangeResponse`.

use std::sync::Arc;

use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};

use crate::admin::{AdminError, MembershipAdmin, NewMember};
use crate::admin_proto::membership_admin_server::{
    MembershipAdmin as GrpcAdmin, MembershipAdminServer,
};
use crate::admin_proto::{
    AddLearnerRequest, AdminErrorKind, ChangeResponse, ListMembersRequest, MemberEntry, MemberRole,
    MembershipView, PromoteRequest, RemoveNodeRequest,
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
}

/// Bind `listen` and spawn the admin gRPC server under a cancel token. Mirrors
/// the peer-server pattern in `drivers/openraft/mod.rs` (bind before spawn so a
/// bind failure surfaces to the caller).
pub(crate) async fn serve_admin(
    admin: Arc<dyn MembershipAdmin>,
    listen: std::net::SocketAddr,
) -> Result<(oneshot::Sender<()>, JoinHandle<()>), std::io::Error> {
    let listener = tokio::net::TcpListener::bind(listen).await?;
    let service = MembershipAdminServer::new(AdminServiceImpl::new(admin))
        .max_decoding_message_size(MAX_ADMIN_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_ADMIN_MESSAGE_BYTES);
    let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
    let join = tokio::spawn(async move {
        let shutdown = async {
            let _ = cancel_rx.await;
        };
        if let Err(err) = tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown)
            .await
        {
            tracing::error!(error = ?err, "admin server died");
        }
    });
    Ok((cancel_tx, join))
}
