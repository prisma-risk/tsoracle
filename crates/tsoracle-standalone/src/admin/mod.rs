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

//! Driver-agnostic membership-admin surface. The openraft impl lives in
//! `admin::openraft`; paxos and file use [`UnsupportedAdmin`].

#[cfg(feature = "openraft")]
pub(crate) mod openraft;

#[cfg(feature = "openraft")]
pub(crate) mod service;

use async_trait::async_trait;

/// A node's role in the current membership.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberRole {
    Voter,
    Learner,
}

/// One member as the queried node understands it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberEntry {
    pub id: u64,
    pub role: MemberRole,
    pub raft_addr: String,
    pub service_endpoint: String,
    pub admin_endpoint: String,
}

/// A snapshot of the cluster membership.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MembershipView {
    pub members: Vec<MemberEntry>,
    pub leader: Option<u64>,
}

/// A node to add to the cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewMember {
    pub id: u64,
    pub raft_addr: String,
    pub service_endpoint: String,
    pub admin_endpoint: String,
}

/// Membership-admin failure modes.
#[derive(Debug, thiserror::Error)]
pub enum AdminError {
    /// Mutating op reached a follower; `leader_admin_endpoint` is the leader's
    /// admin port when one is known.
    #[error("not the leader")]
    NotLeader {
        leader_admin_endpoint: Option<String>,
    },
    /// This driver does not support runtime membership changes.
    #[error("membership changes are not supported by this driver")]
    Unsupported,
    /// The referenced node is not a current member.
    #[error("node {0} is not a member")]
    NotMember(u64),
    /// A learner was asked to be promoted but had not caught up.
    #[error("node {0} has not caught up")]
    NotCaughtUp(u64),
    /// The change would drop the cluster below a viable quorum.
    #[error("change would lose quorum")]
    WouldLoseQuorum,
    /// The change did not commit within the deadline.
    #[error("membership change timed out")]
    Timeout,
    /// Wrapped driver error.
    #[error("driver error: {0}")]
    Driver(String),
}

/// Runtime membership administration. One impl per driver.
#[async_trait]
pub trait MembershipAdmin: Send + Sync {
    async fn list_members(&self) -> Result<MembershipView, AdminError>;
    async fn add_learner(&self, member: NewMember) -> Result<(), AdminError>;
    async fn promote(&self, id: u64) -> Result<(), AdminError>;
    async fn remove(&self, id: u64) -> Result<(), AdminError>;
}

/// Admin handle for drivers without runtime membership (file, and — in this
/// sub-project — paxos). `list_members` reports a fixed view supplied at
/// construction; every mutating op is `Unsupported`.
pub struct UnsupportedAdmin {
    view: MembershipView,
}

impl UnsupportedAdmin {
    pub fn new(view: MembershipView) -> Self {
        Self { view }
    }
}

#[async_trait]
impl MembershipAdmin for UnsupportedAdmin {
    async fn list_members(&self) -> Result<MembershipView, AdminError> {
        Ok(self.view.clone())
    }
    async fn add_learner(&self, _member: NewMember) -> Result<(), AdminError> {
        Err(AdminError::Unsupported)
    }
    async fn promote(&self, _id: u64) -> Result<(), AdminError> {
        Err(AdminError::Unsupported)
    }
    async fn remove(&self, _id: u64) -> Result<(), AdminError> {
        Err(AdminError::Unsupported)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_view() -> MembershipView {
        MembershipView {
            members: Vec::new(),
            leader: None,
        }
    }

    fn new_member() -> NewMember {
        NewMember {
            id: 2,
            raft_addr: "127.0.0.1:9".into(),
            service_endpoint: "127.0.0.1:8".into(),
            admin_endpoint: "127.0.0.1:7".into(),
        }
    }

    #[tokio::test]
    async fn unsupported_admin_rejects_every_mutation() {
        let admin = UnsupportedAdmin::new(empty_view());
        assert!(matches!(
            admin.add_learner(new_member()).await,
            Err(AdminError::Unsupported)
        ));
        assert!(matches!(
            admin.promote(2).await,
            Err(AdminError::Unsupported)
        ));
        assert!(matches!(
            admin.remove(2).await,
            Err(AdminError::Unsupported)
        ));
    }

    #[tokio::test]
    async fn unsupported_admin_returns_its_fixed_view() {
        let view = MembershipView {
            members: vec![MemberEntry {
                id: 1,
                role: MemberRole::Voter,
                raft_addr: "a:1".into(),
                service_endpoint: "a:2".into(),
                admin_endpoint: "a:3".into(),
            }],
            leader: Some(1),
        };
        let admin = UnsupportedAdmin::new(view.clone());
        assert_eq!(admin.list_members().await.unwrap(), view);
    }
}
