//! Membership-change wrappers.
//!
//! Coverage note: excluded from `make coverage` because exercising these
//! wrappers requires a live raft, which the toolkit's own tests deliberately
//! don't stand up. Downstream consumers' integration tests carry the real
//! coverage; the compile-time signature shims in `tests/lifecycle.rs` are what
//! catch openraft API drift inside this file.
//!
//! These thin helpers translate openraft's deeply-nested error enums into a
//! `MembershipError` consumers can match on without pulling in openraft's
//! internal error modules.
//!
//! Both wrappers map the three observable outcomes from
//! `Raft::change_membership` / `Raft::add_learner` in alpha.20:
//!
//! - `ForwardToLeader` -> [`MembershipError::NotLeader`] with a stringified
//!   leader-node hint (consumers wanting structured redirect can build it on
//!   top; the toolkit stays agnostic to `C::Node`'s concrete shape).
//! - `ChangeMembershipError` -> [`MembershipError::Conflict`] (joint-config
//!   in progress, learner-not-found, etc.).
//! - Anything else (including `RaftError::Fatal`) -> [`MembershipError::Other`].

use std::collections::BTreeSet;

use openraft::error::{ClientWriteError, RaftError};
use openraft::storage::RaftStateMachine;
use openraft::{Raft, RaftTypeConfig};
use thiserror::Error;
use tracing::info;

/// Failure modes for [`change_membership`] / [`add_learner`].
#[derive(Debug, Error)]
pub enum MembershipError {
    /// The contacted node is not the leader. `leader` is a best-effort
    /// stringified hint from openraft's `ForwardToLeader.leader_node`; it is
    /// `None` if openraft did not yet know a leader.
    #[error("not leader; redirect to {leader:?}")]
    NotLeader { leader: Option<String> },
    /// openraft rejected the membership change itself (joint-config in
    /// progress, learner not found, empty voter set, etc.). The wrapped
    /// string is the rendered `ChangeMembershipError`.
    #[error("membership change conflict: {0}")]
    Conflict(String),
    /// Any other failure (fatal raft error, snapshot/log failure, etc.).
    #[error("raft error: {0}")]
    Other(String),
}

/// Submit a voter-set change to the raft leader.
///
/// `voters` is the desired voter set; `retain` controls whether removed
/// voters are demoted to learners (`true`) or dropped (`false`). See
/// openraft's `Raft::change_membership` docs for the joint-config semantics.
pub async fn change_membership<C, SM>(
    raft: &Raft<C, SM>,
    voters: BTreeSet<C::NodeId>,
    retain: bool,
) -> Result<(), MembershipError>
where
    C: RaftTypeConfig,
    SM: RaftStateMachine<C>,
{
    match raft.change_membership(voters, retain).await {
        Ok(_) => {
            info!("openraft-toolkit: membership change applied");
            Ok(())
        }
        Err(RaftError::APIError(ClientWriteError::ChangeMembershipError(e))) => {
            Err(MembershipError::Conflict(e.to_string()))
        }
        Err(RaftError::APIError(ClientWriteError::ForwardToLeader(f))) => {
            Err(MembershipError::NotLeader {
                leader: f.leader_node.as_ref().map(|n| format!("{n:?}")),
            })
        }
        Err(e) => Err(MembershipError::Other(e.to_string())),
    }
}

/// Add a learner peer to the raft.
///
/// `blocking` mirrors openraft's flag: when `true`, the call waits for the
/// learner to catch up before returning; when `false`, it returns once the
/// `AddLearner` log entry is committed.
pub async fn add_learner<C, SM>(
    raft: &Raft<C, SM>,
    id: C::NodeId,
    node: C::Node,
    blocking: bool,
) -> Result<(), MembershipError>
where
    C: RaftTypeConfig,
    SM: RaftStateMachine<C>,
{
    match raft.add_learner(id, node, blocking).await {
        Ok(_) => {
            info!("openraft-toolkit: learner added");
            Ok(())
        }
        Err(RaftError::APIError(ClientWriteError::ForwardToLeader(f))) => {
            Err(MembershipError::NotLeader {
                leader: f.leader_node.as_ref().map(|n| format!("{n:?}")),
            })
        }
        Err(RaftError::APIError(ClientWriteError::ChangeMembershipError(e))) => {
            Err(MembershipError::Conflict(e.to_string()))
        }
        Err(e) => Err(MembershipError::Other(e.to_string())),
    }
}
