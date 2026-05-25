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

//! openraft-backed [`MembershipAdmin`].

use std::collections::BTreeSet;

use async_trait::async_trait;
use openraft::Raft;
use openraft::async_runtime::watch::WatchReceiver;
use openraft::error::{ChangeMembershipError, ClientWriteError, ForwardToLeader, RaftError};
use tokio::sync::Mutex;
use tsoracle_driver_openraft::{HighWaterStateMachine, OpenraftPeer, TypeConfig};

use crate::admin::{
    AdminError, MemberEntry, MemberRole, MembershipAdmin, MembershipView, NewMember,
};

/// Map a `change_membership` / `add_learner` error into an `AdminError`,
/// pulling the leader's admin endpoint out of a `ForwardToLeader`.
fn map_write_error(err: RaftError<TypeConfig, ClientWriteError<TypeConfig>>) -> AdminError {
    match err {
        RaftError::APIError(ClientWriteError::ForwardToLeader(ForwardToLeader {
            leader_node,
            ..
        })) => AdminError::NotLeader {
            leader_admin_endpoint: leader_node
                .map(|node| node.admin_endpoint)
                .filter(|endpoint| !endpoint.is_empty()),
        },
        // A promote whose target stopped being a learner between our metrics
        // snapshot and the log apply (e.g. concurrently removed) — surface the
        // typed NotMember rather than an opaque driver string.
        RaftError::APIError(ClientWriteError::ChangeMembershipError(
            ChangeMembershipError::LearnerNotFound(learner),
        )) => AdminError::NotMember(learner.node_id),
        other => AdminError::Driver(other.to_string()),
    }
}

/// The voter set after adding `id`.
fn voters_with(current: &BTreeSet<u64>, id: u64) -> BTreeSet<u64> {
    let mut next = current.clone();
    next.insert(id);
    next
}

/// The voter set after removing `id`.
fn voters_without(current: &BTreeSet<u64>, id: u64) -> BTreeSet<u64> {
    let mut next = current.clone();
    next.remove(&id);
    next
}

/// The voter ids of a membership view. Derived from an already-read view so a
/// mutating op reads `raft.metrics()` once, under its `op_lock`.
fn voter_ids(view: &MembershipView) -> BTreeSet<u64> {
    view.members
        .iter()
        .filter(|entry| entry.role == MemberRole::Voter)
        .map(|entry| entry.id)
        .collect()
}

/// openraft-backed membership admin. Holds a clone of the `Raft` handle and a
/// mutex that serializes mutating ops so two reconfigurations cannot race.
pub(crate) struct OpenraftMembershipAdmin {
    raft: Raft<TypeConfig, HighWaterStateMachine>,
    op_lock: Mutex<()>,
}

impl OpenraftMembershipAdmin {
    pub(crate) fn new(raft: Raft<TypeConfig, HighWaterStateMachine>) -> Self {
        Self {
            raft,
            op_lock: Mutex::new(()),
        }
    }

    /// Build a `MembershipView` from the current raft metrics. Mirrors the
    /// metrics access in `handoff.rs`: `borrow_watched()` then `voter_ids()` /
    /// `nodes()` called on `membership_config`.
    fn view(&self) -> MembershipView {
        let metrics = self.raft.metrics().borrow_watched().clone();
        let voters: BTreeSet<u64> = metrics.membership_config.voter_ids().collect();
        let members = metrics
            .membership_config
            .nodes()
            .map(|(id, node)| MemberEntry {
                id: *id,
                role: if voters.contains(id) {
                    MemberRole::Voter
                } else {
                    MemberRole::Learner
                },
                raft_addr: node.addr.clone(),
                service_endpoint: node.service_endpoint.clone(),
                admin_endpoint: node.admin_endpoint.clone(),
            })
            .collect();
        MembershipView {
            members,
            leader: metrics.current_leader,
        }
    }
}

#[async_trait]
impl MembershipAdmin for OpenraftMembershipAdmin {
    async fn list_members(&self) -> Result<MembershipView, AdminError> {
        Ok(self.view())
    }

    async fn add_learner(&self, member: NewMember) -> Result<(), AdminError> {
        let _guard = self.op_lock.lock().await;
        // Idempotent: a node already in the membership is a no-op.
        if self
            .view()
            .members
            .iter()
            .any(|entry| entry.id == member.id)
        {
            return Ok(());
        }
        let node = OpenraftPeer {
            addr: member.raft_addr,
            service_endpoint: member.service_endpoint,
            admin_endpoint: member.admin_endpoint,
        };
        self.raft
            .add_learner(member.id, node, true)
            .await
            .map(|_| ())
            .map_err(map_write_error)
    }

    async fn promote(&self, id: u64) -> Result<(), AdminError> {
        let _guard = self.op_lock.lock().await;
        let view = self.view();
        match view.members.iter().find(|entry| entry.id == id) {
            None => return Err(AdminError::NotMember(id)),
            Some(entry) if entry.role == MemberRole::Voter => return Ok(()), // idempotent
            _ => {}
        }
        let next = voters_with(&voter_ids(&view), id);
        self.raft
            .change_membership(next, false)
            .await
            .map(|_| ())
            .map_err(map_write_error)
    }

    async fn remove(&self, id: u64) -> Result<(), AdminError> {
        let _guard = self.op_lock.lock().await;
        let view = self.view();
        // Idempotent: removing a non-member is a no-op.
        if !view.members.iter().any(|entry| entry.id == id) {
            return Ok(());
        }
        let voters = voter_ids(&view);
        // Quorum guard: never remove the last voter.
        if voters.contains(&id) && voters.len() <= 1 {
            return Err(AdminError::WouldLoseQuorum);
        }
        // Do NOT pre-transfer leadership when removing the current leader: the
        // admin op runs ON the leader, so handing off first would make this very
        // node a follower and the local change_membership below would then fail
        // with NotLeader. openraft commits the removal WHILE still leader and
        // steps down afterward; a new leader is elected among the remaining
        // voters and the client follows the leader hint.
        let next = voters_without(&voters, id);
        self.raft
            .change_membership(next, false)
            .await
            .map(|_| ())
            .map_err(map_write_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn voters_with_adds_the_id() {
        let current = BTreeSet::from([1, 2, 3]);
        assert_eq!(voters_with(&current, 4), BTreeSet::from([1, 2, 3, 4]));
    }

    #[test]
    fn voters_without_removes_the_id() {
        let current = BTreeSet::from([1, 2, 3]);
        assert_eq!(voters_without(&current, 3), BTreeSet::from([1, 2]));
    }

    #[test]
    fn forward_to_leader_maps_to_not_leader_with_admin_endpoint() {
        let mut ftl = ForwardToLeader::<TypeConfig>::empty();
        ftl.leader_node = Some(OpenraftPeer {
            addr: "a:1".into(),
            service_endpoint: "a:2".into(),
            admin_endpoint: "a:3".into(),
        });
        let err = RaftError::APIError(ClientWriteError::ForwardToLeader(ftl));
        match map_write_error(err) {
            AdminError::NotLeader {
                leader_admin_endpoint,
            } => {
                assert_eq!(leader_admin_endpoint.as_deref(), Some("a:3"));
            }
            other => panic!("expected NotLeader, got {other:?}"),
        }
    }

    #[test]
    fn forward_to_leader_with_no_node_has_no_endpoint() {
        let err = RaftError::APIError(ClientWriteError::ForwardToLeader(ForwardToLeader::<
            TypeConfig,
        >::empty()));
        match map_write_error(err) {
            AdminError::NotLeader {
                leader_admin_endpoint,
            } => {
                assert_eq!(leader_admin_endpoint, None);
            }
            other => panic!("expected NotLeader, got {other:?}"),
        }
    }

    #[test]
    fn learner_not_found_maps_to_not_member() {
        use openraft::error::LearnerNotFound;
        let err: RaftError<TypeConfig, ClientWriteError<TypeConfig>> =
            RaftError::APIError(ClientWriteError::ChangeMembershipError(
                ChangeMembershipError::LearnerNotFound(LearnerNotFound { node_id: 7 }),
            ));
        assert!(matches!(map_write_error(err), AdminError::NotMember(7)));
    }
}
