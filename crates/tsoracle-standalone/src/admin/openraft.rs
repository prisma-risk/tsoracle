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
    AdminError, CapabilityReport, CapabilityState, MemberCapability, MemberEntry, MemberRole,
    MembershipAdmin, MembershipView, NewMember,
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

/// Map a `FormatActivationError` into an `AdminError` using the dedicated
/// kinds added above. `NotLeader` is a unit variant in
/// `FormatActivationError`, so the resulting
/// `AdminError::NotLeader.leader_admin_endpoint` is always None — the
/// CLI cannot auto-redirect to the leader for activation; the operator
/// (or shell orchestrator) must re-issue against a known leader.
fn map_activation_error(err: tsoracle_driver_openraft::FormatActivationError) -> AdminError {
    use tsoracle_driver_openraft::FormatActivationError as FAE;
    match err {
        FAE::NotLeader => AdminError::NotLeader {
            leader_admin_endpoint: None,
        },
        FAE::TargetOutOfRange { target, min, max } => {
            AdminError::TargetOutOfRange { target, min, max }
        }
        FAE::MembersBelowTarget { target, incapable } => {
            AdminError::MembersBelowTarget { target, incapable }
        }
        FAE::MemberUnreachable { node_id, detail } => AdminError::Driver(format!(
            "format activation gate: member {node_id} unreachable: {detail}",
        )),
        FAE::MembershipChangedSinceGate { target } => {
            AdminError::MembershipChangedSinceGate { target }
        }
        FAE::ProposalFailed(detail) => {
            AdminError::Driver(format!("format activation proposal failed: {detail}",))
        }
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

/// Build a [`CapabilityReport`] from a single membership snapshot and the
/// per-member capability results gathered over that exact set. Pure: it never
/// reads `raft.metrics()`, so the report rows correspond one-to-one with the
/// `members` slice handed in.
fn assemble_report(
    leader: Option<u64>,
    voters: &BTreeSet<u64>,
    members: &[(u64, OpenraftPeer)],
    caps: &[(
        u64,
        Result<tsoracle_driver_openraft::NodeCapabilities, String>,
    )],
) -> CapabilityReport {
    let caps_by_id: std::collections::HashMap<
        u64,
        &Result<tsoracle_driver_openraft::NodeCapabilities, String>,
    > = caps.iter().map(|(id, result)| (*id, result)).collect();

    let members = members
        .iter()
        .map(|(id, node)| {
            let caps = match caps_by_id.get(id) {
                Some(Ok(capability)) => CapabilityState::Reported {
                    min_readable: capability.min_readable_version,
                    max_readable: capability.max_readable_version,
                    active_write: capability.active_write_version,
                },
                Some(Err(detail)) => CapabilityState::Unreachable {
                    detail: detail.clone(),
                },
                None => CapabilityState::Unreachable {
                    detail: "no capability report".to_string(),
                },
            };
            MemberCapability {
                member: MemberEntry {
                    id: *id,
                    role: if voters.contains(id) {
                        MemberRole::Voter
                    } else {
                        MemberRole::Learner
                    },
                    raft_addr: node.addr.clone(),
                    service_endpoint: node.service_endpoint.clone(),
                    admin_endpoint: node.admin_endpoint.clone(),
                },
                caps,
            }
        })
        .collect();

    CapabilityReport { members, leader }
}

/// openraft-backed membership admin. Holds a clone of the `Raft` handle
/// for membership ops, plus an `Arc<StandaloneHost>` + concrete
/// `Arc<PeerCapabilitySource>` for `activate_format`. We store
/// `PeerCapabilitySource` CONCRETELY (not as `Arc<dyn CapabilitySource>`)
/// because `StandaloneHost::initiate_format_activation<S>` has `S: CapabilitySource`
/// with the default `Sized` bound — a `&dyn` would not satisfy it.
/// Production has exactly one impl (the peer-RPC dialer), so the
/// concretization costs nothing.
///
/// The mutex serializes mutating membership ops so two reconfigurations
/// cannot race. Activation does not take this mutex —
/// `StandaloneHost::initiate_format_activation` is itself apply-keyed
/// and re-validates membership at the entry's log position.
pub(crate) struct OpenraftMembershipAdmin {
    raft: Raft<TypeConfig, HighWaterStateMachine>,
    host: std::sync::Arc<tsoracle_driver_openraft::StandaloneHost>,
    source: std::sync::Arc<crate::drivers::openraft::network::PeerCapabilitySource>,
    op_lock: Mutex<()>,
}

impl OpenraftMembershipAdmin {
    pub(crate) fn new(
        raft: Raft<TypeConfig, HighWaterStateMachine>,
        host: std::sync::Arc<tsoracle_driver_openraft::StandaloneHost>,
        source: std::sync::Arc<crate::drivers::openraft::network::PeerCapabilitySource>,
    ) -> Self {
        Self {
            raft,
            host,
            source,
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

    async fn activate_format(&self, target: u8) -> Result<(), AdminError> {
        // Activation has its own internal serialization (apply-keyed flip,
        // membership re-validation at entry log position) — no op_lock here.
        self.host
            .initiate_format_activation(target, &*self.source)
            .await
            .map_err(map_activation_error)
    }

    async fn report_capabilities(&self) -> Result<CapabilityReport, AdminError> {
        let (leader, voters, local_id, members) = {
            let metrics = self.raft.metrics().borrow_watched().clone();
            let voters: BTreeSet<u64> = metrics.membership_config.voter_ids().collect();
            let members: Vec<(u64, OpenraftPeer)> = metrics
                .membership_config
                .nodes()
                .map(|(id, node)| (*id, node.clone()))
                .collect();
            (metrics.current_leader, voters, metrics.id, members)
        };
        let local_capabilities =
            tsoracle_driver_openraft::NodeCapabilities::local(self.host.active_write_version());
        let caps = tsoracle_driver_openraft::report_with(
            local_id,
            local_capabilities,
            &members,
            &*self.source,
        )
        .await;
        Ok(assemble_report(leader, &voters, &members, &caps))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(addr: &str) -> OpenraftPeer {
        OpenraftPeer {
            addr: format!("{addr}:1"),
            service_endpoint: format!("{addr}:2"),
            admin_endpoint: format!("{addr}:3"),
        }
    }

    fn ncaps(min: u8, max: u8, active: u8) -> tsoracle_driver_openraft::NodeCapabilities {
        tsoracle_driver_openraft::NodeCapabilities {
            min_readable_version: min,
            max_readable_version: max,
            active_write_version: active,
        }
    }

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
    fn assemble_report_builds_one_row_per_member_with_roles_and_caps() {
        let members = vec![(1u64, peer("a")), (2u64, peer("b"))];
        let voters = BTreeSet::from([1u64]);
        let caps = vec![(1u64, Ok(ncaps(4, 6, 4))), (2u64, Ok(ncaps(4, 5, 4)))];
        let report = assemble_report(Some(1), &voters, &members, &caps);

        assert_eq!(report.leader, Some(1));
        assert_eq!(report.members.len(), 2);
        assert_eq!(report.members[0].member.id, 1);
        assert_eq!(report.members[0].member.role, MemberRole::Voter);
        assert_eq!(
            report.members[0].caps,
            crate::admin::CapabilityState::Reported {
                min_readable: 4,
                max_readable: 6,
                active_write: 4,
            }
        );
        assert_eq!(report.members[1].member.role, MemberRole::Learner);
        assert_eq!(report.members[1].member.raft_addr, "b:1");
    }

    #[test]
    fn assemble_report_marks_failed_query_unreachable_with_detail() {
        let members = vec![(1u64, peer("a")), (2u64, peer("b"))];
        let voters = BTreeSet::from([1u64, 2u64]);
        let caps = vec![
            (1u64, Ok(ncaps(4, 6, 4))),
            (2u64, Err("connection refused".to_string())),
        ];
        let report = assemble_report(None, &voters, &members, &caps);
        assert_eq!(report.leader, None);
        assert_eq!(
            report.members[1].caps,
            crate::admin::CapabilityState::Unreachable {
                detail: "connection refused".to_string()
            }
        );
    }

    #[test]
    fn assemble_report_marks_member_absent_from_caps_unreachable() {
        // A member in the snapshot but missing from the caps slice is
        // Unreachable, never silently dropped.
        let members = vec![(1u64, peer("a")), (2u64, peer("b"))];
        let voters = BTreeSet::from([1u64, 2u64]);
        let caps = vec![(1u64, Ok(ncaps(4, 6, 4)))];
        let report = assemble_report(None, &voters, &members, &caps);
        assert_eq!(report.members.len(), 2);
        assert!(matches!(
            report.members[1].caps,
            crate::admin::CapabilityState::Unreachable { .. }
        ));
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

    #[test]
    fn activation_not_leader_maps_to_not_leader_none_endpoint() {
        use tsoracle_driver_openraft::FormatActivationError as FAE;
        match map_activation_error(FAE::NotLeader) {
            AdminError::NotLeader {
                leader_admin_endpoint,
            } => {
                assert_eq!(leader_admin_endpoint, None);
            }
            other => panic!("expected NotLeader, got {other:?}"),
        }
    }

    #[test]
    fn activation_target_out_of_range_maps_correctly() {
        use tsoracle_driver_openraft::FormatActivationError as FAE;
        match map_activation_error(FAE::TargetOutOfRange {
            target: 99,
            min: 4,
            max: 5,
        }) {
            AdminError::TargetOutOfRange { target, min, max } => {
                assert_eq!((target, min, max), (99, 4, 5));
            }
            other => panic!("expected TargetOutOfRange, got {other:?}"),
        }
    }

    #[test]
    fn activation_members_below_target_maps_preserves_incapable() {
        use tsoracle_driver_openraft::FormatActivationError as FAE;
        match map_activation_error(FAE::MembersBelowTarget {
            target: 5,
            incapable: vec![(1, 4), (3, 4)],
        }) {
            AdminError::MembersBelowTarget { target, incapable } => {
                assert_eq!(target, 5);
                assert_eq!(incapable, vec![(1u64, 4u8), (3u64, 4u8)]);
            }
            other => panic!("expected MembersBelowTarget, got {other:?}"),
        }
    }

    #[test]
    fn activation_member_unreachable_maps_to_driver_with_detail() {
        use tsoracle_driver_openraft::FormatActivationError as FAE;
        match map_activation_error(FAE::MemberUnreachable {
            node_id: 2,
            detail: "timeout".into(),
        }) {
            AdminError::Driver(s) => {
                assert!(s.contains("member 2"), "missing node id in {s:?}");
                assert!(s.contains("timeout"), "missing detail in {s:?}");
            }
            other => panic!("expected Driver, got {other:?}"),
        }
    }

    #[test]
    fn activation_membership_changed_maps_correctly() {
        use tsoracle_driver_openraft::FormatActivationError as FAE;
        match map_activation_error(FAE::MembershipChangedSinceGate { target: 5 }) {
            AdminError::MembershipChangedSinceGate { target } => {
                assert_eq!(target, 5);
            }
            other => panic!("expected MembershipChangedSinceGate, got {other:?}"),
        }
    }

    #[test]
    fn activation_proposal_failed_maps_to_driver_with_detail() {
        use tsoracle_driver_openraft::FormatActivationError as FAE;
        match map_activation_error(FAE::ProposalFailed("write failed".into())) {
            AdminError::Driver(s) => {
                assert!(s.contains("write failed"), "missing detail in {s:?}");
            }
            other => panic!("expected Driver, got {other:?}"),
        }
    }
}
