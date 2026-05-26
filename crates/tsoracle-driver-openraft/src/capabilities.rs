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

//! Format-migration capability reporting and the leader-side all-members
//! activation gate's pure logic.
//!
//! A [`NodeCapabilities`] is the answer to "what schema versions can this node
//! read, and what version is it actively writing?" Every member reports its own
//! via the `Capabilities` peer RPC (see `tsoracle-standalone`); the leader
//! gathers all members' reports and runs [`all_members_can_read`] before any
//! format bump is proposed (the proposal itself is a later phase). The struct
//! crosses the wire as a postcard body inside the existing `RaftMessage`
//! envelope, so it lives here in the driver crate where both the peer transport
//! and the leader-side gate can reach it.

use std::collections::BTreeSet;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

type NodeId = u64;

/// What schema versions a single node can read, and the version it is actively
/// writing right now.
///
/// `min_readable_version` / `max_readable_version` are compile-time constants of
/// the running binary (the oldest and newest formats it has a parser for);
/// `active_write_version` is the durable, runtime value the node currently
/// stamps on new records and wire bodies. The activation gate compares every
/// member's `max_readable_version` against a proposed target so no committed
/// record can reach a member that cannot decode it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeCapabilities {
    pub min_readable_version: u8,
    pub max_readable_version: u8,
    pub active_write_version: u8,
}

impl NodeCapabilities {
    /// Build the local node's capabilities: the readable range is fixed by this
    /// binary's compile-time constants; the active write version is the durable
    /// runtime value supplied by the caller (read through the state machine's
    /// `active_write_version()` accessor).
    pub fn local(active_write_version: u8) -> Self {
        Self {
            min_readable_version: tsoracle_openraft_toolkit::MIN_READABLE_VERSION,
            max_readable_version: tsoracle_openraft_toolkit::MAX_READABLE_VERSION,
            active_write_version,
        }
    }
}

/// The all-members activation gate: return the set of member node ids iff
/// **every** gathered member can read `target` (its `max_readable_version >=
/// target`), otherwise `None`.
///
/// The gate is all-members, not quorum, and the caller must have gathered every
/// current member (voters AND learners) before calling this — a lagging or
/// learner peer on an old-only binary would otherwise reject every record at
/// the new version once it committed. The returned set is exactly the gathered
/// members; a later phase embeds it in the bump entry so apply can re-validate
/// the membership at the entry's own log position against this snapshot.
pub fn all_members_can_read(
    target: u8,
    capabilities: &[(NodeId, NodeCapabilities)],
) -> Option<BTreeSet<NodeId>> {
    if capabilities
        .iter()
        .all(|(_, member)| member.max_readable_version >= target)
    {
        Some(capabilities.iter().map(|(node_id, _)| *node_id).collect())
    } else {
        None
    }
}

/// Why an operator-initiated format activation could not proceed past the gate.
///
/// Returned by `StandaloneHost::initiate_format_activation`. None of these are
/// retryable in place by the caller without remediation: `NotLeader` means
/// re-issue against the leader; `MembersBelowTarget` means upgrade or remove
/// the named members and re-issue; `MemberUnreachable` means the cluster could
/// not confirm a member's capability and the gate fails closed rather than
/// guess.
#[derive(Debug, thiserror::Error)]
pub enum FormatActivationError {
    /// This node is not the raft leader, so it cannot drive an activation.
    #[error("cannot initiate format activation: this node is not the leader")]
    NotLeader,
    /// At least one current member cannot read `target`. `incapable` lists the
    /// offending `(node_id, max_readable_version)` pairs for the operator.
    #[error("format activation to target {target} blocked: members below target: {incapable:?}")]
    MembersBelowTarget {
        target: u8,
        incapable: Vec<(NodeId, u8)>,
    },
    /// A member could not be queried for its capabilities; the gate fails closed.
    #[error("format activation gate failed: member {node_id} unreachable: {detail}")]
    MemberUnreachable { node_id: NodeId, detail: String },
}

/// Abstracts querying one peer member for its [`NodeCapabilities`]. Implemented
/// in the standalone transport crate as a thin adapter over the `Capabilities`
/// peer RPC; defined here so [`StandaloneHost::gather_member_capabilities`]
/// (in `standalone.rs`) can be generic over it (the driver crate does not
/// depend on the transport crate) and so the gate is unit-testable with a fake
/// source.
///
/// [`StandaloneHost::gather_member_capabilities`]: crate::standalone::StandaloneHost::gather_member_capabilities
#[async_trait]
pub trait CapabilitySource: Send + Sync {
    /// The `Node` (membership endpoint) type this source dials.
    type Node: Send + Sync;

    /// Query `member`'s capabilities. `detail` on failure is a human-readable
    /// reason folded into [`FormatActivationError::MemberUnreachable`].
    async fn query(&self, node_id: NodeId, member: &Self::Node)
    -> Result<NodeCapabilities, String>;
}

/// Gather every member's capabilities, answering the `local_node` from
/// `local_capabilities` directly (no self-RPC) and every other member via
/// `source`. Fails closed ([`FormatActivationError::MemberUnreachable`]) the
/// moment any remote query fails — the all-members gate cannot pass on a
/// member it could not confirm. `membership` is the full current voter+learner
/// set.
pub async fn gather_with<S: CapabilitySource>(
    local_node: NodeId,
    local_capabilities: NodeCapabilities,
    membership: &[(NodeId, S::Node)],
    source: &S,
) -> Result<Vec<(NodeId, NodeCapabilities)>, FormatActivationError> {
    let mut gathered = Vec::with_capacity(membership.len());
    for (node_id, member) in membership {
        let capabilities = if *node_id == local_node {
            local_capabilities
        } else {
            source.query(*node_id, member).await.map_err(|detail| {
                FormatActivationError::MemberUnreachable {
                    node_id: *node_id,
                    detail,
                }
            })?
        };
        gathered.push((*node_id, capabilities));
    }
    Ok(gathered)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;

    fn caps(active: u8, max: u8) -> NodeCapabilities {
        NodeCapabilities {
            min_readable_version: tsoracle_openraft_toolkit::MIN_READABLE_VERSION,
            max_readable_version: max,
            active_write_version: active,
        }
    }

    #[test]
    fn local_capabilities_reports_compile_time_read_range() {
        let capabilities = NodeCapabilities::local(7);
        assert_eq!(
            capabilities.min_readable_version,
            tsoracle_openraft_toolkit::MIN_READABLE_VERSION
        );
        assert_eq!(
            capabilities.max_readable_version,
            tsoracle_openraft_toolkit::MAX_READABLE_VERSION
        );
        assert_eq!(capabilities.active_write_version, 7);
    }

    #[test]
    fn node_capabilities_postcard_round_trips() {
        let original = NodeCapabilities {
            min_readable_version: 3,
            max_readable_version: 5,
            active_write_version: 4,
        };
        let bytes = postcard::to_stdvec(&original).expect("encode");
        let decoded: NodeCapabilities = postcard::from_bytes(&bytes).expect("decode");
        assert_eq!(original, decoded);
    }

    #[test]
    fn gate_passes_when_all_members_can_read_target() {
        let reports = vec![(1u64, caps(3, 4)), (2, caps(3, 5)), (3, caps(3, 4))];
        let gated = all_members_can_read(4, &reports).expect("all members can read 4");
        assert_eq!(gated, BTreeSet::from([1, 2, 3]));
    }

    #[test]
    fn gate_passes_at_exact_equality() {
        // max_readable_version == target is sufficient (>=, not >).
        let reports = vec![(1u64, caps(3, 4)), (2, caps(3, 4))];
        assert_eq!(
            all_members_can_read(4, &reports),
            Some(BTreeSet::from([1, 2]))
        );
    }

    #[test]
    fn gate_fails_when_one_member_is_below_target() {
        // Node 2 can only read up to v3; target v4 must be refused.
        let reports = vec![(1u64, caps(3, 4)), (2, caps(3, 3)), (3, caps(3, 4))];
        assert_eq!(all_members_can_read(4, &reports), None);
    }

    #[test]
    fn gate_on_empty_membership_is_vacuously_satisfied_with_empty_set() {
        // No members → no member can fail the predicate → Some(empty). A real
        // cluster always has at least the local node, but the predicate itself
        // is total; the empty case is documented and tested so callers above
        // it can rely on Some meaning "every gathered member passed".
        assert_eq!(all_members_can_read(4, &[]), Some(BTreeSet::new()));
    }

    #[test]
    fn format_activation_error_below_target_names_members_and_target() {
        let err = FormatActivationError::MembersBelowTarget {
            target: 4,
            incapable: vec![(2, 3), (5, 3)],
        };
        let rendered = err.to_string();
        assert!(rendered.contains("target 4"), "got: {rendered}");
        assert!(
            rendered.contains('2') && rendered.contains('5'),
            "got: {rendered}"
        );
    }

    struct FakeSource {
        responses: HashMap<NodeId, Result<NodeCapabilities, String>>,
    }

    #[async_trait]
    impl CapabilitySource for FakeSource {
        type Node = ();

        async fn query(&self, node_id: NodeId, _member: &()) -> Result<NodeCapabilities, String> {
            self.responses
                .get(&node_id)
                .cloned()
                .unwrap_or_else(|| Err(format!("no fake response for {node_id}")))
        }
    }

    #[tokio::test]
    async fn gather_with_collects_remote_and_local() {
        // Local node 1 reports active=3,max=4 directly; remote node 2 answers
        // via the source. Membership = {1: (), 2: ()}.
        let source = FakeSource {
            responses: HashMap::from([(2u64, Ok(caps(3, 5)))]),
        };
        let membership: Vec<(NodeId, ())> = vec![(1, ()), (2, ())];
        let gathered = gather_with(1, caps(3, 4), &membership, &source)
            .await
            .expect("gather succeeds");
        let by_id: HashMap<NodeId, NodeCapabilities> = gathered.into_iter().collect();
        assert_eq!(by_id[&1].active_write_version, 3);
        assert_eq!(by_id[&2].max_readable_version, 5);
    }

    #[tokio::test]
    async fn gather_with_surfaces_unreachable_member() {
        let source = FakeSource {
            responses: HashMap::from([(2u64, Err("connection refused".to_string()))]),
        };
        let membership: Vec<(NodeId, ())> = vec![(1, ()), (2, ())];
        let err = gather_with(1, caps(3, 4), &membership, &source)
            .await
            .expect_err("an unreachable member fails the gather closed");
        assert!(matches!(
            err,
            FormatActivationError::MemberUnreachable { node_id: 2, .. }
        ));
    }
}
