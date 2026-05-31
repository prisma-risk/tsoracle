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
/// guess; `TargetOutOfRange` means re-issue with a target the local binary
/// can actually read or upgrade the binary.
#[derive(Debug, thiserror::Error)]
pub enum FormatActivationError {
    /// This node is not the raft leader, so it cannot drive an activation.
    #[error("cannot initiate format activation: this node is not the leader")]
    NotLeader,
    /// `target` is outside the local binary's readable range
    /// `[min, max]`. The gate short-circuits here before any peer RPC: an
    /// out-of-range target is the same answer regardless of peer reports,
    /// and the operator gets a fast, unambiguous rejection. The same
    /// surface is used by the apply arm's defense-in-depth — see
    /// `ApplyOutcome::FormatActivationTargetOutOfRange`.
    #[error(
        "format activation to target {target} blocked: target outside local readable range [{min}, {max}]"
    )]
    TargetOutOfRange { target: u8, min: u8, max: u8 },
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
    /// The membership committed as of the bump's own log position was no
    /// longer a subset of the set the gate observed — an un-gated member was
    /// added or promoted between the live gate query and the entry's
    /// commit/apply, so the bump applied as a no-op. Re-gate and re-issue.
    #[error(
        "format activation to target {target} applied as a no-op: membership changed since the gate"
    )]
    MembershipChangedSinceGate { target: u8 },
    /// The proposal (`raft.client_write`) failed before / during commit —
    /// e.g. lost leadership, the cluster lost quorum, or a fatal raft error.
    #[error("format activation proposal failed: {0}")]
    ProposalFailed(String),
}

/// Reject `target` if it is outside the local binary's readable range
/// `[MIN_READABLE_VERSION, MAX_READABLE_VERSION]`.
///
/// This is the lower-bound (and symmetric upper-bound) companion to the
/// per-member [`all_members_can_read`] check. The per-member predicate
/// guards against any member that cannot read the target; this local check
/// guards against an unsupported target up front so the activation never
/// even reaches a peer RPC or the apply arm. A target below MIN would flip
/// the active write version to a value the encode/decode codec arms reject,
/// wedging all subsequent log appends and snapshot builds.
pub fn target_in_local_readable_range(target: u8) -> Result<(), FormatActivationError> {
    let min = tsoracle_openraft_toolkit::MIN_READABLE_VERSION;
    let max = tsoracle_openraft_toolkit::MAX_READABLE_VERSION;
    if !(min..=max).contains(&target) {
        return Err(FormatActivationError::TargetOutOfRange { target, min, max });
    }
    Ok(())
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

/// Tolerant sibling of [`gather_with`]: query every member for its
/// capabilities, but instead of failing closed on the first unreachable peer,
/// capture each member's result. Returns one entry per member, in membership
/// order, with the local node answered directly from `local_capabilities`.
///
/// Where [`gather_with`] backs the activation gate (which must refuse to
/// proceed on a member it cannot confirm), this backs the read-only
/// capabilities report (which must surface an unreachable member rather than
/// hide the whole cluster behind one failure).
pub async fn report_with<S: CapabilitySource>(
    local_node: NodeId,
    local_capabilities: NodeCapabilities,
    membership: &[(NodeId, S::Node)],
    source: &S,
) -> Vec<(NodeId, Result<NodeCapabilities, String>)> {
    let mut reports = Vec::with_capacity(membership.len());
    for (node_id, member) in membership {
        let result = if *node_id == local_node {
            Ok(local_capabilities)
        } else {
            source.query(*node_id, member).await
        };
        reports.push((*node_id, result));
    }
    reports
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
    fn existing_incompatible_learner_blocks_activation_via_all_members_gate() {
        // An existing incompatible learner blocks activation until
        // remediated. There is NO separate "existing-learner block" code
        // path: the all-members gate already covers learners (the leader
        // gathers capabilities from voters AND learners read from
        // `raft.metrics().borrow_watched().membership_config.nodes()`, then
        // runs this predicate). The toolkit's `learner_meets_active_write_version`
        // (the joiner gate) refuses NEW learners on admission; this
        // predicate refuses an activation when an already-admitted member
        // (voter or learner) cannot read the target. The two together
        // cover spec §12's joiner-gate and existing-learner-block
        // requirements without duplicate enforcement.
        let target = 4u8;
        let capable_voter = caps(3, 4);
        let incompatible_learner = caps(3, 3); // max_readable_version < target
        let reports = vec![(1u64, capable_voter), (2u64, incompatible_learner)];
        assert_eq!(
            all_members_can_read(target, &reports),
            None,
            "an incompatible already-admitted member (here a learner) must \
             fail the all-members gate, blocking activation until remediated"
        );
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

    // ---- Local-binary readable-range gate (lower-bound check) ----
    //
    // The all-members gate at `all_members_can_read` checks every member's
    // `max_readable_version >= target` — the upper-bound side. The
    // lower-bound side is symmetric: `target` must also be at least
    // `MIN_READABLE_VERSION` of the local binary, otherwise the activation
    // would flip the cell to a version no member can encode/decode (an
    // operator-facing footgun and durable-damage hazard). The gate also
    // short-circuits at the local binary's range before any peer RPC: an
    // out-of-range target is the same answer regardless of what peers
    // report, and the operator gets a fast, clear rejection instead of an
    // unreachable-peer noise tail.

    #[test]
    fn target_in_local_readable_range_accepts_min() {
        assert!(
            target_in_local_readable_range(tsoracle_openraft_toolkit::MIN_READABLE_VERSION).is_ok()
        );
    }

    #[test]
    fn target_in_local_readable_range_accepts_max() {
        assert!(
            target_in_local_readable_range(tsoracle_openraft_toolkit::MAX_READABLE_VERSION).is_ok()
        );
    }

    #[test]
    fn target_in_local_readable_range_rejects_zero() {
        // Target 0 is the worst case: 0 is also the legacy-unframed wire
        // sentinel, so a 0-stamped record collides with the legacy
        // interpretation while also failing recovery decode.
        let err = target_in_local_readable_range(0).expect_err("0 is below MIN");
        match err {
            FormatActivationError::TargetOutOfRange { target, min, max } => {
                assert_eq!(target, 0);
                assert_eq!(min, tsoracle_openraft_toolkit::MIN_READABLE_VERSION);
                assert_eq!(max, tsoracle_openraft_toolkit::MAX_READABLE_VERSION);
            }
            other => panic!("expected TargetOutOfRange, got: {other:?}"),
        }
    }

    #[test]
    fn target_in_local_readable_range_rejects_below_min() {
        // The exact below-min boundary. MIN is at least 1 in any sensible
        // build (the codec's compile-time assert forbids an empty range),
        // so MIN - 1 is always a representable u8 and always out of range.
        let just_below = tsoracle_openraft_toolkit::MIN_READABLE_VERSION - 1;
        let err = target_in_local_readable_range(just_below).expect_err("MIN-1 is out of range");
        assert!(matches!(
            err,
            FormatActivationError::TargetOutOfRange { target, .. } if target == just_below
        ));
    }

    #[test]
    fn target_in_local_readable_range_rejects_above_max() {
        // MAX + 1: this case is normally caught by the per-member
        // `max_readable_version >= target` predicate at the gate, but the
        // local-binary short-circuit must also reject it so a one-member
        // cluster (or a misconfigured operator command) fails fast at the
        // leader before any RPC.
        let just_above = tsoracle_openraft_toolkit::MAX_READABLE_VERSION.saturating_add(1);
        if just_above == tsoracle_openraft_toolkit::MAX_READABLE_VERSION {
            // MAX is u8::MAX (impossible in practice but defended against);
            // skip rather than spuriously fail.
            return;
        }
        let err = target_in_local_readable_range(just_above).expect_err("MAX+1 is out of range");
        assert!(matches!(
            err,
            FormatActivationError::TargetOutOfRange { target, .. } if target == just_above
        ));
    }

    #[test]
    fn target_out_of_range_error_names_target_and_local_range() {
        // The operator-facing message must name the offending target AND
        // the binary's own range, so the remediation is unambiguous: either
        // re-issue with an in-range target, or upgrade the binary.
        let err = FormatActivationError::TargetOutOfRange {
            target: 1,
            min: 4,
            max: 4,
        };
        let rendered = err.to_string();
        assert!(rendered.contains('1'), "target missing: {rendered}");
        assert!(rendered.contains('4'), "range missing: {rendered}");
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

    #[tokio::test]
    async fn report_with_answers_local_directly_and_queries_remotes() {
        let source = FakeSource {
            responses: HashMap::from([(2u64, Ok(caps(3, 5)))]),
        };
        let membership: Vec<(NodeId, ())> = vec![(1, ()), (2, ())];
        let reports = report_with(1, caps(3, 4), &membership, &source).await;
        assert_eq!(reports.len(), 2);
        assert_eq!(reports[0].0, 1);
        assert_eq!(reports[0].1.as_ref().unwrap().active_write_version, 3);
        assert_eq!(reports[1].0, 2);
        assert_eq!(reports[1].1.as_ref().unwrap().max_readable_version, 5);
    }

    #[tokio::test]
    async fn report_with_captures_unreachable_without_short_circuiting() {
        // Node 2 is unreachable; node 3 must still be reported (gather_with
        // would have bailed at node 2). This is the tolerant contract.
        let source = FakeSource {
            responses: HashMap::from([
                (2u64, Err("connection refused".to_string())),
                (3u64, Ok(caps(3, 4))),
            ]),
        };
        let membership: Vec<(NodeId, ())> = vec![(1, ()), (2, ()), (3, ())];
        let reports = report_with(1, caps(3, 4), &membership, &source).await;
        assert_eq!(reports.len(), 3);
        assert!(reports[0].1.is_ok());
        assert_eq!(reports[1].1.as_ref().unwrap_err(), "connection refused");
        assert!(
            reports[2].1.is_ok(),
            "node 3 reported despite node 2 failing"
        );
    }

    #[tokio::test]
    async fn report_with_empty_membership_is_empty() {
        let source = FakeSource {
            responses: HashMap::new(),
        };
        let membership: Vec<(NodeId, ())> = vec![];
        let reports = report_with(1, caps(3, 4), &membership, &source).await;
        assert!(reports.is_empty());
    }
}
