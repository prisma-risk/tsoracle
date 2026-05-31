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

//! Bundled host: owns its own raft cluster and the [`HighWaterStateMachine`].
//!
//! Use this when you want a dedicated TSO raft, separate from any other
//! consensus state your service runs. For services that already run an
//! openraft cluster (e.g. a placement driver), implement
//! [`OpenraftHighWaterHost`] directly against
//! that existing cluster instead.

use std::collections::BTreeSet;

use async_trait::async_trait;
use openraft::Raft;
use openraft::RaftMetrics;
use openraft::ReadPolicy;
use openraft::ServerState;
use openraft::async_runtime::watch::WatchReceiver;
use openraft::error::{ClientWriteError, LinearizableReadError, RaftError};
use openraft::type_config::alias::WatchReceiverOf;
use tsoracle_consensus::ConsensusError;

use crate::capabilities::{
    CapabilitySource, FormatActivationError, NodeCapabilities, all_members_can_read, gather_with,
    target_in_local_readable_range,
};
use crate::host::OpenraftHighWaterHost;
use crate::log_entry::{HighWaterCommand, SetFormatVersionPayload};
use crate::state_machine::HighWaterStateMachine;
use crate::type_config::{ApplyOutcome, OpenraftPeer, TypeConfig};
use tsoracle_consensus::AdvancePayload;

/// `OpenraftHighWaterHost` that owns its own raft cluster + state machine.
///
/// `state_machine` must share the inner `Arc` with the one handed to
/// `Raft::new`; otherwise `current_high_water` will read from a state machine
/// that never sees the apply pipeline.
pub struct StandaloneHost {
    raft: Raft<TypeConfig, HighWaterStateMachine>,
    state_machine: HighWaterStateMachine,
}

impl StandaloneHost {
    /// Build a standalone host from a pre-constructed raft handle and a
    /// state-machine clone.
    pub fn new(
        raft: Raft<TypeConfig, HighWaterStateMachine>,
        state_machine: HighWaterStateMachine,
    ) -> Self {
        Self {
            raft,
            state_machine,
        }
    }

    /// The format version this node currently emits (stamps onto persisted
    /// records, snapshots, and — in a later phase — peer RPC payloads). Reads
    /// the shared
    /// [`ActiveWriteVersion`](tsoracle_openraft_toolkit::ActiveWriteVersion)
    /// cell via the state machine, which shares the one cell with the log
    /// store. In this release it is always
    /// [`BASELINE_WRITE_VERSION`](tsoracle_openraft_toolkit::BASELINE_WRITE_VERSION);
    /// a successful activation apply (later phase) advances it.
    pub fn active_write_version(&self) -> u8 {
        self.state_machine.active_write_version()
    }

    /// The local node's format-migration capabilities: compile-time read range
    /// plus the durable active write version.
    pub fn local_capabilities(&self) -> NodeCapabilities {
        NodeCapabilities::local(self.active_write_version())
    }

    /// Read the current voter+learner membership and the local node id /
    /// leader state from `raft.metrics()`. Returns `(local_node_id, is_leader,
    /// members)`, where `members` is every node in
    /// `membership_config.nodes()` — voters and learners both, because the
    /// all-members gate covers learners too.
    fn membership_snapshot(&self) -> (u64, bool, Vec<(u64, OpenraftPeer)>) {
        let metrics = self.raft.metrics();
        let snapshot = metrics.borrow_watched();
        let local_node_id = snapshot.id;
        let is_leader = snapshot.state == ServerState::Leader;
        let members = snapshot
            .membership_config
            .nodes()
            .map(|(node_id, node)| (*node_id, node.clone()))
            .collect();
        (local_node_id, is_leader, members)
    }

    /// Query every current member (voters AND learners) for its capabilities,
    /// answering the local node directly. `source` dials remote members. Fails
    /// closed the moment any remote query fails — the all-members gate cannot
    /// pass on a member it could not confirm.
    pub async fn gather_member_capabilities<S>(
        &self,
        source: &S,
    ) -> Result<Vec<(u64, NodeCapabilities)>, FormatActivationError>
    where
        S: CapabilitySource<Node = OpenraftPeer>,
    {
        let (local_node_id, _is_leader, members) = self.membership_snapshot();
        gather_with(local_node_id, self.local_capabilities(), &members, source).await
    }

    /// Run the all-members activation gate for `target`. Requires local
    /// leadership (only the leader may drive activation), gathers every
    /// member's capabilities, and returns the gated member set iff all can
    /// read `target`. On failure returns a [`FormatActivationError`] (not
    /// leader / a member below target / a member unreachable). This is the
    /// function boundary that a later phase's proposal step will consume: the
    /// returned set is exactly the snapshot a `SetFormatVersion` log entry
    /// would embed for apply to re-validate against.
    pub async fn run_activation_gate<S>(
        &self,
        target: u8,
        source: &S,
    ) -> Result<BTreeSet<u64>, FormatActivationError>
    where
        S: CapabilitySource<Node = OpenraftPeer>,
    {
        let (_local_node_id, is_leader, _members) = self.membership_snapshot();
        if !is_leader {
            return Err(FormatActivationError::NotLeader);
        }
        // Local-binary range short-circuit BEFORE any peer RPC: an
        // out-of-range target cannot be made valid by what peers report,
        // and the apply arm would no-op it anyway. Fail fast with a
        // clear, operator-actionable error and account the rejection
        // under the same `rejected_by_gate` counter as the per-member
        // path — both are "the gate refused this activation".
        if let Err(err) = target_in_local_readable_range(target) {
            tracing::warn!(
                target = target,
                ?err,
                "format activation rejected: target outside local readable range"
            );
            crate::observability::record_rejected_by_gate();
            return Err(err);
        }
        let gathered = self.gather_member_capabilities(source).await?;

        // Format-migration observability: the binding constraint on
        // activation is the lowest `max_readable_version` across all
        // current members; publish it every gate run and trace each
        // member's capability.
        let min_member_max_readable = gathered
            .iter()
            .map(|(_node_id, capability)| capability.max_readable_version)
            .min()
            .unwrap_or(tsoracle_openraft_toolkit::MIN_READABLE_VERSION);
        crate::observability::record_min_member_read_capability(min_member_max_readable);
        for (node_id, capability) in &gathered {
            tracing::debug!(
                node_id = node_id,
                min_readable_version = capability.min_readable_version,
                max_readable_version = capability.max_readable_version,
                active_write_version = capability.active_write_version,
                "format activation: member capability"
            );
        }

        match all_members_can_read(target, &gathered) {
            Some(gated_members) => {
                tracing::info!(
                    target = target,
                    gated_members = ?gated_members,
                    "format activation gate passed"
                );
                Ok(gated_members)
            }
            None => {
                let incapable: Vec<(u64, u8)> = gathered
                    .iter()
                    .filter(|(_, capabilities)| capabilities.max_readable_version < target)
                    .map(|(node_id, capabilities)| (*node_id, capabilities.max_readable_version))
                    .collect();
                tracing::warn!(
                    target = target,
                    min_member_read_capability = min_member_max_readable,
                    incapable = ?incapable,
                    "format activation rejected by all-members gate"
                );
                crate::observability::record_rejected_by_gate();
                Err(FormatActivationError::MembersBelowTarget { target, incapable })
            }
        }
    }

    /// Propose a `SetFormatVersion` bump and return the apply outcome
    /// observed through the `client_write` response. The flip (if any) has
    /// already happened inside the apply by the time this returns — the
    /// shared cell reads `target` iff the outcome is
    /// [`ApplyOutcome::FormatActivated`]. NO meta key is written; durability
    /// is the committed log entry plus the snapshot byte.
    async fn submit_set_format_version(
        &self,
        target: u8,
        gated_members: BTreeSet<u64>,
    ) -> Result<ApplyOutcome, FormatActivationError> {
        match self
            .raft
            .client_write(HighWaterCommand::SetFormatVersion(
                SetFormatVersionPayload {
                    target,
                    gated_members,
                },
            ))
            .await
        {
            Ok(resp) => {
                // The entry committed and applied (client_write blocks
                // until apply); record both signals here. Apply-keyed
                // counters (applied / noop) are emitted inside the apply
                // arm so they reflect the actual subset-check outcome.
                crate::observability::record_committed();
                Ok(resp.data.outcome)
            }
            Err(err) => Err(FormatActivationError::ProposalFailed(err.to_string())),
        }
    }

    /// Operator entry point to begin a format-version activation to `target`
    /// (interim, library-only).
    ///
    /// Runs the all-members gate (which live-queries every current
    /// voter+learner via `source` for its capabilities) and, on a passing
    /// gate, proposes a `SetFormatVersion { target, gated_members }` entry
    /// via `raft.client_write`. The state-machine apply re-validates the
    /// subset against the membership committed AS OF the entry's own log
    /// position — apply-keyed, never raw-commit-keyed — and sets the shared
    /// active-write-version cell only on a successful (non-no-op) apply.
    ///
    /// Returns `Ok(())` iff the apply reported
    /// [`ApplyOutcome::FormatActivated`] (the cell now reads `target`). A
    /// [`ApplyOutcome::FormatActivationNoop`] surfaces as
    /// [`FormatActivationError::MembershipChangedSinceGate`] — the operator
    /// re-gates and re-issues.
    pub async fn initiate_format_activation<S>(
        &self,
        target: u8,
        source: &S,
    ) -> Result<(), FormatActivationError>
    where
        S: CapabilitySource<Node = OpenraftPeer>,
    {
        // Gate: live-query every current member; returns the exact gated
        // member set on success, or a gate error.
        let gated_members = self.run_activation_gate(target, source).await?;

        // Gate passed — about to propose. The `proposed` counter
        // increments here so a rejected_by_gate path (handled inside
        // run_activation_gate) does not also increment proposed.
        crate::observability::record_proposed();

        // Propose the bump carrying the gated set. Apply re-validates the
        // subset at the entry's own log position and sets the shared cell
        // ONLY on a successful (non-no-op) apply. We learn which happened
        // from the returned `ApplyOutcome` — the apply-keyed flip.
        let outcome = self
            .submit_set_format_version(target, gated_members)
            .await?;
        classify_activation_outcome(outcome, target)
    }
}

/// Map a `SetFormatVersion` apply outcome to the activation result.
///
/// `FormatActivated { target }` is the apply-keyed flip succeeding (the
/// shared cell now reads `target`). `FormatActivationNoop` means membership
/// drifted out of the gated set by the bump's log position; surface as
/// [`FormatActivationError::MembershipChangedSinceGate`]. `Advanced` is
/// impossible for a `SetFormatVersion` entry but is mapped defensively to
/// the no-op error rather than silently claiming success.
/// `FormatActivationTargetOutOfRange` is the apply arm's defense-in-depth
/// firing — possible only if a buggy or protocol-violating proposal reached
/// commit despite the gate's `target_in_local_readable_range` short-circuit;
/// surface it as [`FormatActivationError::TargetOutOfRange`] using the local
/// binary's range so the operator's remediation matches the gate-rejection
/// surface.
fn classify_activation_outcome(
    outcome: ApplyOutcome,
    target: u8,
) -> Result<(), FormatActivationError> {
    match outcome {
        ApplyOutcome::FormatActivated { target: applied } if applied == target => Ok(()),
        ApplyOutcome::FormatActivationTargetOutOfRange { target: applied } => {
            Err(FormatActivationError::TargetOutOfRange {
                target: applied,
                min: tsoracle_openraft_toolkit::MIN_READABLE_VERSION,
                max: tsoracle_openraft_toolkit::MAX_READABLE_VERSION,
            })
        }
        ApplyOutcome::FormatActivated { .. }
        | ApplyOutcome::FormatActivationNoop { .. }
        | ApplyOutcome::Advanced => {
            Err(FormatActivationError::MembershipChangedSinceGate { target })
        }
        // A SetFormatVersion entry's apply can only yield a Format*/Advanced
        // outcome; a dense outcome here would be a state-machine bug, not an
        // activation result. Fail loud rather than misreport it as a benign
        // membership change (which would send the operator down the wrong
        // remediation path).
        other @ (ApplyOutcome::DenseAdvanced { .. }
        | ApplyOutcome::DenseCardinalityExceeded { .. }
        | ApplyOutcome::DenseOverflow
        | ApplyOutcome::DenseBatchAdvanced { .. }
        | ApplyOutcome::DenseBatchCardinalityExceeded { .. }
        | ApplyOutcome::DenseBatchOverflow) => {
            unreachable!("dense ApplyOutcome {other:?} returned from a SetFormatVersion entry")
        }
    }
}

#[async_trait]
impl OpenraftHighWaterHost for StandaloneHost {
    type Config = TypeConfig;

    fn metrics(&self) -> WatchReceiverOf<Self::Config, RaftMetrics<Self::Config>> {
        self.raft.metrics()
    }

    async fn current_high_water(&self) -> Result<u64, ConsensusError> {
        // Issue the openraft read barrier so the local SM read below reflects
        // every committed write from any prior leader at any prior epoch —
        // the contract on `ConsensusDriver::load_high_water`. The fence in
        // `tsoracle-server/src/fence.rs` is the load-bearing reader; a stale
        // value there would let a new leader's `serving_floor + 1` land below
        // a timestamp the prior leader could have served.
        if let Err(e) = self.raft.ensure_linearizable(ReadPolicy::ReadIndex).await {
            return Err(classify_read_error(e));
        }
        Ok(self.state_machine.current_value().await)
    }

    async fn submit_advance(&self, at_least: u64) -> Result<u64, ConsensusError> {
        match self
            .raft
            .client_write(HighWaterCommand::Advance(AdvancePayload { at_least }))
            .await
        {
            Ok(resp) => Ok(resp.data.value),
            Err(e) => Err(classify_client_write_error(e)),
        }
    }

    fn active_write_version(&self) -> u8 {
        // Delegate to the inherent method on `StandaloneHost` which reads the
        // shared `ActiveWriteVersion` cell via the state machine.
        StandaloneHost::active_write_version(self)
    }

    async fn submit_advance_dense(
        &self,
        key: &tsoracle_core::SeqKey,
        count: u32,
    ) -> Result<u64, ConsensusError> {
        match self
            .raft
            .client_write(HighWaterCommand::AdvanceDense {
                key: key.clone(),
                count,
            })
            .await
        {
            Ok(resp) => match resp.data.outcome {
                crate::type_config::ApplyOutcome::DenseAdvanced { start } => Ok(start),
                crate::type_config::ApplyOutcome::DenseCardinalityExceeded { cap } => {
                    Err(ConsensusError::SeqKeyCardinalityExceeded { cap })
                }
                crate::type_config::ApplyOutcome::DenseOverflow => Err(ConsensusError::SeqOverflow),
                other => Err(ConsensusError::PermanentDriver(
                    format!("unexpected ApplyOutcome for AdvanceDense: {other:?}").into(),
                )),
            },
            Err(e) => Err(classify_client_write_error(e)),
        }
    }

    async fn submit_advance_dense_batch(
        &self,
        entries: &[(tsoracle_core::SeqKey, u32)],
    ) -> Result<Vec<u64>, ConsensusError> {
        let payload: Vec<crate::log_entry::DenseAdvance> = entries
            .iter()
            .map(|(key, count)| crate::log_entry::DenseAdvance {
                key: key.clone(),
                count: *count,
            })
            .collect();
        match self
            .raft
            .client_write(HighWaterCommand::AdvanceDenseBatch { entries: payload })
            .await
        {
            Ok(resp) => match resp.data.outcome {
                crate::type_config::ApplyOutcome::DenseBatchAdvanced { starts } => Ok(starts),
                crate::type_config::ApplyOutcome::DenseBatchCardinalityExceeded { cap } => {
                    Err(ConsensusError::SeqKeyCardinalityExceeded { cap })
                }
                crate::type_config::ApplyOutcome::DenseBatchOverflow => {
                    Err(ConsensusError::SeqOverflow)
                }
                other => Err(ConsensusError::PermanentDriver(
                    format!("unexpected ApplyOutcome for AdvanceDenseBatch: {other:?}").into(),
                )),
            },
            Err(e) => Err(classify_client_write_error(e)),
        }
    }

    async fn current_dense_seq(&self, key: &tsoracle_core::SeqKey) -> Result<u64, ConsensusError> {
        // Issue the same linearizable read barrier as `current_high_water` so
        // the local SM read below reflects every committed write from any prior
        // leader at any prior epoch.
        if let Err(e) = self.raft.ensure_linearizable(ReadPolicy::ReadIndex).await {
            return Err(classify_read_error(e));
        }
        Ok(self.state_machine.dense_value(key.as_str()))
    }
}

// `ForwardToLeader` maps to `NotLeader` so the server's step-down path
// triggers cleanly. `RaftError::Fatal` is the unrecoverable case — storage
// failure, raft-task panic, explicit shutdown — and must surface as
// `PermanentDriver` so the service layer returns `INTERNAL` rather than the
// retryable `UNAVAILABLE` (see the classification table on `ConsensusError`).
// Everything else (quorum loss, leadership churn) is transient.
fn classify_read_error(
    err: RaftError<TypeConfig, LinearizableReadError<TypeConfig>>,
) -> ConsensusError {
    match err {
        RaftError::APIError(LinearizableReadError::ForwardToLeader(_)) => {
            ConsensusError::NotLeader { observed: None }
        }
        RaftError::Fatal(_) => ConsensusError::PermanentDriver(Box::new(err)),
        _ => ConsensusError::TransientDriver(Box::new(err)),
    }
}

fn classify_client_write_error(
    err: RaftError<TypeConfig, ClientWriteError<TypeConfig>>,
) -> ConsensusError {
    match err {
        RaftError::APIError(ClientWriteError::ForwardToLeader(_)) => {
            ConsensusError::NotLeader { observed: None }
        }
        RaftError::Fatal(_) => ConsensusError::PermanentDriver(Box::new(err)),
        _ => ConsensusError::TransientDriver(Box::new(err)),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use openraft::error::{
        ChangeMembershipError, EmptyMembership, Fatal, ForwardToLeader, QuorumNotEnough,
    };

    use super::*;

    #[test]
    fn fatal_read_error_classifies_as_permanent_driver() {
        let err =
            RaftError::<TypeConfig, LinearizableReadError<TypeConfig>>::Fatal(Fatal::Panicked);
        assert!(matches!(
            classify_read_error(err),
            ConsensusError::PermanentDriver(_)
        ));
    }

    #[test]
    fn fatal_client_write_error_classifies_as_permanent_driver() {
        let err = RaftError::<TypeConfig, ClientWriteError<TypeConfig>>::Fatal(Fatal::Stopped);
        assert!(matches!(
            classify_client_write_error(err),
            ConsensusError::PermanentDriver(_)
        ));
    }

    #[test]
    fn forward_to_leader_read_error_classifies_as_not_leader() {
        let err = RaftError::<TypeConfig, LinearizableReadError<TypeConfig>>::APIError(
            LinearizableReadError::ForwardToLeader(ForwardToLeader::empty()),
        );
        assert!(matches!(
            classify_read_error(err),
            ConsensusError::NotLeader { observed: None }
        ));
    }

    #[test]
    fn quorum_not_enough_read_error_classifies_as_transient_driver() {
        let err = RaftError::<TypeConfig, LinearizableReadError<TypeConfig>>::APIError(
            LinearizableReadError::QuorumNotEnough(QuorumNotEnough {
                cluster: String::new(),
                got: BTreeSet::new(),
            }),
        );
        assert!(matches!(
            classify_read_error(err),
            ConsensusError::TransientDriver(_)
        ));
    }

    #[test]
    fn change_membership_client_write_error_classifies_as_transient_driver() {
        let err = RaftError::<TypeConfig, ClientWriteError<TypeConfig>>::APIError(
            ClientWriteError::ChangeMembershipError(ChangeMembershipError::EmptyMembership(
                EmptyMembership {},
            )),
        );
        assert!(matches!(
            classify_client_write_error(err),
            ConsensusError::TransientDriver(_)
        ));
    }

    #[tokio::test]
    async fn host_active_write_version_delegates_to_the_shared_cell() {
        // StandaloneHost surfaces the SM's shared cell. Assert the delegation
        // target (the SM accessor) behaves; the host's accessor is a thin
        // pass-through to it. Building a full `Raft` in a unit test is heavy
        // and the live-host tests live in the integration harness; the
        // _assert_signature line below also gives compile-time evidence that
        // the host's accessor exists with the right signature.
        let cell = tsoracle_openraft_toolkit::ActiveWriteVersion::default();
        let store: std::sync::Arc<dyn crate::snapshot_store::SnapshotStore> =
            std::sync::Arc::new(crate::snapshot_store::InMemorySnapshotStore::new());
        let state_machine =
            HighWaterStateMachine::with_store_and_active_version(store, cell.clone()).expect("sm");
        assert_eq!(
            state_machine.active_write_version(),
            tsoracle_openraft_toolkit::BASELINE_WRITE_VERSION
        );
        cell.set(7);
        assert_eq!(state_machine.active_write_version(), 7);
        // Compile-time guard that the host accessor exists and returns u8:
        let _assert_signature: fn(&StandaloneHost) -> u8 = StandaloneHost::active_write_version;
    }

    #[test]
    fn classify_activation_outcome_maps_success_to_ok() {
        let result = classify_activation_outcome(ApplyOutcome::FormatActivated { target: 7 }, 7);
        assert!(matches!(result, Ok(())));
    }

    #[test]
    fn classify_activation_outcome_maps_noop_to_membership_changed() {
        let result =
            classify_activation_outcome(ApplyOutcome::FormatActivationNoop { target: 7 }, 7);
        assert!(matches!(
            result,
            Err(FormatActivationError::MembershipChangedSinceGate { target: 7 })
        ));
    }

    #[test]
    fn classify_activation_outcome_maps_advanced_to_membership_changed() {
        // `Advanced` cannot legally come back from a SetFormatVersion entry,
        // but we map it defensively rather than silently claiming success.
        let result = classify_activation_outcome(ApplyOutcome::Advanced, 7);
        assert!(matches!(
            result,
            Err(FormatActivationError::MembershipChangedSinceGate { target: 7 })
        ));
    }

    #[test]
    fn classify_activation_outcome_rejects_target_mismatch() {
        // A reply naming a different target than the proposer asked for is
        // mapped to the same error — never silently claim success.
        let result = classify_activation_outcome(ApplyOutcome::FormatActivated { target: 8 }, 7);
        assert!(matches!(
            result,
            Err(FormatActivationError::MembershipChangedSinceGate { target: 7 })
        ));
    }

    #[test]
    fn membership_changed_since_gate_is_a_distinct_variant() {
        // Pin that the variant is constructible and matchable; it is the
        // operator-actionable surface for the apply-time no-op.
        let err = FormatActivationError::MembershipChangedSinceGate { target: 4 };
        assert!(matches!(
            err,
            FormatActivationError::MembershipChangedSinceGate { target: 4 }
        ));
    }

    #[test]
    fn classify_activation_outcome_maps_target_out_of_range_to_local_range_error() {
        // Apply-arm defense-in-depth surfaces a distinct outcome variant;
        // the proposer-side classifier must re-emit it as the
        // operator-facing `TargetOutOfRange` (carrying the local binary's
        // range), matching the surface the gate's short-circuit returns.
        let outcome = ApplyOutcome::FormatActivationTargetOutOfRange { target: 1 };
        let result = classify_activation_outcome(outcome, 1);
        match result {
            Err(FormatActivationError::TargetOutOfRange { target, min, max }) => {
                assert_eq!(target, 1);
                assert_eq!(min, tsoracle_openraft_toolkit::MIN_READABLE_VERSION);
                assert_eq!(max, tsoracle_openraft_toolkit::MAX_READABLE_VERSION);
            }
            other => panic!("expected TargetOutOfRange, got: {other:?}"),
        }
    }
}
