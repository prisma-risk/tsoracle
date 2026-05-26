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
//! [`OpenraftHighWaterHost`](crate::OpenraftHighWaterHost) directly against
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
};
use crate::host::OpenraftHighWaterHost;
use crate::log_entry::HighWaterCommand;
use crate::state_machine::HighWaterStateMachine;
use crate::type_config::{OpenraftPeer, TypeConfig};
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
        let gathered = self.gather_member_capabilities(source).await?;
        match all_members_can_read(target, &gathered) {
            Some(gated_members) => Ok(gated_members),
            None => {
                let incapable = gathered
                    .iter()
                    .filter(|(_, capabilities)| capabilities.max_readable_version < target)
                    .map(|(node_id, capabilities)| (*node_id, capabilities.max_readable_version))
                    .collect();
                Err(FormatActivationError::MembersBelowTarget { target, incapable })
            }
        }
    }

    /// Operator entry point to begin a format-version activation to `target`
    /// (interim, library-only). Runs the all-members gate and returns
    /// `Ok(())` when it passes. A later phase fills the body between the gate
    /// and this return — it takes the `gated_members` set returned by
    /// [`run_activation_gate`](Self::run_activation_gate) and proposes a
    /// `SetFormatVersion { target, gated_members }` entry at the pre-bump
    /// active write version via `self.raft.client_write(...)`, then keys the
    /// write-version flip off the successful apply. The function boundary
    /// `run_activation_gate(target) -> Ok(gated_members)` is the exact seam
    /// that proposal step builds on; today the gate result is simply not yet
    /// consumed.
    pub async fn initiate_format_activation<S>(
        &self,
        target: u8,
        source: &S,
    ) -> Result<(), FormatActivationError>
    where
        S: CapabilitySource<Node = OpenraftPeer>,
    {
        let _gated_members = self.run_activation_gate(target, source).await?;
        Ok(())
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
}
