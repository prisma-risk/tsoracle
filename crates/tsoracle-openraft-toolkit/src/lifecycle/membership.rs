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
            info!("tsoracle-openraft-toolkit: membership change applied");
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
            info!("tsoracle-openraft-toolkit: learner added");
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

/// Why a learner-admission capability pre-check refused a candidate.
///
/// LATENT: nothing in this crate currently calls [`add_learner_gated`]; the
/// guard is a pure predicate ready for a future admit-path consumer (the
/// dynamic-membership admin already calls `Raft::add_learner` directly and
/// would need to be re-routed through [`add_learner_gated`] to take effect).
#[derive(Debug, Error, PartialEq, Eq)]
pub enum JoinerGateError {
    /// The candidate cannot read the cluster's active write version, so it
    /// must not be admitted as a learner: openraft replicates and
    /// snapshot-installs a learner before promotion, and those bytes are at
    /// the active write version. Admitting it would either strand the
    /// candidate or block a later activation behind an incompatible member.
    #[error(
        "candidate cannot read the cluster active write version: \
         candidate max_readable_version={candidate_max_readable_version}, \
         active_write_version={active_write_version}"
    )]
    IncompatibleCandidate {
        candidate_max_readable_version: u8,
        active_write_version: u8,
    },
}

/// Capability pre-check for learner admission.
///
/// Returns `Ok(())` when the candidate's `candidate_max_readable_version` is
/// at or above the cluster's `active_write_version`; otherwise
/// [`JoinerGateError::IncompatibleCandidate`]. Pure and driver-agnostic on
/// purpose: the caller projects the candidate's capability struct (e.g. the
/// driver's `NodeCapabilities.max_readable_version`) and the leader's active
/// write version into the two `u8` arguments, so the toolkit does not
/// depend on the driver's capability type.
pub fn learner_meets_active_write_version(
    candidate_max_readable_version: u8,
    active_write_version: u8,
) -> Result<(), JoinerGateError> {
    if candidate_max_readable_version < active_write_version {
        return Err(JoinerGateError::IncompatibleCandidate {
            candidate_max_readable_version,
            active_write_version,
        });
    }
    Ok(())
}

/// Failure of [`add_learner_gated`]: either the joiner gate refused the
/// candidate (no raft call made) or the underlying [`add_learner`] failed.
#[derive(Debug, Error)]
pub enum GatedAdmissionError {
    /// The joiner-gate capability pre-check refused the candidate; no raft
    /// membership change was attempted.
    #[error(transparent)]
    Gate(#[from] JoinerGateError),
    /// The capability check passed but the raft `add_learner` call failed.
    #[error(transparent)]
    Membership(#[from] MembershipError),
}

/// Capability-gated learner admission.
///
/// Runs [`learner_meets_active_write_version`] against the candidate's
/// `candidate_max_readable_version` and the cluster's `active_write_version`
/// FIRST; only if it passes does it delegate to [`add_learner`]. An admit
/// path that routes through this entry point refuses an incompatible
/// candidate before any replication or snapshot transfer is initiated.
///
/// LATENT: the existing admin path on this branch calls
/// `Raft::add_learner` directly, not the toolkit `add_learner`. This
/// wrapper is the seam a future admit caller can switch to.
pub async fn add_learner_gated<C, SM>(
    raft: &Raft<C, SM>,
    id: C::NodeId,
    node: C::Node,
    blocking: bool,
    candidate_max_readable_version: u8,
    active_write_version: u8,
) -> Result<(), GatedAdmissionError>
where
    C: RaftTypeConfig,
    SM: RaftStateMachine<C>,
{
    learner_meets_active_write_version(candidate_max_readable_version, active_write_version)?;
    add_learner(raft, id, node, blocking).await?;
    Ok(())
}

#[cfg(test)]
mod joiner_gate_tests {
    use super::*;

    #[test]
    fn admits_when_candidate_can_read_active_version() {
        // max_readable >= active => admitted.
        assert!(learner_meets_active_write_version(3, 3).is_ok());
        assert!(learner_meets_active_write_version(4, 3).is_ok());
    }

    #[test]
    fn refuses_when_candidate_below_active_version() {
        // Refuse a learner whose max_readable_version is below the cluster's
        // active write version — replication / snapshot transfer would hand
        // it bytes it cannot decode.
        let err = learner_meets_active_write_version(3, 4)
            .expect_err("a candidate that cannot read the active version must be refused");
        let JoinerGateError::IncompatibleCandidate {
            candidate_max_readable_version,
            active_write_version,
        } = err;
        assert_eq!(candidate_max_readable_version, 3);
        assert_eq!(active_write_version, 4);
    }

    #[test]
    fn refusal_message_is_actionable() {
        let err = learner_meets_active_write_version(2, 5).unwrap_err();
        let message = err.to_string();
        assert!(message.contains('2'), "names the candidate capability");
        assert!(message.contains('5'), "names the active write version");
    }

    #[test]
    fn gated_admission_error_unifies_gate_and_membership_failures() {
        // `add_learner_gated` can fail either because the joiner gate
        // refused the candidate (before touching raft) or because the
        // underlying `add_learner` raft call failed. `GatedAdmissionError`
        // must carry both via `#[from]`.
        let gate: GatedAdmissionError = JoinerGateError::IncompatibleCandidate {
            candidate_max_readable_version: 3,
            active_write_version: 4,
        }
        .into();
        assert!(matches!(gate, GatedAdmissionError::Gate(_)));

        let membership: GatedAdmissionError = MembershipError::NotLeader { leader: None }.into();
        assert!(matches!(membership, GatedAdmissionError::Membership(_)));
    }
}
