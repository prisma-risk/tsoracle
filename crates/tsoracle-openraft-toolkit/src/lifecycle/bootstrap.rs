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

//! Cluster bootstrap helpers.
//!
//! Coverage note: excluded from `make coverage` because exercising this
//! wrapper requires a live raft, which the toolkit's own tests deliberately
//! don't stand up. Downstream consumers' integration tests carry the real
//! coverage; the compile-time signature shim in `tests/lifecycle.rs` is what
//! catches openraft API drift inside this file.
//!
//! A node coming up as a voter needs one of two startup modes:
//!
//! - **Fresh**: first-time start; the caller knows the initial voter set.
//! - **Reopen**: restart against existing on-disk state; do nothing special.
//!
//! [`bootstrap`] folds these into a single async call, mapping openraft's
//! `InitializeError::NotAllowed` (raised when the raft already has log/snapshot
//! state) to success under `Reopen` and to a clear `BootstrapError` under
//! `Fresh`.
//!
//! A node joining an existing cluster as a learner uses neither mode: it does
//! no local initialization, so it calls [`join`] (a documented no-op on the
//! joining side) and waits for the leader to register it via
//! [`add_learner`](super::membership::add_learner).

use std::collections::BTreeMap;

use openraft::error::{InitializeError, RaftError};
use openraft::storage::RaftStateMachine;
use openraft::{Raft, RaftTypeConfig};
use thiserror::Error;
use tracing::info;

/// Startup mode for [`bootstrap`].
///
/// A learner joining an existing cluster is intentionally **not** a variant
/// here: the joining node does no local initialization, so it calls [`join`]
/// and waits for the leader to register it via
/// [`add_learner`](super::membership::add_learner). Keeping it out of this enum
/// means every arm performs a distinct local action — there is no mode whose
/// only difference from another is the log line it emits.
#[derive(Debug)]
pub enum BootstrapMode<C: RaftTypeConfig> {
    /// First-time start: initialize with the given voter set.
    Fresh {
        initial_members: BTreeMap<C::NodeId, C::Node>,
    },
    /// Restart against existing on-disk state.
    Reopen,
}

/// Failure modes for [`bootstrap`].
#[derive(Debug, Error)]
pub enum BootstrapError {
    /// `Fresh` mode was requested but openraft reported the log already has
    /// membership entries (`InitializeError::NotAllowed`). Either the caller
    /// should have used `Reopen` or the on-disk state is unexpected.
    #[error("expected fresh cluster but raft is already initialized")]
    UnexpectedExistingState,
    /// `Raft::initialize` returned an error other than "already initialized".
    #[error("initialize failed: {0}")]
    Initialize(String),
}

/// Bring a raft instance to a serviceable state without duplicating the
/// fresh/reopen branching in every consumer.
///
/// - `Fresh { initial_members }`: calls `raft.initialize(initial_members)`.
///   `InitializeError::NotAllowed` (the log already has membership) is
///   translated to [`BootstrapError::UnexpectedExistingState`]; other errors
///   surface as [`BootstrapError::Initialize`].
/// - `Reopen`: does not touch the raft. The caller is expected to have opened
///   existing storage; openraft replays the log on its own.
///
/// A node joining as a learner does not call [`bootstrap`] at all — see
/// [`join`].
pub async fn bootstrap<C, SM>(
    raft: &Raft<C, SM>,
    mode: BootstrapMode<C>,
) -> Result<(), BootstrapError>
where
    C: RaftTypeConfig,
    SM: RaftStateMachine<C>,
{
    match mode {
        BootstrapMode::Fresh { initial_members } => match raft.initialize(initial_members).await {
            Ok(()) => {
                info!("tsoracle-openraft-toolkit: raft initialized with fresh membership");
                Ok(())
            }
            Err(RaftError::APIError(InitializeError::NotAllowed(_))) => {
                Err(BootstrapError::UnexpectedExistingState)
            }
            Err(e) => Err(BootstrapError::Initialize(e.to_string())),
        },
        BootstrapMode::Reopen => {
            info!("tsoracle-openraft-toolkit: reopen mode; relying on existing raft state");
            Ok(())
        }
    }
}

/// Record that this node is joining an existing cluster as a learner.
///
/// This is a deliberate no-op on the joining node and never touches the raft:
/// a learner must not initialize its own membership — doing so would fork the
/// cluster into two single-node clusters. The node simply comes up against
/// empty or replayed storage and sits idle until it is promoted.
///
/// The join itself is driven from the **leader**, which registers this node by
/// calling [`add_learner`](super::membership::add_learner). Call [`join`] only
/// to record the startup intent in the logs; use [`bootstrap`] with
/// [`BootstrapMode::Fresh`] or [`BootstrapMode::Reopen`] for the voter paths.
pub fn join() {
    info!(
        "tsoracle-openraft-toolkit: join mode; node will sit as learner until the leader calls add_learner"
    );
}
