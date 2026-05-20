//! Cluster bootstrap helpers.
//!
//! Most raft consumers need one of three startup modes:
//!
//! - **Fresh**: first-time start; the caller knows the initial voter set.
//! - **Reopen**: restart against existing on-disk state; do nothing special.
//! - **Join**: start as a learner; an orchestrator will promote later.
//!
//! [`bootstrap`] folds these into a single async call, mapping openraft's
//! `InitializeError::NotAllowed` (raised when the raft already has log/snapshot
//! state) to success under `Reopen` and to a clear `BootstrapError` under
//! `Fresh`.

use std::collections::BTreeMap;

use openraft::error::{InitializeError, RaftError};
use openraft::storage::RaftStateMachine;
use openraft::{Raft, RaftTypeConfig};
use thiserror::Error;
use tracing::{info, warn};

/// Startup mode for [`bootstrap`].
#[derive(Debug)]
pub enum BootstrapMode<C: RaftTypeConfig> {
    /// First-time start: initialize with the given voter set.
    Fresh {
        initial_members: BTreeMap<C::NodeId, C::Node>,
    },
    /// Restart against existing on-disk state.
    Reopen,
    /// Start as a learner; orchestrator will promote later.
    Join,
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
/// - `Join`: does not touch the raft. The node will sit as a learner until an
///   orchestrator promotes it via membership change.
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
                info!("openraft-toolkit: raft initialized with fresh membership");
                Ok(())
            }
            Err(RaftError::APIError(InitializeError::NotAllowed(_))) => {
                Err(BootstrapError::UnexpectedExistingState)
            }
            Err(e) => Err(BootstrapError::Initialize(e.to_string())),
        },
        BootstrapMode::Reopen => {
            info!("openraft-toolkit: reopen mode; relying on existing raft state");
            Ok(())
        }
        BootstrapMode::Join => {
            warn!("openraft-toolkit: join mode; node will sit as learner until promoted");
            Ok(())
        }
    }
}
