//! Bundled host: owns its own raft cluster and the [`HighWaterStateMachine`].
//!
//! Use this when you want a dedicated TSO raft, separate from any other
//! consensus state your service runs. For services that already run an
//! openraft cluster (e.g. a placement driver), implement
//! [`OpenraftHighWaterHost`](crate::OpenraftHighWaterHost) directly against
//! that existing cluster instead.

use async_trait::async_trait;
use openraft::Raft;
use openraft::error::{ClientWriteError, RaftError};
use tsoracle_consensus::ConsensusError;

use crate::host::OpenraftHighWaterHost;
use crate::log_entry::HighWaterCommand;
use crate::state_machine::HighWaterStateMachine;
use crate::type_config::TypeConfig;

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
}

#[async_trait]
impl OpenraftHighWaterHost for StandaloneHost {
    type Config = TypeConfig;
    type StateMachine = HighWaterStateMachine;

    fn raft(&self) -> &Raft<Self::Config, Self::StateMachine> {
        &self.raft
    }

    async fn current_high_water(&self) -> Result<u64, ConsensusError> {
        // The standalone host doesn't issue a linearizable barrier here for the
        // same reason the original driver didn't: a single-voter cluster has
        // trivially linearizable local reads, and multi-node deployments will
        // gain a barrier in a follow-up alongside the network impl.
        Ok(self.state_machine.current_value().await)
    }

    async fn submit_advance(&self, at_least: u64) -> Result<u64, ConsensusError> {
        match self
            .raft
            .client_write(HighWaterCommand::Bump { target: at_least })
            .await
        {
            Ok(resp) => Ok(resp.data.value),
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(_))) => {
                Err(ConsensusError::NotLeader { observed: None })
            }
            Err(e) => Err(ConsensusError::TransientDriver(Box::new(e))),
        }
    }
}
