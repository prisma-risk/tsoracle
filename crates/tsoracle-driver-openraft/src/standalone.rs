//
//  ░▀█▀░█▀▀░█▀█░█▀▄░█▀█░█▀▀░█░░░█▀▀
//  ░░█░░▀▀█░█░█░█▀▄░█▀█░█░░░█░░░█▀▀
//  ░░▀░░▀▀▀░▀▀▀░▀░▀░▀░▀░▀▀▀░▀▀▀░▀▀▀
//
//  tsoracle — Distributed Timestamp Oracle
//
//  Copyright (c) 2026 Prisma Risk
//  Licensed under the Apache License, Version 2.0
//  https://github.com/prisma-risk/tsoracle
//

//! Bundled host: owns its own raft cluster and the [`HighWaterStateMachine`].
//!
//! Use this when you want a dedicated TSO raft, separate from any other
//! consensus state your service runs. For services that already run an
//! openraft cluster (e.g. a placement driver), implement
//! [`OpenraftHighWaterHost`](crate::OpenraftHighWaterHost) directly against
//! that existing cluster instead.

use async_trait::async_trait;
use openraft::Raft;
use openraft::ReadPolicy;
use openraft::error::{ClientWriteError, LinearizableReadError, RaftError};
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
            .client_write(HighWaterCommand::Bump { target: at_least })
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
    use super::*;
    use openraft::error::Fatal;

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
}
