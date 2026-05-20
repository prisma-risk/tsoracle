//! `ConsensusDriver` implementation backed by an `openraft::Raft` instance.
//!
//! `OpenraftDriver` wraps three pieces of shared state:
//!  - the `Raft` handle for submitting log entries and issuing read barriers,
//!  - an `Arc<RwLock<AppliedState>>` for reading the committed high-water after a
//!    barrier, and
//!  - a `watch::Receiver<LeaderState>` populated by the leader-watch task (Task 11).

use core::pin::Pin;

use futures::{Stream, StreamExt};
use openraft::error::{ClientWriteError, RaftError};
use openraft::{Raft, ReadPolicy};
use std::sync::Arc;
use tokio::sync::{RwLock, watch};
use tokio_stream::wrappers::WatchStream;
use tsoracle_consensus::{ConsensusDriver, ConsensusError, LeaderState};
use tsoracle_core::Epoch;

use crate::store::AppliedState;
use crate::types::{TsoExtend, TypeConfig};

// ---------------------------------------------------------------------------
// Struct
// ---------------------------------------------------------------------------

/// A `ConsensusDriver` that delegates to an openraft cluster.
///
/// The `SM` type parameter is the state-machine implementation passed to
/// `Raft::new`. All methods used here are defined on `Raft<C, SM>` without
/// any bound on `SM`, so this struct remains storage-agnostic.
pub struct OpenraftDriver<SM> {
    /// The openraft handle — used for writes and read barriers.
    pub raft: Raft<TypeConfig, SM>,
    /// Shared handle into the state machine; read after a linearizable barrier.
    pub state: Arc<RwLock<AppliedState>>,
    /// Leadership transition stream, populated by the leader-watch task (Task 11).
    pub leader_events: watch::Receiver<LeaderState>,
}

// ---------------------------------------------------------------------------
// ConsensusDriver impl
// ---------------------------------------------------------------------------

#[async_trait::async_trait]
impl<SM: Send + Sync + 'static> ConsensusDriver for OpenraftDriver<SM> {
    /// Return a stream of leadership transitions.
    ///
    /// We clone the watch receiver so that the stream can be owned independently
    /// of `self`. `WatchStream` emits the current value immediately and then
    /// every subsequent change.
    fn leadership_events(&self) -> Pin<Box<dyn Stream<Item = LeaderState> + Send>> {
        Box::pin(WatchStream::new(self.leader_events.clone()).boxed())
    }

    /// Read the durably-persisted high-water mark with linearizable semantics.
    ///
    /// We issue a read-index barrier (`ReadPolicy::ReadIndex`) so that openraft
    /// confirms we are still the leader and that the state machine has applied
    /// all entries up to the barrier log id. Only after that barrier passes do
    /// we read `state.high_water` — guaranteeing we see all prior committed
    /// writes.
    async fn load_high_water(&self) -> Result<u64, ConsensusError> {
        // `ensure_linearizable` returns Ok(Some(log_id)) on success, or an
        // error if we are not the leader / quorum is unavailable.
        // Raft linearizability barrier failure typically means transient
        // quorum loss or a leadership change in progress — both classify as
        // transient (the client should retry).
        self.raft
            .ensure_linearizable(ReadPolicy::ReadIndex)
            .await
            .map_err(|e| ConsensusError::TransientDriver(Box::new(e)))?;

        // Barrier passed — now safe to read the state machine directly.
        Ok(self.state.read().await.high_water)
    }

    /// Durably advance the high-water mark to at least `at_least`.
    ///
    /// We write a `TsoExtend` entry through the Raft log. The state machine
    /// applies `max(prev_high_water, at_least)`, so this call is idempotent and
    /// monotone even under retries.
    ///
    /// If this node is not the current leader, openraft returns
    /// `ClientWriteError::ForwardToLeader`, which we translate to
    /// `ConsensusError::NotLeader`.
    async fn persist_high_water(&self, at_least: u64, epoch: Epoch) -> Result<u64, ConsensusError> {
        let req = TsoExtend {
            at_least,
            epoch: epoch.0,
        };

        match self.raft.client_write(req).await {
            Ok(resp) => Ok(resp.data.persisted),

            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(_))) => {
                Err(ConsensusError::NotLeader { observed: None })
            }

            // Any other raft error (network partition, slow replica, write
            // timeout) is transient from the caller's perspective — quorum
            // may form on retry.
            Err(e) => Err(ConsensusError::TransientDriver(Box::new(e))),
        }
    }
}
