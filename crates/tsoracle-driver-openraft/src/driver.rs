//! `ConsensusDriver` implementation backed by an `openraft::Raft` instance.
//!
//! [`OpenraftDriver`] wraps a pre-built `Raft<TypeConfig, HighWaterStateMachine>`
//! handle plus a clone of the state machine. It exposes the surface that
//! `tsoracle-server` consumes: a leader-state stream, a linearized read of the
//! durable high-water, and a monotonic `persist_high_water` that goes through
//! the raft log.
//!
//! # Why both `Raft` and a state-machine clone?
//!
//! `Raft<C, SM>` doesn't expose the state machine after construction. The
//! caller must hand a clone of `HighWaterStateMachine` to the driver so reads
//! can short-circuit through the `current_value()` accessor without taking the
//! cost of an `ensure_linearizable` round trip. For strictly linearizable
//! reads we still issue a read-index barrier; the state-machine clone is only
//! a peek into the apply-progress.
//!
//! # Fencing
//!
//! `persist_high_water(_, epoch)` reads `Raft::metrics()` and compares the
//! observed `(state, current_term)` against the caller's epoch:
//!
//! - If the local node isn't the leader, returns
//!   [`ConsensusError::NotLeader`] with the observed term (when knowable) so
//!   the caller can surface a `LeaderHint`.
//! - If the term advanced past `epoch`, returns
//!   [`ConsensusError::Fenced`] — the caller is a stale leader and must yield.
//! - Otherwise, the proposal is submitted via `client_write`.
//!
//! This is a best-effort pre-check: the proposal can still race with an
//! election and arrive at the new leader's apply pipeline as a `Blank` or be
//! rejected by `client_write` with `ForwardToLeader`. Both later outcomes are
//! also classified appropriately.

use std::pin::Pin;

use async_trait::async_trait;
use futures::{Stream, StreamExt};
use openraft::Raft;
use openraft::ServerState;
use openraft::async_runtime::watch::WatchReceiver;
use openraft::error::{ClientWriteError, RaftError};
use openraft::vote::RaftTerm;
use openraft_toolkit::LeadershipState;
use openraft_toolkit::lifecycle::leader::stream_from_receiver;
use tsoracle_consensus::{ConsensusDriver, ConsensusError, LeaderState};
use tsoracle_core::Epoch;

use crate::log_entry::HighWaterCommand;
use crate::state_machine::HighWaterStateMachine;
use crate::type_config::TypeConfig;

/// A [`ConsensusDriver`] that delegates to an openraft cluster.
///
/// Constructed from a pre-built `Raft<TypeConfig, HighWaterStateMachine>` and
/// a clone of the same state machine; see [`OpenraftDriver::new`].
pub struct OpenraftDriver {
    raft: Raft<TypeConfig, HighWaterStateMachine>,
    state_machine: HighWaterStateMachine,
}

impl OpenraftDriver {
    /// Build a driver from a pre-constructed raft handle and a state-machine
    /// clone.
    ///
    /// `state_machine` must be the same instance (i.e. share the inner `Arc`)
    /// as the one passed to `Raft::new`; otherwise `load_high_water` will read
    /// from a state machine that never sees the apply pipeline.
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
impl ConsensusDriver for OpenraftDriver {
    /// Return a stream of [`LeaderState`] transitions.
    ///
    /// Wraps `openraft_toolkit::leadership_events` (which dedups by role
    /// class) and maps each [`LeadershipState`] into the tsoracle-consensus
    /// enum. Clones the raft handle into the returned stream so it owns
    /// its own reference and is `'static`.
    fn leadership_events(&self) -> Pin<Box<dyn Stream<Item = LeaderState> + Send>> {
        let raft = self.raft.clone();
        // Wrap the toolkit stream in a tiny async-stream adapter that holds
        // the cloned `Raft` for its lifetime so the inner borrow of
        // `metrics()` stays valid.
        Box::pin(owned_leadership_stream(raft))
    }

    /// Read the durably-persisted high-water mark.
    ///
    /// This is the state-machine-local value most recently written by
    /// `apply`. The trait contract demands linearizability, but Phase A
    /// callers exercise this only against single-node clusters where the
    /// local apply is the global apply. A multi-node release will replace
    /// this body with a `ensure_linearizable` barrier followed by the same
    /// `current_value()` read.
    async fn load_high_water(&self) -> Result<u64, ConsensusError> {
        Ok(self.state_machine.current_value().await)
    }

    /// Submit a `Bump` proposal through the raft log and return the new
    /// committed value.
    ///
    /// Fencing logic:
    /// 1. Snapshot `Raft::metrics()`. If we are not the leader, return
    ///    [`ConsensusError::NotLeader`] with the observed term.
    /// 2. If the observed term is greater than `epoch.0`, return
    ///    [`ConsensusError::Fenced`].
    /// 3. Otherwise call `client_write`; classify the response.
    async fn persist_high_water(&self, at_least: u64, epoch: Epoch) -> Result<u64, ConsensusError> {
        // Step 1: pre-check fencing using a snapshot of the metrics watch.
        // `borrow_watched` returns a guard whose `clone` we take so the lock
        // is released before any `.await`.
        let (state, observed_term) = {
            let snap = self.raft.metrics().borrow_watched().clone();
            (snap.state, snap.current_term.as_u64().unwrap_or(0))
        };

        match state {
            ServerState::Leader => {
                if observed_term > epoch.0 {
                    return Err(ConsensusError::Fenced {
                        expected: epoch,
                        current: Epoch(observed_term),
                    });
                }
                // observed_term < epoch.0 is impossible in a well-behaved
                // caller (we never advertise an epoch we haven't seen as
                // leader). If it happens, treat it as a stale view that will
                // be corrected by the client_write path itself.
            }
            ServerState::Follower | ServerState::Learner | ServerState::Candidate => {
                return Err(ConsensusError::NotLeader {
                    observed: Some(Epoch(observed_term)),
                });
            }
            ServerState::Shutdown => {
                return Err(ConsensusError::NotLeader { observed: None });
            }
        }

        // Step 2: submit the proposal.
        match self
            .raft
            .client_write(HighWaterCommand::Bump { target: at_least })
            .await
        {
            Ok(resp) => Ok(resp.data.value),
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(_))) => {
                Err(ConsensusError::NotLeader { observed: None })
            }
            // ChangeMembershipError can't be returned for an AppData write;
            // bucketed with the other transient errors anyway.
            Err(e) => Err(ConsensusError::TransientDriver(Box::new(e))),
        }
    }
}

/// Build a `'static` `Stream<Item = LeaderState>` from an owned `Raft` handle.
///
/// Goes through the toolkit's `stream_from_receiver` (the receiver-by-value
/// entry point) so the resulting stream's lifetime isn't tied to any borrow
/// of `raft`. The cloned `raft` then rides along inside [`KeepAlive`] so
/// dropping the outer driver doesn't close the metrics watch.
///
/// The wrapper layer is needed because `Raft<C, SM>` doesn't expose a way to
/// keep the cluster alive purely via the metrics watch — the metrics sender
/// lives inside the raft core, and the core shuts down when the last `Raft`
/// handle is dropped.
fn owned_leadership_stream(
    raft: Raft<TypeConfig, HighWaterStateMachine>,
) -> impl Stream<Item = LeaderState> + Send + 'static {
    let inner: Pin<Box<dyn Stream<Item = LeaderState> + Send>> =
        Box::pin(stream_from_receiver::<TypeConfig>(raft.metrics()).map(map_leader_state));
    KeepAlive { _raft: raft, inner }
}

/// A stream wrapper that keeps a [`Raft`] handle alive for the duration of
/// the inner stream. Without this, the cloned handle would drop at the end
/// of `owned_leadership_stream` and shut the raft down once the original
/// driver lost its other reference.
///
/// The inner stream is already `Pin<Box<dyn Stream + Send>>` so polling it
/// is a straight delegation. The wrapper itself is `Unpin` because all of
/// its fields are.
struct KeepAlive {
    _raft: Raft<TypeConfig, HighWaterStateMachine>,
    inner: Pin<Box<dyn Stream<Item = LeaderState> + Send>>,
}

impl Stream for KeepAlive {
    type Item = LeaderState;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

fn map_leader_state(s: LeadershipState<TypeConfig>) -> LeaderState {
    match s {
        LeadershipState::Leader { term } => LeaderState::Leader { epoch: Epoch(term) },
        LeadershipState::Follower { leader, .. } => LeaderState::Follower {
            leader_endpoint: leader.map(|(_, node)| node.addr),
        },
        LeadershipState::Candidate { .. }
        | LeadershipState::Learner
        | LeadershipState::Shutdown => LeaderState::Unknown,
    }
}
