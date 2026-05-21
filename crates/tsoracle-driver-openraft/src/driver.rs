//! `ConsensusDriver` impl on top of any [`OpenraftHighWaterHost`].
//!
//! [`OpenraftDriver`] is a thin bridge: it owns the trait-surface boilerplate
//! and leadership-event mapping, then delegates storage/submission to the
//! supplied host. The bundled [`crate::StandaloneHost`] gives you the original
//! "owns its own raft cluster" behavior; services that already run an openraft
//! cluster implement [`OpenraftHighWaterHost`] directly against their existing
//! cluster.
//!
//! # Fencing
//!
//! `persist_high_water(_, epoch)` deliberately ignores the `epoch` argument.
//! Monotonicity is enforced inside the state machine's apply path
//! (`max(prev, at_least)`); a stale leader can still submit a write, but the
//! result is either dropped by `client_write`'s `ForwardToLeader` path or
//! absorbed by the apply-time `max`. Both outcomes preserve correctness
//! without a term-based pre-check.

use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use futures::{Stream, StreamExt};
use openraft::RaftTypeConfig;
use openraft_toolkit::LeadershipState;
use openraft_toolkit::lifecycle::leader::stream_from_receiver;
use tsoracle_consensus::{ConsensusDriver, ConsensusError, LeaderState};
use tsoracle_core::Epoch;

use crate::host::OpenraftHighWaterHost;

/// Driver bridging any [`OpenraftHighWaterHost`] to
/// [`tsoracle_consensus::ConsensusDriver`].
///
/// Owns the leadership-mapping and trait-surface boilerplate; delegates the
/// actual high-water storage to the host.
pub struct OpenraftDriver<H: OpenraftHighWaterHost> {
    host: Arc<H>,
}

impl<H: OpenraftHighWaterHost> OpenraftDriver<H> {
    /// Build a driver from a host value. The driver wraps the host in an
    /// `Arc` so the leadership stream can keep it alive independently of the
    /// outer driver handle.
    pub fn new(host: H) -> Arc<Self> {
        Arc::new(Self {
            host: Arc::new(host),
        })
    }

    /// Build a driver from a pre-shared `Arc<H>`. Useful when the host is
    /// already shared with other subsystems (e.g. a placement driver that
    /// hands the same backend to multiple gRPC services).
    pub fn from_arc(host: Arc<H>) -> Arc<Self> {
        Arc::new(Self { host })
    }
}

#[async_trait]
impl<H: OpenraftHighWaterHost> ConsensusDriver for OpenraftDriver<H> {
    /// Return a stream of [`LeaderState`] transitions.
    ///
    /// Goes through the toolkit's `stream_from_receiver` (the by-value entry
    /// point) rather than `leadership_events(&raft)` to side-step a Rust 2024
    /// lifetime-over-capture issue: the `&raft` form would require the
    /// returned stream to borrow from `raft`, but we want `'static`. The
    /// cloned host then rides along inside [`KeepAlive`] so dropping the
    /// outer driver doesn't shut the raft down.
    fn leadership_events(&self) -> Pin<Box<dyn Stream<Item = LeaderState> + Send>> {
        let host = Arc::clone(&self.host);
        Box::pin(owned_leadership_stream::<H>(host))
    }

    /// Read the durably-persisted high-water mark via the host.
    async fn load_high_water(&self) -> Result<u64, ConsensusError> {
        self.host.current_high_water().await
    }

    /// Submit a "bump to at_least" proposal via the host.
    ///
    /// The `epoch` arg is intentionally ignored: fencing is the state
    /// machine's monotonicity guarantee (`max(prev, at_least)`), not a
    /// term-based pre-check.
    async fn persist_high_water(
        &self,
        at_least: u64,
        _epoch: Epoch,
    ) -> Result<u64, ConsensusError> {
        self.host.submit_advance(at_least).await
    }
}

/// Build a `'static` `Stream<Item = LeaderState>` from an owned host handle.
///
/// The cloned `Arc<H>` rides along inside [`KeepAlive`] so the host (and the
/// raft it owns) outlives the stream's polling.
fn owned_leadership_stream<H: OpenraftHighWaterHost>(
    host: Arc<H>,
) -> impl Stream<Item = LeaderState> + Send + 'static {
    let rx = host.raft().metrics();
    let inner: Pin<Box<dyn Stream<Item = LeaderState> + Send>> =
        Box::pin(stream_from_receiver::<H::Config>(rx).map(map_leader_state::<H::Config>));
    KeepAlive { _host: host, inner }
}

/// Stream wrapper that keeps an `Arc<H>` alive for the duration of the inner
/// stream. Without this, the cloned host would drop at the end of
/// `owned_leadership_stream` and shut the raft down once the outer driver
/// lost its other reference.
struct KeepAlive<H: OpenraftHighWaterHost> {
    _host: Arc<H>,
    inner: Pin<Box<dyn Stream<Item = LeaderState> + Send>>,
}

impl<H: OpenraftHighWaterHost> Stream for KeepAlive<H> {
    type Item = LeaderState;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

/// Project a toolkit [`LeadershipState`] into a tsoracle-consensus
/// [`LeaderState`].
///
/// Always returns `leader_endpoint: None` on the follower branch: the generic
/// mapper has no way to extract an endpoint from `C::Node` (different hosts
/// pick different `Node` types). Hosts that need endpoint resolution wrap the
/// driver themselves and provide their own `ConsensusDriver` impl.
fn map_leader_state<C: RaftTypeConfig>(s: LeadershipState<C>) -> LeaderState {
    match s {
        LeadershipState::Leader { term } => LeaderState::Leader { epoch: Epoch(term) },
        LeadershipState::Follower { .. } => LeaderState::Follower {
            leader_endpoint: None,
        },
        LeadershipState::Candidate { .. }
        | LeadershipState::Learner
        | LeadershipState::Shutdown => LeaderState::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::map_leader_state;
    use crate::type_config::TypeConfig;
    use openraft_toolkit::LeadershipState;
    use tsoracle_consensus::LeaderState;
    use tsoracle_core::Epoch;

    #[test]
    fn leader_maps_to_leader_with_epoch() {
        let s = map_leader_state::<TypeConfig>(LeadershipState::Leader { term: 7 });
        assert_eq!(s, LeaderState::Leader { epoch: Epoch(7) });
    }

    #[test]
    fn follower_with_no_leader_maps_to_follower() {
        let s = map_leader_state::<TypeConfig>(LeadershipState::Follower {
            term: 3,
            leader: None,
        });
        assert_eq!(
            s,
            LeaderState::Follower {
                leader_endpoint: None
            }
        );
    }

    #[test]
    fn follower_with_known_leader_still_maps_without_endpoint() {
        let s = map_leader_state::<TypeConfig>(LeadershipState::Follower {
            term: 4,
            leader: Some((
                2u64,
                crate::type_config::OpenraftPeer {
                    addr: "ignored".into(),
                },
            )),
        });
        // The generic mapper intentionally drops endpoint info; hosts that
        // want endpoint resolution wrap the driver themselves.
        assert_eq!(
            s,
            LeaderState::Follower {
                leader_endpoint: None
            }
        );
    }

    #[test]
    fn candidate_maps_to_unknown() {
        let s = map_leader_state::<TypeConfig>(LeadershipState::Candidate { term: 5 });
        assert_eq!(s, LeaderState::Unknown);
    }

    #[test]
    fn learner_maps_to_unknown() {
        let s = map_leader_state::<TypeConfig>(LeadershipState::Learner);
        assert_eq!(s, LeaderState::Unknown);
    }

    #[test]
    fn shutdown_maps_to_unknown() {
        let s = map_leader_state::<TypeConfig>(LeadershipState::Shutdown);
        assert_eq!(s, LeaderState::Unknown);
    }
}
