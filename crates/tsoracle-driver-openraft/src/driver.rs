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

// #[PerformanceCriticalPath]
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
use tsoracle_consensus::{ConsensusDriver, ConsensusError, LeaderState};
use tsoracle_core::Epoch;
use tsoracle_openraft_toolkit::DENSE_WRITE_VERSION;
use tsoracle_openraft_toolkit::LeadershipState;
use tsoracle_openraft_toolkit::leadership_events_from_metrics;

use crate::host::OpenraftHighWaterHost;
use crate::type_config::ServiceEndpoint;

/// Driver bridging any [`OpenraftHighWaterHost`] to
/// [`tsoracle_consensus::ConsensusDriver`].
///
/// Owns the leadership-mapping and trait-surface boilerplate; delegates the
/// actual high-water storage to the host. The leader's tsoracle service
/// endpoint is read directly from the membership node via the
/// [`ServiceEndpoint`] trait; no static peer map is required.
pub struct OpenraftDriver<H: OpenraftHighWaterHost> {
    host: Arc<H>,
}

impl<H: OpenraftHighWaterHost> OpenraftDriver<H> {
    /// Build a driver from a host value.
    pub fn new(host: H) -> Arc<Self> {
        Arc::new(Self {
            host: Arc::new(host),
        })
    }

    /// Build a driver from a pre-shared `Arc<H>`.
    pub fn from_arc(host: Arc<H>) -> Arc<Self> {
        Arc::new(Self { host })
    }
}

#[async_trait]
impl<H: OpenraftHighWaterHost> ConsensusDriver for OpenraftDriver<H>
where
    <H::Config as RaftTypeConfig>::Node: ServiceEndpoint,
{
    /// Return a stream of [`LeaderState`] transitions.
    ///
    /// Goes through the toolkit's `leadership_events_from_metrics` (the
    /// by-value entry point) rather than `leadership_events(&raft)` to
    /// side-step a Rust 2024 lifetime-over-capture issue: the `&raft` form
    /// would require the returned stream to borrow from `raft`, but we want
    /// `'static`. The cloned host then rides along inside [`KeepAlive`] so
    /// dropping the outer driver doesn't shut the raft down.
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
        // Reject an out-of-range value before it enters the replicated log:
        // the apply path computes an unchecked `max(prev, at_least)`, so a
        // committed poison value can never be served and cannot self-heal.
        tsoracle_consensus::reject_out_of_range_advance(at_least)?;
        self.host.submit_advance(at_least).await
    }

    /// Advance the dense counter for `key` by `count`, gated on format
    /// activation.
    ///
    /// The `_expected_epoch` arg is intentionally ignored, mirroring
    /// `persist_high_water`: openraft's leadership fence rejects a stale
    /// leader's `client_write`, and the per-key fetch-add is linearized by the
    /// committed log.
    async fn advance_dense(
        &self,
        key: &tsoracle_core::SeqKey,
        count: u32,
        _expected_epoch: Epoch,
    ) -> Result<u64, ConsensusError> {
        // Rollout gate: refuse until the dense format is active cluster-wide.
        // Appending an `AdvanceDense` entry before activation could hand an
        // un-upgraded follower an entry it cannot decode.
        let active = self.host.active_write_version();
        if active < DENSE_WRITE_VERSION {
            return Err(ConsensusError::DenseNotActivated {
                required: DENSE_WRITE_VERSION,
                active,
            });
        }
        self.host.submit_advance_dense(key, count).await
    }

    /// Read a key's committed dense counter (0 if absent), linearized.
    async fn load_dense_seq(&self, key: &tsoracle_core::SeqKey) -> Result<u64, ConsensusError> {
        self.host.current_dense_seq(key).await
    }
}

/// Build a `'static` `Stream<Item = LeaderState>` from an owned host handle.
///
/// The cloned `Arc<H>` rides along inside [`KeepAlive`] so the host (and the
/// raft it owns) outlives the stream's polling.
fn owned_leadership_stream<H: OpenraftHighWaterHost>(
    host: Arc<H>,
) -> impl Stream<Item = LeaderState> + Send + 'static
where
    <H::Config as RaftTypeConfig>::Node: ServiceEndpoint,
{
    let rx = host.metrics();
    let inner: Pin<Box<dyn Stream<Item = LeaderState> + Send>> = Box::pin(
        leadership_events_from_metrics::<H::Config>(rx).map(map_leader_state::<H::Config>),
    );
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
/// On the follower branch, `leader_epoch` is the follower's term (equal to the
/// leader's term once a leader is accepted) and `leader_endpoint` is read
/// directly from the leader's membership node via [`ServiceEndpoint::service_endpoint`].
/// A node with an empty service endpoint, or the absence of a known leader,
/// yields `leader_endpoint: None`.
fn map_leader_state<C: RaftTypeConfig>(s: LeadershipState<C>) -> LeaderState
where
    C::Node: ServiceEndpoint,
{
    match s {
        LeadershipState::Leader { term } => LeaderState::Leader {
            epoch: Epoch(u128::from(term)),
        },
        LeadershipState::Follower { term, leader } => LeaderState::Follower {
            leader_endpoint: leader.and_then(|(_id, node)| node.service_endpoint()),
            leader_epoch: Some(Epoch(u128::from(term))),
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
    use tsoracle_consensus::LeaderState;
    use tsoracle_core::{Epoch, PeerEndpoint};
    use tsoracle_openraft_toolkit::LeadershipState;

    #[test]
    fn follower_resolves_endpoint_from_node_service_endpoint() {
        let state = map_leader_state::<TypeConfig>(LeadershipState::Follower {
            term: 4,
            leader: Some((
                2u64,
                crate::type_config::OpenraftPeer {
                    addr: "node-2:50052".into(),
                    service_endpoint: "node-2:50051".into(),
                    admin_endpoint: String::new(),
                },
            )),
        });
        assert_eq!(
            state,
            LeaderState::Follower {
                leader_endpoint: Some(PeerEndpoint::try_from("node-2:50051").unwrap()),
                leader_epoch: Some(Epoch(4)),
            }
        );
    }

    #[test]
    fn follower_with_endpointless_node_has_no_endpoint() {
        let state = map_leader_state::<TypeConfig>(LeadershipState::Follower {
            term: 5,
            leader: Some((
                2u64,
                crate::type_config::OpenraftPeer {
                    addr: "node-2:50052".into(),
                    service_endpoint: String::new(),
                    admin_endpoint: String::new(),
                },
            )),
        });
        assert_eq!(
            state,
            LeaderState::Follower {
                leader_endpoint: None,
                leader_epoch: Some(Epoch(5)),
            }
        );
    }

    #[test]
    fn follower_with_no_leader_maps_to_follower_with_epoch() {
        let state = map_leader_state::<TypeConfig>(LeadershipState::Follower {
            term: 3,
            leader: None,
        });
        assert_eq!(
            state,
            LeaderState::Follower {
                leader_endpoint: None,
                leader_epoch: Some(Epoch(3)),
            }
        );
    }

    #[test]
    fn leader_maps_to_leader_with_epoch() {
        let s = map_leader_state::<TypeConfig>(LeadershipState::Leader { term: 7 });
        assert_eq!(s, LeaderState::Leader { epoch: Epoch(7) });
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

    /// A host whose `submit_advance` echoes the requested value back as the
    /// persisted high-water. It owns only a metrics watch (the piggy-back
    /// shape), so it boots without a raft cluster. The echo lets a
    /// `persist_high_water` test distinguish "the range guard rejected the
    /// value before submission" from "the value was submitted and persisted":
    /// an out-of-range value that reaches `submit_advance` comes back as
    /// `Ok(at_least)`, so an `Err` proves the guard fired first.
    struct EchoHost {
        rx: openraft::type_config::alias::WatchReceiverOf<
            TypeConfig,
            openraft::RaftMetrics<TypeConfig>,
        >,
    }

    #[async_trait::async_trait]
    impl crate::host::OpenraftHighWaterHost for EchoHost {
        type Config = TypeConfig;

        fn metrics(
            &self,
        ) -> openraft::type_config::alias::WatchReceiverOf<
            Self::Config,
            openraft::RaftMetrics<Self::Config>,
        > {
            self.rx.clone()
        }

        async fn current_high_water(&self) -> Result<u64, tsoracle_consensus::ConsensusError> {
            Ok(0)
        }

        async fn submit_advance(
            &self,
            at_least: u64,
        ) -> Result<u64, tsoracle_consensus::ConsensusError> {
            Ok(at_least)
        }

        fn active_write_version(&self) -> u8 {
            tsoracle_openraft_toolkit::BASELINE_WRITE_VERSION
        }

        async fn submit_advance_dense(
            &self,
            _key: &tsoracle_core::SeqKey,
            _count: u32,
        ) -> Result<u64, tsoracle_consensus::ConsensusError> {
            Err(tsoracle_consensus::ConsensusError::DenseUnsupported)
        }

        async fn current_dense_seq(
            &self,
            _key: &tsoracle_core::SeqKey,
        ) -> Result<u64, tsoracle_consensus::ConsensusError> {
            Err(tsoracle_consensus::ConsensusError::DenseUnsupported)
        }
    }

    fn echo_driver() -> std::sync::Arc<super::OpenraftDriver<EchoHost>> {
        use openraft::type_config::TypeConfigExt;
        let metrics = openraft::RaftMetrics::<TypeConfig>::new_initial(1u64);
        let (_tx, rx) = <TypeConfig as TypeConfigExt>::watch_channel(metrics);
        super::OpenraftDriver::new(EchoHost { rx })
    }

    #[tokio::test]
    async fn persist_high_water_rejects_out_of_range_before_submit() {
        use tsoracle_consensus::ConsensusDriver;
        use tsoracle_core::PHYSICAL_MS_MAX;

        let driver = echo_driver();

        // Out-of-range: the guard must reject *before* the value reaches the
        // host's log, so the poison is never durably committed.
        let err = driver
            .persist_high_water(PHYSICAL_MS_MAX + 1, Epoch::ZERO)
            .await
            .expect_err("an out-of-range advance must be rejected, not persisted");
        assert!(
            matches!(err, tsoracle_consensus::ConsensusError::AdvanceOutOfRange(at_least) if at_least == PHYSICAL_MS_MAX + 1),
            "out-of-range advance must surface as AdvanceOutOfRange carrying the offending value, got {err:?}"
        );

        // The boundary value is in range and still delegates to the host,
        // which echoes it back — the guard rejects only what exceeds the cap.
        assert_eq!(
            driver
                .persist_high_water(PHYSICAL_MS_MAX, Epoch::ZERO)
                .await
                .expect("the maximum in-range value must persist"),
            PHYSICAL_MS_MAX
        );
    }
}
