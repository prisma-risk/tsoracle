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

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use futures::{Stream, StreamExt};
use openraft::RaftTypeConfig;
use tsoracle_consensus::{ConsensusDriver, ConsensusError, LeaderState};
use tsoracle_core::{Epoch, TsoPeer};
use tsoracle_openraft_toolkit::LeadershipState;
use tsoracle_openraft_toolkit::leadership_events_from_metrics;

use crate::host::OpenraftHighWaterHost;

/// Driver bridging any [`OpenraftHighWaterHost`] to
/// [`tsoracle_consensus::ConsensusDriver`].
///
/// Owns the leadership-mapping and trait-surface boilerplate; delegates the
/// actual high-water storage to the host.
pub struct OpenraftDriver<H: OpenraftHighWaterHost> {
    host: Arc<H>,
    /// Maps a peer's raft `NodeId` to its advertised tsoracle service address.
    /// Used to populate `LeaderState::Follower::leader_endpoint` so clients can
    /// redirect. Empty means "no endpoint resolution" (the bare-driver case).
    peers: Arc<HashMap<<H::Config as RaftTypeConfig>::NodeId, String>>,
}

impl<H: OpenraftHighWaterHost> OpenraftDriver<H> {
    /// Build a driver from a host value with no endpoint resolution. Follower
    /// hints carry `leader_endpoint: None`; use [`Self::with_peers`] to resolve.
    pub fn new(host: H) -> Arc<Self> {
        Arc::new(Self {
            host: Arc::new(host),
            peers: Arc::new(HashMap::new()),
        })
    }

    /// Build a driver from a host value and the cluster's [`TsoPeer`] topology.
    /// The peers are consulted on the follower branch to resolve the elected
    /// leader's advertised endpoint for `LeaderHint` redirects.
    pub fn with_peers(host: H, peers: impl IntoIterator<Item = TsoPeer>) -> Arc<Self>
    where
        <H::Config as RaftTypeConfig>::NodeId: From<u64>,
    {
        Arc::new(Self {
            host: Arc::new(host),
            peers: Arc::new(endpoint_map_from_peers::<H::Config>(peers)),
        })
    }

    /// Build a driver from a pre-shared `Arc<H>` with no endpoint resolution.
    pub fn from_arc(host: Arc<H>) -> Arc<Self> {
        Arc::new(Self {
            host,
            peers: Arc::new(HashMap::new()),
        })
    }

    /// Build a driver from a pre-shared `Arc<H>` and the cluster's [`TsoPeer`]
    /// topology.
    pub fn from_arc_with_peers(host: Arc<H>, peers: impl IntoIterator<Item = TsoPeer>) -> Arc<Self>
    where
        <H::Config as RaftTypeConfig>::NodeId: From<u64>,
    {
        Arc::new(Self {
            host,
            peers: Arc::new(endpoint_map_from_peers::<H::Config>(peers)),
        })
    }
}

#[async_trait]
impl<H: OpenraftHighWaterHost> ConsensusDriver for OpenraftDriver<H> {
    /// Return a stream of [`LeaderState`] transitions.
    ///
    /// Goes through the toolkit's `leadership_events_from_metrics` (the
    /// by-value entry point) rather than `leadership_events(&raft)` to
    /// side-step a Rust 2024
    /// lifetime-over-capture issue: the `&raft` form would require the
    /// returned stream to borrow from `raft`, but we want `'static`. The
    /// cloned host then rides along inside [`KeepAlive`] so dropping the
    /// outer driver doesn't shut the raft down.
    fn leadership_events(&self) -> Pin<Box<dyn Stream<Item = LeaderState> + Send>> {
        let host = Arc::clone(&self.host);
        let peers = Arc::clone(&self.peers);
        Box::pin(owned_leadership_stream::<H>(host, peers))
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
}

/// Build a `'static` `Stream<Item = LeaderState>` from an owned host handle.
///
/// The cloned `Arc<H>` rides along inside [`KeepAlive`] so the host (and the
/// raft it owns) outlives the stream's polling.
fn owned_leadership_stream<H: OpenraftHighWaterHost>(
    host: Arc<H>,
    peers: Arc<HashMap<<H::Config as RaftTypeConfig>::NodeId, String>>,
) -> impl Stream<Item = LeaderState> + Send + 'static {
    let rx = host.metrics();
    let inner: Pin<Box<dyn Stream<Item = LeaderState> + Send>> = Box::pin(
        leadership_events_from_metrics::<H::Config>(rx)
            .map(move |state| map_leader_state::<H::Config>(state, peers.as_ref())),
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

/// Build a `NodeId -> tsoracle-service-endpoint` map from a [`TsoPeer`] list.
///
/// Each peer's `node_id` (a `u64`) is widened into the config's `NodeId` via
/// `From<u64>` — the identity for every real config, which keys nodes by `u64`.
/// The resulting map is what the follower branch consults to resolve the
/// elected leader's advertised endpoint for `LeaderHint` redirects. A repeated
/// `node_id` keeps the last endpoint, as a misconfigured topology would.
fn endpoint_map_from_peers<C: RaftTypeConfig>(
    peers: impl IntoIterator<Item = TsoPeer>,
) -> HashMap<C::NodeId, String>
where
    C::NodeId: From<u64>,
{
    peers
        .into_iter()
        .map(|peer| (C::NodeId::from(peer.node_id), peer.endpoint))
        .collect()
}

/// Project a toolkit [`LeadershipState`] into a tsoracle-consensus
/// [`LeaderState`].
///
/// On the follower branch, `leader_epoch` is the follower's term (equal to the
/// leader's term once a leader is accepted) and `leader_endpoint` is resolved
/// from `peers` (the `NodeId -> tsoracle-service-addr` map); an empty map or an
/// unlisted leader yields `None`.
fn map_leader_state<C: RaftTypeConfig>(
    s: LeadershipState<C>,
    peers: &HashMap<C::NodeId, String>,
) -> LeaderState {
    match s {
        LeadershipState::Leader { term } => LeaderState::Leader {
            epoch: Epoch(u128::from(term)),
        },
        LeadershipState::Follower { term, leader } => LeaderState::Follower {
            leader_endpoint: leader.and_then(|(id, _node)| peers.get(&id).cloned()),
            leader_epoch: Some(Epoch(u128::from(term))),
        },
        LeadershipState::Candidate { .. }
        | LeadershipState::Learner
        | LeadershipState::Shutdown => LeaderState::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::{endpoint_map_from_peers, map_leader_state};
    use crate::type_config::TypeConfig;
    use std::collections::HashMap;
    use tsoracle_consensus::LeaderState;
    use tsoracle_core::{Epoch, TsoPeer};
    use tsoracle_openraft_toolkit::LeadershipState;

    #[test]
    fn endpoint_map_from_peers_keys_each_endpoint_by_node_id() {
        let map = endpoint_map_from_peers::<TypeConfig>([
            TsoPeer {
                node_id: 2,
                endpoint: "http://node-2:50051".to_string(),
            },
            TsoPeer {
                node_id: 3,
                endpoint: "http://node-3:50051".to_string(),
            },
        ]);
        assert_eq!(map.len(), 2);
        assert_eq!(map.get(&2), Some(&"http://node-2:50051".to_string()));
        assert_eq!(map.get(&3), Some(&"http://node-3:50051".to_string()));
    }

    #[test]
    fn endpoint_map_from_peers_then_resolves_follower_endpoint() {
        // The map a TsoPeer list builds must drive the same follower-redirect
        // resolution as a hand-built NodeId->endpoint map.
        let peers = endpoint_map_from_peers::<TypeConfig>([TsoPeer {
            node_id: 2,
            endpoint: "http://node-2:50051".to_string(),
        }]);
        let state = map_leader_state::<TypeConfig>(
            LeadershipState::Follower {
                term: 4,
                leader: Some((
                    2u64,
                    crate::type_config::OpenraftPeer {
                        addr: "raft-addr".into(),
                        service_endpoint: String::new(),
                    },
                )),
            },
            &peers,
        );
        assert_eq!(
            state,
            LeaderState::Follower {
                leader_endpoint: Some("http://node-2:50051".into()),
                leader_epoch: Some(Epoch(4)),
            }
        );
    }

    #[test]
    fn leader_maps_to_leader_with_epoch() {
        let peers = HashMap::new();
        let s = map_leader_state::<TypeConfig>(LeadershipState::Leader { term: 7 }, &peers);
        assert_eq!(s, LeaderState::Leader { epoch: Epoch(7) });
    }

    #[test]
    fn follower_with_no_leader_maps_to_follower_with_epoch() {
        let peers = HashMap::new();
        let s = map_leader_state::<TypeConfig>(
            LeadershipState::Follower {
                term: 3,
                leader: None,
            },
            &peers,
        );
        assert_eq!(
            s,
            LeaderState::Follower {
                leader_endpoint: None,
                leader_epoch: Some(Epoch(3))
            }
        );
    }

    #[test]
    fn follower_with_known_leader_resolves_endpoint_and_epoch() {
        let mut peers = HashMap::new();
        peers.insert(2u64, "http://node-2:50051".to_string());
        let s = map_leader_state::<TypeConfig>(
            LeadershipState::Follower {
                term: 4,
                leader: Some((
                    2u64,
                    crate::type_config::OpenraftPeer {
                        addr: "raft-addr".into(),
                        service_endpoint: String::new(),
                    },
                )),
            },
            &peers,
        );
        assert_eq!(
            s,
            LeaderState::Follower {
                leader_endpoint: Some("http://node-2:50051".into()),
                leader_epoch: Some(Epoch(4)),
            }
        );
    }

    #[test]
    fn follower_with_leader_absent_from_peer_map_has_no_endpoint() {
        let peers = HashMap::new();
        let s = map_leader_state::<TypeConfig>(
            LeadershipState::Follower {
                term: 4,
                leader: Some((
                    9u64,
                    crate::type_config::OpenraftPeer {
                        addr: "raft-addr".into(),
                        service_endpoint: String::new(),
                    },
                )),
            },
            &peers,
        );
        assert_eq!(
            s,
            LeaderState::Follower {
                leader_endpoint: None,
                leader_epoch: Some(Epoch(4))
            }
        );
    }

    #[test]
    fn follower_with_leader_absent_from_nonempty_peer_map_has_no_endpoint() {
        // A populated map that simply does not list the elected leader must
        // still resolve to `None` — guards against a lookup that falls back to
        // some other entry rather than the leader's own id.
        let mut peers = HashMap::new();
        peers.insert(1u64, "http://node-1:50051".to_string());
        peers.insert(3u64, "http://node-3:50051".to_string());
        let s = map_leader_state::<TypeConfig>(
            LeadershipState::Follower {
                term: 5,
                leader: Some((
                    2u64,
                    crate::type_config::OpenraftPeer {
                        addr: "raft-addr".into(),
                        service_endpoint: String::new(),
                    },
                )),
            },
            &peers,
        );
        assert_eq!(
            s,
            LeaderState::Follower {
                leader_endpoint: None,
                leader_epoch: Some(Epoch(5))
            }
        );
    }

    #[test]
    fn candidate_maps_to_unknown() {
        let peers = HashMap::new();
        let s = map_leader_state::<TypeConfig>(LeadershipState::Candidate { term: 5 }, &peers);
        assert_eq!(s, LeaderState::Unknown);
    }

    #[test]
    fn learner_maps_to_unknown() {
        let peers = HashMap::new();
        let s = map_leader_state::<TypeConfig>(LeadershipState::Learner, &peers);
        assert_eq!(s, LeaderState::Unknown);
    }

    #[test]
    fn shutdown_maps_to_unknown() {
        let peers = HashMap::new();
        let s = map_leader_state::<TypeConfig>(LeadershipState::Shutdown, &peers);
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
            matches!(err, tsoracle_consensus::ConsensusError::PermanentDriver(_)),
            "out-of-range advance must classify as PermanentDriver, got {err:?}"
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
