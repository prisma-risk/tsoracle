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

//! Compose `OpenraftDriver<StandaloneHost>` with host-specific endpoint
//! resolution for `LeaderHint` follower-redirect.
//!
//! The driver crate's generic mapper has no way to project `C::Node` into a
//! tsoracle gRPC endpoint — different hosts pick different `Node` types. This
//! example knows: `--tso-peers` carries the `NodeId -> tsoracle-addr` map.
//! This wrapper hand-rolls `leadership_events` to populate the endpoint;
//! `load_high_water` and `persist_high_water` delegate to the inner driver.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use futures::{Stream, StreamExt};
use openraft::Raft;
use tsoracle_consensus::{ConsensusDriver, ConsensusError, LeaderState};
use tsoracle_core::Epoch;
use tsoracle_driver_openraft::{HighWaterStateMachine, OpenraftDriver, StandaloneHost, TypeConfig};
use tsoracle_openraft_toolkit::LeadershipState;
use tsoracle_openraft_toolkit::lifecycle::leader::stream_from_receiver;

/// `ConsensusDriver` wrapper that delegates load/persist to the bundled
/// `OpenraftDriver<StandaloneHost>` and rebuilds `leadership_events()` over
/// the local raft metrics watch + a `NodeId -> tsoracle-addr` map.
pub struct StandaloneRouter {
    inner: Arc<OpenraftDriver<StandaloneHost>>,
    raft: Raft<TypeConfig, HighWaterStateMachine>,
    tso_addrs: Arc<HashMap<u64, String>>,
}

impl StandaloneRouter {
    pub fn new(
        inner: Arc<OpenraftDriver<StandaloneHost>>,
        raft: Raft<TypeConfig, HighWaterStateMachine>,
        tso_addrs: Arc<HashMap<u64, String>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner,
            raft,
            tso_addrs,
        })
    }
}

#[async_trait]
impl ConsensusDriver for StandaloneRouter {
    fn leadership_events(&self) -> Pin<Box<dyn Stream<Item = LeaderState> + Send>> {
        let receiver = self.raft.metrics();
        let addrs = Arc::clone(&self.tso_addrs);
        Box::pin(
            stream_from_receiver::<TypeConfig>(receiver).map(move |state| match state {
                LeadershipState::Leader { term } => LeaderState::Leader {
                    epoch: Epoch(u128::from(term)),
                },
                LeadershipState::Follower {
                    term,
                    leader: Some((id, _peer)),
                } => LeaderState::Follower {
                    leader_endpoint: addrs.get(&id).cloned(),
                    leader_epoch: Some(Epoch(u128::from(term))),
                },
                LeadershipState::Follower { term, leader: None } => LeaderState::Follower {
                    leader_endpoint: None,
                    leader_epoch: Some(Epoch(u128::from(term))),
                },
                LeadershipState::Candidate { .. }
                | LeadershipState::Learner
                | LeadershipState::Shutdown => LeaderState::Unknown,
            }),
        )
    }

    async fn load_high_water(&self) -> Result<u64, ConsensusError> {
        self.inner.load_high_water().await
    }

    async fn persist_high_water(&self, at_least: u64, epoch: Epoch) -> Result<u64, ConsensusError> {
        self.inner.persist_high_water(at_least, epoch).await
    }
}
