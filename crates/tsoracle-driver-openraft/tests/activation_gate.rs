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

//! Leader-side activation-gate tests.
//!
//! Stands up a single-voter openraft cluster via [`common::build_single_node`]
//! and drives the gate on a [`StandaloneHost`] built from the cluster's raft +
//! state machine. Single-node means the local-node short-circuit answers
//! everything, so the supplied [`CapabilitySource`] is never called — we
//! supply [`UnusedSource`] which panics on any remote query as a sanity check.

mod common;

use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use tokio::time::timeout;
use tsoracle_consensus::{ConsensusDriver, LeaderState};
use tsoracle_driver_openraft::{
    CapabilitySource, FormatActivationError, NodeCapabilities, OpenraftPeer, StandaloneHost,
};

use common::build_single_node;

/// A source that must never be called: a single-node gate answers entirely
/// from the local short-circuit, so any remote query is a bug.
struct UnusedSource;

#[async_trait]
impl CapabilitySource for UnusedSource {
    type Node = OpenraftPeer;

    async fn query(
        &self,
        node_id: u64,
        _member: &OpenraftPeer,
    ) -> Result<NodeCapabilities, String> {
        panic!("single-node gate must not query remote node {node_id}");
    }
}

/// Stand up a single-voter cluster, wait until it elects itself, and return a
/// [`StandaloneHost`] handle plus a guard that holds the temp dirs alive for
/// the duration of the test.
async fn single_node_leader_host() -> (StandaloneHost, common::TestCluster) {
    let cluster = build_single_node().await;
    let driver = &cluster.drivers[0];

    let mut events = driver.leadership_events();
    timeout(Duration::from_secs(5), async {
        loop {
            let state = events.next().await.expect("event stream alive");
            if matches!(state, LeaderState::Leader { .. }) {
                break;
            }
        }
    })
    .await
    .expect("single-node openraft did not elect itself within 5s");

    let host = StandaloneHost::new(cluster.nodes[0].raft.clone(), cluster.nodes[0].sm.clone());
    (host, cluster)
}

#[tokio::test(start_paused = true)]
async fn gate_passes_when_target_within_local_max() {
    let (host, _cluster) = single_node_leader_host().await;
    // The local node's max_readable_version is MAX_READABLE_VERSION; a target
    // at or below it must pass and return the gated set {1}.
    let gated = host
        .run_activation_gate(
            tsoracle_openraft_toolkit::MAX_READABLE_VERSION,
            &UnusedSource,
        )
        .await
        .expect("gate passes for an in-range target");
    assert_eq!(gated, std::collections::BTreeSet::from([1]));
}

#[tokio::test(start_paused = true)]
async fn gate_fails_when_target_above_local_max() {
    let (host, _cluster) = single_node_leader_host().await;
    let target = tsoracle_openraft_toolkit::MAX_READABLE_VERSION + 1;
    let err = host
        .run_activation_gate(target, &UnusedSource)
        .await
        .expect_err("gate fails when the only member cannot read the target");
    let FormatActivationError::MembersBelowTarget {
        target: returned,
        incapable,
    } = err
    else {
        panic!("expected MembersBelowTarget");
    };
    assert_eq!(returned, target);
    assert_eq!(incapable.len(), 1);
    assert_eq!(incapable[0].0, 1);
    assert_eq!(
        incapable[0].1,
        tsoracle_openraft_toolkit::MAX_READABLE_VERSION
    );
}

#[tokio::test(start_paused = true)]
async fn initiate_format_activation_runs_gate_and_returns_ok_for_now() {
    let (host, _cluster) = single_node_leader_host().await;
    // The all-members gate against the only voter (local short-circuit)
    // passes; the body between the gate and Ok(()) is a later phase's
    // proposal step.
    host.initiate_format_activation(
        tsoracle_openraft_toolkit::MAX_READABLE_VERSION,
        &UnusedSource,
    )
    .await
    .expect("a passing gate returns Ok in this phase");
}
