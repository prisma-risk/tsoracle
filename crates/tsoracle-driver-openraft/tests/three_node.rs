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

//! Three-voter integration tests for `OpenraftDriver`.
//!
//! Asserts:
//! 1. A leader is elected.
//! 2. A write through the leader becomes visible on every node's state
//!    machine (eventually — `StandaloneHost::current_high_water` reads
//!    locally without a linearizable barrier; see spec §8.2).
//! 3. A write through a follower returns `ConsensusError::NotLeader`.

mod common;

use std::time::Duration;

use tokio::time::timeout;
use tsoracle_consensus::{ConsensusDriver, ConsensusError};
use tsoracle_core::Epoch;

use common::{TestCluster, build_three_node, eventually_eq};

async fn find_leader_idx(cluster: &TestCluster) -> usize {
    timeout(Duration::from_secs(10), async {
        loop {
            for (idx, node) in cluster.nodes.iter().enumerate() {
                if let Some(l) = node.raft.current_leader().await
                    && l == node.id
                {
                    return idx;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("some node became leader within 10s")
}

#[tokio::test(start_paused = true)]
async fn three_node_leader_persists_and_followers_converge() {
    let cluster = build_three_node().await;

    let leader_idx = find_leader_idx(&cluster).await;
    let leader_driver = &cluster.drivers[leader_idx];

    // Empty start.
    assert_eq!(leader_driver.load_high_water().await.unwrap(), 0);

    // Advance.
    let v = leader_driver
        .persist_high_water(100, Epoch(1))
        .await
        .unwrap();
    assert_eq!(v, 100);

    // All three SMs should converge on 100 within 5 seconds. Read each
    // node's SM directly: `load_high_water` requires a linearizable barrier
    // that followers cannot satisfy locally.
    for i in 0..3 {
        let sm = cluster.nodes[i].sm.clone();
        eventually_eq(100u64, Duration::from_secs(5), || {
            let sm = sm.clone();
            async move { sm.current_value().await }
        })
        .await;
    }
}

#[tokio::test(start_paused = true)]
async fn three_node_follower_returns_not_leader() {
    let cluster = build_three_node().await;

    let leader_idx = find_leader_idx(&cluster).await;
    let follower_idx = (0..3)
        .find(|i| *i != leader_idx)
        .expect("a follower exists");
    let follower_driver = &cluster.drivers[follower_idx];

    let err = follower_driver
        .persist_high_water(50, Epoch(1))
        .await
        .expect_err("follower must reject write");

    match err {
        ConsensusError::NotLeader { observed: None } => {} // expected
        other => panic!("expected NotLeader{{ observed: None }}, got {other:?}"),
    }
}
