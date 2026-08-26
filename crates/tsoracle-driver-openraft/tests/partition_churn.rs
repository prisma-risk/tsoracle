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

//! Partition + leader-churn integration test.
//!
//! Establishes a 3-voter cluster with a baseline value, isolates the
//! current leader L, waits for a new leader L' to be elected on the majority
//! side, writes through L', heals the partition, and asserts L catches up
//! monotonically to the new value.

mod common;

use std::time::Duration;

use tokio::time::timeout;
use tsoracle_consensus::ConsensusDriver;
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

async fn find_leader_excluding(cluster: &TestCluster, exclude_idx: usize) -> usize {
    timeout(Duration::from_secs(10), async {
        loop {
            for (idx, node) in cluster.nodes.iter().enumerate() {
                if idx == exclude_idx {
                    continue;
                }
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
    .expect("a new leader (excluding old) was elected within 10s")
}

#[tokio::test(start_paused = true)]
async fn partition_then_heal_converges_monotonically() {
    let cluster = build_three_node().await;
    let partitions = cluster
        .partitions
        .as_ref()
        .expect("three-node cluster has partitions")
        .clone();

    // Baseline: bump to 100 via the current leader; all nodes converge.
    let l_idx = find_leader_idx(&cluster).await;
    let l_id = cluster.nodes[l_idx].id;
    let v = cluster.drivers[l_idx]
        .persist_high_water(100, Epoch(1))
        .await
        .expect("baseline bump");
    assert_eq!(v, 100);
    for i in 0..3 {
        let sm = cluster.nodes[i].sm.clone();
        eventually_eq(100u64, Duration::from_secs(5), || {
            let sm = sm.clone();
            async move { sm.current_value().await }
        })
        .await;
    }

    // Partition: isolate L. Real-time sleep past election_timeout_max.
    partitions.isolate(l_id);
    tokio::time::sleep(Duration::from_millis(500)).await;

    let l_prime_idx = find_leader_excluding(&cluster, l_idx).await;
    assert_ne!(l_prime_idx, l_idx, "new leader must differ from old");

    // Majority-side write via L'. L' + remaining follower = quorum.
    let v = cluster.drivers[l_prime_idx]
        .persist_high_water(200, Epoch(2))
        .await
        .expect("L_prime persists 200");
    assert_eq!(v, 200);

    // Diagnostic: an attempt on the isolated leader should stall, not
    // succeed silently with a stale read or return NotLeader. (openraft
    // doesn't preemptively step down; the leader keeps trying to replicate
    // and hits no quorum.)
    let attempt = timeout(
        Duration::from_secs(2),
        cluster.drivers[l_idx].persist_high_water(300, Epoch(3)),
    )
    .await;
    assert!(
        attempt.is_err(),
        "isolated leader should stall, not return; got {attempt:?}",
    );

    // Heal the partition.
    partitions.heal(l_id);

    // L converges on 200 (never went backwards: 100 -> 200, monotone).
    // L is now a follower after the partition heal, so read its SM directly.
    let sm = cluster.nodes[l_idx].sm.clone();
    eventually_eq(200u64, Duration::from_secs(10), || {
        let sm = sm.clone();
        async move { sm.current_value().await }
    })
    .await;
}
