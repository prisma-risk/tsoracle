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
                if let Some(l) = node.raft.current_leader().await {
                    if l == node.id {
                        return idx;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("some node became leader within 10s")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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
