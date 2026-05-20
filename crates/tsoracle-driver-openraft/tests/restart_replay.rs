//! Restart-replay integration test.
//!
//! Bumps the value on a single-node cluster, drops the Raft handle (after
//! a clean shutdown), reopens the rocksdb log with a fresh state machine,
//! and asserts the new state machine replays to the most recently
//! persisted value. Then asserts the cluster is still functional
//! post-replay (can accept a new bump).

mod common;

use std::time::Duration;

use futures::StreamExt;
use tokio::time::timeout;
use tsoracle_consensus::{ConsensusDriver, LeaderState};
use tsoracle_core::Epoch;

use common::{build_single_node, eventually_eq, reopen_node, TestCluster};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_replays_high_water_from_rocksdb_log() {
    let cluster = build_single_node().await;
    let TestCluster {
        mut nodes,
        drivers,
        ..
    } = cluster;
    let driver = drivers[0].clone();

    // Wait for leadership before bumping.
    let mut events = driver.leadership_events();
    let _epoch = timeout(Duration::from_secs(5), async {
        loop {
            let s = events.next().await.expect("event stream alive");
            if let LeaderState::Leader { epoch } = s {
                break epoch;
            }
        }
    })
    .await
    .expect("became leader within 5s");
    drop(events);

    // First bump.
    let v = driver.persist_high_water(500, Epoch(1)).await.unwrap();
    assert_eq!(v, 500);

    // Second bump.
    let v = driver.persist_high_water(700, Epoch(1)).await.unwrap();
    assert_eq!(v, 700);

    // Drop the drivers so they don't keep the raft alive past shutdown.
    drop(driver);
    drop(drivers);

    // Reopen the node: shut down the prior Raft cleanly, then construct a
    // fresh Raft + state machine against the same on-disk rocksdb log.
    let prior = nodes.remove(0);
    let reopened = reopen_node(prior).await;

    // Build a fresh driver against the reopened node.
    let host = tsoracle_driver_openraft::StandaloneHost::new(
        reopened.raft.clone(),
        reopened.sm.clone(),
    );
    let reopened_driver = tsoracle_driver_openraft::OpenraftDriver::new(host);

    // Replay yields the last persisted value.
    let d = reopened_driver.clone();
    eventually_eq(700u64, Duration::from_secs(10), || {
        let d = d.clone();
        async move { d.load_high_water().await.unwrap() }
    })
    .await;

    // Cluster is still functional post-replay. Wait for re-leadership.
    let mut events = reopened_driver.leadership_events();
    let _epoch = timeout(Duration::from_secs(10), async {
        loop {
            let s = events.next().await.expect("event stream alive");
            if let LeaderState::Leader { epoch } = s {
                break epoch;
            }
        }
    })
    .await
    .expect("re-became leader within 10s");

    let v = reopened_driver
        .persist_high_water(800, Epoch(1))
        .await
        .expect("post-replay bump");
    assert_eq!(v, 800);
}
