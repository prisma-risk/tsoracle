//! Single-node integration test for [`OpenraftDriver`].
//!
//! Builds a one-voter openraft cluster via [`common::build_single_node`] and
//! drives the [`ConsensusDriver`] surface: confirms an initial Leader event,
//! an empty `load_high_water`, a `persist_high_water` round trip, and the
//! monotonic-advance semantics.

mod common;

use std::time::Duration;

use futures::StreamExt;
use tokio::time::timeout;
use tsoracle_consensus::{ConsensusDriver, LeaderState};

use common::build_single_node;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_node_leader_persists_high_water() {
    let cluster = build_single_node().await;
    let driver = &cluster.drivers[0];

    // Drain leadership events until we see a Leader.
    let mut events = driver.leadership_events();
    let epoch = timeout(Duration::from_secs(5), async {
        loop {
            let s = events.next().await.expect("event stream alive");
            if let LeaderState::Leader { epoch } = s {
                break epoch;
            }
        }
    })
    .await
    .expect("became leader within 5s");

    // Empty start.
    assert_eq!(driver.load_high_water().await.unwrap(), 0);

    // Advance.
    let v = driver.persist_high_water(100, epoch).await.unwrap();
    assert_eq!(v, 100);

    // Stale call: should be silently absorbed, value unchanged.
    let v = driver.persist_high_water(50, epoch).await.unwrap();
    assert_eq!(v, 100);

    // Forward.
    let v = driver.persist_high_water(200, epoch).await.unwrap();
    assert_eq!(v, 200);

    // Linearized load matches the last apply.
    assert_eq!(driver.load_high_water().await.unwrap(), 200);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_state_epoch_matches_raft_term() {
    let cluster = build_single_node().await;
    let driver = &cluster.drivers[0];
    let raft = &cluster.nodes[0].raft;

    let mut events = driver.leadership_events();
    let epoch = timeout(Duration::from_secs(5), async {
        loop {
            let s = events.next().await.expect("event stream alive");
            if let LeaderState::Leader { epoch } = s {
                break epoch;
            }
        }
    })
    .await
    .expect("became leader within 5s");

    // The Epoch carried by LeaderState::Leader must match the raft's
    // notion of term at the time we observed leadership.
    use openraft::async_runtime::watch::WatchReceiver;
    use openraft::vote::RaftTerm;
    let metrics_rx = raft.metrics();
    let term = {
        let snap = metrics_rx.borrow_watched();
        snap.current_term.as_u64().unwrap_or(0)
    };
    assert_eq!(epoch.0, term);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leadership_stream_outlives_driver_drop() {
    let cluster = build_single_node().await;

    let driver_for_stream = std::sync::Arc::clone(&cluster.drivers[0]);
    let mut events = driver_for_stream.leadership_events();
    drop(driver_for_stream);

    // KeepAlive keeps the host alive while the stream is held.
    let _first = timeout(Duration::from_secs(5), events.next())
        .await
        .expect("stream produced an event within 5s")
        .expect("event stream alive");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn persist_high_water_ignores_epoch_arg() {
    use tsoracle_core::Epoch;

    let cluster = build_single_node().await;
    let driver = &cluster.drivers[0];

    // Wait for leadership.
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

    // Call persist_high_water twice with very different (arbitrary) epoch
    // arguments. Both should succeed because the driver ignores the epoch
    // (state-machine monotonicity is the fencing mechanism).
    let v = driver.persist_high_water(100, Epoch(99)).await.unwrap();
    assert_eq!(v, 100);

    let v = driver.persist_high_water(200, Epoch(7)).await.unwrap();
    assert_eq!(v, 200);
}
