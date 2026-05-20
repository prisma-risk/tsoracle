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
