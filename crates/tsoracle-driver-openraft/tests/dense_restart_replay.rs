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

//! Restart-replay integration test for dense state.
//!
//! Activates write version 5, advances a couple of dense keys, shuts down
//! the cluster cleanly, reopens it against the same on-disk rocksdb log, and
//! asserts that:
//!
//! - The dense counters replayed to their persisted values (the next
//!   `advance_dense` call returns the persisted `start`, with no gap).
//! - The genesis cardinality cap survived (advancing continues to work
//!   without cardinality errors).

mod common;

use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use tokio::time::timeout;
use tsoracle_consensus::{ConsensusDriver, LeaderState};
use tsoracle_core::{Epoch, SeqKey};
use tsoracle_driver_openraft::{
    CapabilitySource, NodeCapabilities, OpenraftDriver, OpenraftPeer, StandaloneHost,
};
use tsoracle_openraft_toolkit::DENSE_WRITE_VERSION;

use common::{TestCluster, build_single_node, reopen_node};

/// A `CapabilitySource` for single-node clusters that must never be called:
/// the local node short-circuit answers entirely from itself without any peer
/// RPCs.
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

/// Wait for a single-node cluster to elect itself leader; return the epoch.
async fn wait_for_leadership(driver: &OpenraftDriver<StandaloneHost>) {
    let mut events = driver.leadership_events();
    timeout(Duration::from_secs(5), async {
        loop {
            let s = events.next().await.expect("event stream alive");
            if matches!(s, LeaderState::Leader { .. }) {
                break;
            }
        }
    })
    .await
    .expect("single-node did not elect itself within 5s");
}

#[tokio::test(start_paused = true)]
async fn restart_replays_dense_map_from_rocksdb_log() {
    let cluster = build_single_node().await;
    let TestCluster {
        mut nodes, drivers, ..
    } = cluster;
    let driver = drivers[0].clone();

    wait_for_leadership(&driver).await;

    // Build a StandaloneHost to call `initiate_format_activation`.
    let host = StandaloneHost::new(nodes[0].raft.clone(), nodes[0].sm.clone());

    // Activate write version 5 through the real gate. Single-node short-circuits
    // the all-members check, so UnusedSource is never called.
    host.initiate_format_activation(DENSE_WRITE_VERSION, &UnusedSource)
        .await
        .expect("activation must succeed on a single-node cluster");
    assert_eq!(
        host.active_write_version(),
        DENSE_WRITE_VERSION,
        "cell must read DENSE_WRITE_VERSION after activation"
    );

    let key_orders = SeqKey::try_new("orders").unwrap();
    let key_users = SeqKey::try_new("users").unwrap();

    // Advance "orders" by 5, then by 3.
    let start0 = driver
        .advance_dense(&key_orders, 5, Epoch::ZERO)
        .await
        .expect("advance orders by 5");
    assert_eq!(start0, 0, "first advance of orders starts at 0");

    let start1 = driver
        .advance_dense(&key_orders, 3, Epoch::ZERO)
        .await
        .expect("advance orders by 3");
    assert_eq!(start1, 5, "second advance of orders starts at 5");

    // Advance "users" by 1.
    let start2 = driver
        .advance_dense(&key_users, 1, Epoch::ZERO)
        .await
        .expect("advance users by 1");
    assert_eq!(start2, 0, "first advance of users starts at 0");

    // Verify SM state directly.
    assert_eq!(
        nodes[0].sm.dense_value("orders"),
        8,
        "orders dense counter = 8 before restart"
    );
    assert_eq!(
        nodes[0].sm.dense_value("users"),
        1,
        "users dense counter = 1 before restart"
    );

    // Drop the drivers so they don't keep the raft alive past shutdown.
    drop(driver);
    drop(drivers);

    // Reopen the node: shut down the prior Raft cleanly, then construct a
    // fresh Raft + state machine against the same on-disk rocksdb log.
    let prior = nodes.remove(0);
    let reopened = reopen_node(prior).await;

    // The SM must have replayed the dense counters from the log.
    assert_eq!(
        reopened.sm.dense_value("orders"),
        8,
        "orders dense counter must replay to 8 after restart"
    );
    assert_eq!(
        reopened.sm.dense_value("users"),
        1,
        "users dense counter must replay to 1 after restart"
    );

    // Build a fresh driver + host against the reopened node.
    let reopened_host = StandaloneHost::new(reopened.raft.clone(), reopened.sm.clone());
    let reopened_driver = OpenraftDriver::new(reopened_host);

    // Wait for re-leadership on the reopened node.
    wait_for_leadership(&reopened_driver).await;

    // The next advance_dense on "orders" must continue from start = 8 (no gap).
    let start_after = reopened_driver
        .advance_dense(&key_orders, 1, Epoch::ZERO)
        .await
        .expect("advance orders after restart");
    assert_eq!(
        start_after, 8,
        "first advance of orders after restart must start at 8 (replayed)"
    );

    // load_dense_seq on "users" must return the replayed counter (1).
    let users_seq = reopened_driver
        .load_dense_seq(&key_users)
        .await
        .expect("load_dense_seq users after restart");
    assert_eq!(
        users_seq, 1,
        "load_dense_seq(users) must return 1 after restart replay"
    );

    // Genesis cap survived: advancing a new key still works (no cardinality error
    // from a stale cap = 0). DEFAULT_DENSE_CARDINALITY_CAP = 10_000 so two keys
    // is nowhere near the limit.
    let key_new = SeqKey::try_new("payments").unwrap();
    let start_new = reopened_driver
        .advance_dense(&key_new, 10, Epoch::ZERO)
        .await
        .expect("advance a new key after restart — genesis cap must survive");
    assert_eq!(start_new, 0, "new key after restart starts at 0");
}
