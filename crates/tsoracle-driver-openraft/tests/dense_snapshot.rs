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

//! Snapshot → purge → restart → replay acceptance test for the dense map.
//!
//! Configures a single-node cluster with an aggressive snapshot policy
//! (`LogsSinceLast(4)` + `max_in_snapshot_log_to_keep = 0`) backed by a
//! [`RocksdbSnapshotStore`] sharing the same `Arc<DB>` as the rocksdb log store.
//!
//! Activates write version 5, then advances dense keys enough to trigger a
//! snapshot and log purge. Shuts the raft down, reopens the same on-disk
//! rocksdb, and asserts that the dense map was restored FROM THE SNAPSHOT
//! (the counters survive even though the log prefix covering the advances was
//! purged).
//!
//! Without dense-state restore, the SM would come back with `dense = {}` and the
//! first post-restart `advance_dense` would return `start = 0` rather than the
//! persisted counter, violating the gapless guarantee.

mod common;

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use openraft::async_runtime::watch::WatchReceiver;
use openraft::storage::RaftLogStorage;
use openraft::{Config, SnapshotPolicy};
use tokio::time::timeout;
use tsoracle_consensus::{ConsensusDriver, LeaderState};
use tsoracle_core::{Epoch, SeqKey};
use tsoracle_driver_openraft::{
    CapabilitySource, NodeCapabilities, OpenraftDriver, OpenraftLogCodec, OpenraftPeer,
    StandaloneHost, TypeConfig,
};
use tsoracle_openraft_toolkit::{DENSE_WRITE_VERSION, Flat, RocksdbLogStore};

use common::{TestCluster, build_single_node_with_config, reopen_node_with_config};

/// A `CapabilitySource` for single-node clusters that must never be called.
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

fn aggressive_snapshot_config() -> Arc<Config> {
    Arc::new(
        Config {
            heartbeat_interval: 50,
            election_timeout_min: 150,
            election_timeout_max: 300,
            // Snapshot every 4 entries; purge everything covered by the snapshot.
            snapshot_policy: SnapshotPolicy::LogsSinceLast(4),
            max_in_snapshot_log_to_keep: 0,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    )
}

/// Wait for a single-node cluster to elect itself leader.
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
async fn dense_map_survives_snapshot_build_and_log_purge() {
    let cluster = build_single_node_with_config(aggressive_snapshot_config()).await;
    let TestCluster {
        mut nodes, drivers, ..
    } = cluster;
    let driver = drivers[0].clone();

    wait_for_leadership(&driver).await;

    // Build a StandaloneHost to call `initiate_format_activation`.
    let host = StandaloneHost::new(nodes[0].raft.clone(), nodes[0].sm.clone());

    // Activate write version 5. Single-node short-circuits the all-members check.
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

    // Advance dense keys interleaved with high-water bumps. With
    // LogsSinceLast(4) and max_in_snapshot_log_to_keep=0, we need enough
    // entries to trigger at least one snapshot and log purge. 8 rounds of two
    // entries (advance_dense + persist_high_water) gives 16 entries, well above
    // the threshold.
    for i in 0u32..8 {
        driver
            .advance_dense(&key_orders, 1, Epoch::ZERO)
            .await
            .expect("advance orders");
        // Interleave a high-water bump to ensure plenty of entries flow through.
        driver
            .persist_high_water(u64::from(i + 1) * 100, Epoch::ZERO)
            .await
            .expect("persist_high_water");
    }

    // Advance "users" to a non-trivial value.
    driver
        .advance_dense(&key_users, 5, Epoch::ZERO)
        .await
        .expect("advance users");

    // Expected dense state: orders = 8, users = 5.
    let orders_before = nodes[0].sm.dense_value("orders");
    let users_before = nodes[0].sm.dense_value("users");
    assert_eq!(
        orders_before, 8,
        "orders counter = 8 before snapshot+restart"
    );
    assert_eq!(users_before, 5, "users counter = 5 before snapshot+restart");

    // Wait for a snapshot to have been built (and thus log purge).
    let metrics = nodes[0].raft.metrics();
    timeout(Duration::from_secs(5), async {
        loop {
            let snapshot_log_id = metrics.borrow_watched().snapshot;
            if snapshot_log_id.is_some() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("snapshot built within 5s");

    let snapshot_log_id = metrics.borrow_watched().snapshot.expect("snapshot built");

    // Verify openraft actually purged the log past the snapshot. Without this
    // the test could pass even without snapshot persistence — the recovered SM
    // would just rebuild from log replay.
    let log_inspector: RocksdbLogStore<TypeConfig, Flat, OpenraftLogCodec> =
        RocksdbLogStore::open(nodes[0].db.clone(), "raft_log", "raft_meta", Flat).unwrap();
    timeout(Duration::from_secs(5), async {
        let mut inspector = log_inspector;
        loop {
            let state = inspector.get_log_state().await.unwrap();
            if let Some(purged) = state.last_purged_log_id
                && purged.index >= snapshot_log_id.index
            {
                return purged;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("log purged up to the snapshot within 5s");

    // Drop driver handles before the reopen helper shuts the raft down.
    drop(driver);
    drop(drivers);

    // Reopen with the same rocksdb DB. The state machine must come up with the
    // dense map restored FROM THE SNAPSHOT (the log prefix covering the advances
    // has been purged).
    let prior = nodes.remove(0);
    let reopened = reopen_node_with_config(prior, aggressive_snapshot_config()).await;

    assert_eq!(
        reopened.sm.dense_value("orders"),
        8,
        "orders dense counter must be restored from snapshot (expected 8)"
    );
    assert_eq!(
        reopened.sm.dense_value("users"),
        5,
        "users dense counter must be restored from snapshot (expected 5)"
    );

    // Cluster is still functional post-restart.
    let reopened_host = StandaloneHost::new(reopened.raft.clone(), reopened.sm.clone());
    let reopened_driver = OpenraftDriver::new(StandaloneHost::new(
        reopened.raft.clone(),
        reopened.sm.clone(),
    ));

    let mut events = reopened_driver.leadership_events();
    timeout(Duration::from_secs(10), async {
        loop {
            let state = events.next().await.expect("event stream alive");
            if matches!(state, LeaderState::Leader { .. }) {
                break;
            }
        }
    })
    .await
    .expect("re-became leader within 10s");

    // The `ActiveWriteVersion` cell starts at BASELINE after restart — it is
    // recovered only via log replay of `SetFormatVersion` entries. With
    // aggressive snapshot + purge, those log entries are gone. Re-activate
    // write version 5 so the gate allows `advance_dense`. This is the
    // expected operator flow: on a cluster that has already activated v5, the
    // re-activation no-ops (the subset is already satisfied; the activation
    // entry re-flips the cell when applied). The dense map was already
    // correctly restored from the snapshot above.
    reopened_host
        .initiate_format_activation(DENSE_WRITE_VERSION, &UnusedSource)
        .await
        .expect("re-activation after snapshot-restore restart must succeed");
    assert_eq!(
        reopened_host.active_write_version(),
        DENSE_WRITE_VERSION,
        "write version cell must be at DENSE_WRITE_VERSION after re-activation"
    );

    // The next advance_dense on "orders" must continue from start = 8 (no gap).
    let start_after = reopened_driver
        .advance_dense(&key_orders, 2, Epoch::ZERO)
        .await
        .expect("advance orders after snapshot restart");
    assert_eq!(
        start_after, 8,
        "first advance of orders after snapshot-restore must start at 8"
    );

    // load_dense_seq on "users" must return the restored counter (5).
    let users_seq = reopened_driver
        .load_dense_seq(&key_users)
        .await
        .expect("load_dense_seq users after snapshot restart");
    assert_eq!(
        users_seq, 5,
        "load_dense_seq(users) must return 5 after snapshot-restore restart"
    );
}
