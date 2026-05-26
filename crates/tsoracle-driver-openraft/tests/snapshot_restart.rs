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

//! Snapshot → purge → restart → replay acceptance test for the persisted
//! snapshot backend.
//!
//! Configures a single-node cluster with an aggressive snapshot policy
//! (`LogsSinceLast(4)` + `max_in_snapshot_log_to_keep = 0`) backed by a
//! [`RocksdbSnapshotStore`] sharing the same `Arc<DB>` as the rocksdb log
//! store. Bumps the high-water value enough times that openraft fires a
//! snapshot and purges the log prefix it covers. Shuts the raft down, reopens
//! the same on-disk rocksdb, constructs a fresh
//! `HighWaterStateMachine::with_store`, and asserts the SM recovers to the
//! value at shutdown — even though the log entries that originally bumped it
//! there are gone.
//!
//! Without persistent snapshots the SM would come back at `current_value = 0`
//! with `last_applied = None`, and openraft would refuse to start (or panic)
//! because the log store reports `last_purged_log_id` above index 0 — a
//! state-machine-durability mismatch.

mod common;

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use openraft::async_runtime::watch::WatchReceiver;
use openraft::storage::RaftLogStorage;
use openraft::{Config, SnapshotPolicy};
use tokio::time::timeout;
use tsoracle_consensus::{ConsensusDriver, LeaderState};
use tsoracle_core::Epoch;
use tsoracle_driver_openraft::{OpenraftLogCodec, TypeConfig};
use tsoracle_openraft_toolkit::{Flat, RocksdbLogStore};

use common::{TestCluster, build_single_node_with_config, reopen_node_with_config};

fn aggressive_snapshot_config() -> Arc<Config> {
    Arc::new(
        Config {
            heartbeat_interval: 50,
            election_timeout_min: 150,
            election_timeout_max: 300,
            // Snapshot every 4 entries; purge everything covered by snapshot.
            snapshot_policy: SnapshotPolicy::LogsSinceLast(4),
            max_in_snapshot_log_to_keep: 0,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    )
}

#[tokio::test(start_paused = true)]
async fn snapshot_persists_across_restart_when_log_is_purged() {
    let cluster = build_single_node_with_config(aggressive_snapshot_config()).await;
    let TestCluster {
        mut nodes, drivers, ..
    } = cluster;
    let driver = drivers[0].clone();

    // Wait for leadership before bumping.
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
    .expect("became leader within 5s");
    drop(events);

    // 8 bumps with LogsSinceLast(4) fires at least two snapshots and purges
    // the log prefix each one covers.
    for target in [100u64, 200, 300, 400, 500, 600, 700, 800] {
        let v = driver.persist_high_water(target, Epoch(1)).await.unwrap();
        assert_eq!(v, target);
    }

    // Wait for a snapshot to have been built. `metrics.snapshot` reports the
    // last snapshot log_id openraft installed on this node; without it we
    // cannot be sure the persistent path has been exercised.
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
    // assertion the test could pass even if snapshot persistence were broken
    // — the recovered SM would just rebuild from log replay. Open a separate
    // log-store view on the same `Arc<DB>` so we can inspect `get_log_state`
    // without disturbing the running raft.
    let log_inspector: RocksdbLogStore<TypeConfig, Flat, OpenraftLogCodec> =
        RocksdbLogStore::open(nodes[0].db.clone(), "raft_log", "raft_meta", Flat).unwrap();
    timeout(Duration::from_secs(5), async {
        let mut inspector = log_inspector;
        loop {
            let state = inspector.get_log_state().await.unwrap();
            if let Some(purged) = state.last_purged_log_id {
                if purged.index >= snapshot_log_id.index {
                    return purged;
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("log purged up to the snapshot within 5s");

    // Drop driver handles so the reopen helper can shut the raft down cleanly.
    drop(driver);
    drop(drivers);

    // Reopen with the same rocksdb DB. The state machine must come up at
    // value = 800 from the persisted snapshot, not from the (now-purged) log.
    let prior = nodes.remove(0);
    let reopened = reopen_node_with_config(prior, aggressive_snapshot_config()).await;

    let value = reopened.sm.current_value().await;
    assert_eq!(
        value, 800,
        "state machine must recover from persisted snapshot after log purge \
         (got {value}; expected the last value before shutdown)",
    );

    // Cluster is still functional post-replay.
    let host =
        tsoracle_driver_openraft::StandaloneHost::new(reopened.raft.clone(), reopened.sm.clone());
    let reopened_driver = tsoracle_driver_openraft::OpenraftDriver::new(host);

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

    let v = reopened_driver
        .persist_high_water(900, Epoch(1))
        .await
        .expect("post-replay bump");
    assert_eq!(v, 900);
}
