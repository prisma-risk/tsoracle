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

//! Snapshot policy + restart coverage.
//!
//! Boots a 3-node RocksDB cluster with `SnapshotPolicy::every(10)`,
//! appends 20 `Advance`s, drives the cluster until every node decides
//! all 20. Confirms the snapshot policy fired (`get_compacted_idx() > 0`
//! on the leader). Stops one follower, restarts it, and asserts the
//! restarted node converges to the cluster's high-water — the on-disk
//! snapshot persists across the restart, and the recovered log suffix
//! plus any missing entries replicated from peers bring the node back
//! to the latest decided state.

use tsoracle_driver_paxos::AdvancePayload;
use tsoracle_driver_paxos::{HighWaterCommand, SnapshotPolicy};

#[path = "common/mod.rs"]
mod common;

// Driven by the deterministic step-driver (`step_until`) rather than async
// runner/pump tasks + real-time `drive_until`, so the snapshot-policy +
// restart-replay coverage converges in simulated steps with no wall-clock
// budget to overrun. Real RocksDB storage + snapshot persistence stay tested.
#[tokio::test]
async fn snapshot_policy_persists_across_restart() {
    let mut cluster = common::build_rocksdb_cluster_with_policy(3, SnapshotPolicy::every(10));

    cluster.step_until(common::some_leader_elected(), 2_000);
    let leader_id = cluster.leader();

    // Append 20 advances: the policy must fire at least once by
    // decided_idx = 10 and again at 20.
    for n in 1..=20u64 {
        cluster
            .node(leader_id)
            .omnipaxos()
            .lock()
            .append(HighWaterCommand::Advance(AdvancePayload { at_least: n }))
            .expect("append succeeds on leader");
    }

    cluster.step_until(common::all_decided_at_least(20), 3_000);
    cluster.step_until(
        |state| {
            state
                .nodes
                .iter()
                .all(|node| state.high_water_on(node.node_id) >= 20)
        },
        3_000,
    );

    // Verify the policy fired on the leader's local OmniPaxos — the
    // compacted index advances when `snapshot(idx, false)` succeeds, so
    // a non-zero compacted_idx is the load-bearing observation.
    let leader_compacted = cluster
        .node(leader_id)
        .omnipaxos()
        .lock()
        .get_compacted_idx();
    assert!(
        leader_compacted >= 10,
        "leader's compacted_idx must reflect at least one snapshot trigger (saw {leader_compacted})",
    );

    // Pick a follower whose own compacted_idx also advanced (the
    // cluster-wide snapshot path means every node trims its log).
    let follower_id = cluster
        .nodes
        .iter()
        .map(|node| node.node_id)
        .find(|id| *id != leader_id)
        .expect("at least one follower");
    let follower_compacted_before = cluster
        .node(follower_id)
        .omnipaxos()
        .lock()
        .get_compacted_idx();
    assert!(
        follower_compacted_before >= 10,
        "follower must have observed at least one snapshot trigger (saw {follower_compacted_before})",
    );

    // Bounce the follower. The TempDir + RocksDB on-disk state (log
    // entries, snapshot, compacted_idx) persists.
    cluster.stop_node(follower_id).await;
    cluster.rebuild_rocksdb_node(follower_id);

    // After recovery the persisted compacted_idx must round-trip.
    let follower_compacted_after = cluster
        .node(follower_id)
        .omnipaxos()
        .lock()
        .get_compacted_idx();
    assert_eq!(
        follower_compacted_after, follower_compacted_before,
        "compacted_idx must survive a restart cycle",
    );

    // The rebuilt follower is immediately steppable again (no async start
    // needed); it must converge to the latest high-water of 20.
    cluster.step_until(|state| state.high_water_on(follower_id) >= 20, 3_000);
    assert_eq!(cluster.high_water_on(follower_id), 20);

    cluster.stop_all().await;
}
