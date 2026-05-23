//
//  ░▀█▀░█▀▀░█▀█░█▀▄░█▀█░█▀▀░█░░░█▀▀
//  ░░█░░▀▀█░█░█░█▀▄░█▀█░█░░░█░░░█▀▀
//  ░░▀░░▀▀▀░▀▀▀░▀░▀░▀░▀░▀▀▀░▀▀▀░▀▀▀
//
//  tsoracle — Distributed Timestamp Oracle
//
//  Copyright (c) 2026 Prisma Risk
//  Licensed under the Apache License, Version 2.0
//  https://github.com/prisma-risk/tsoracle
//

//! Restart coverage with RocksDB-backed storage.
//!
//! Boots a 3-node RocksDB cluster, decides a couple of `Advance`s, stops
//! one follower mid-flight, decides one more entry on the remaining
//! majority, then re-opens the stopped follower's storage and rebuilds
//! its host. The restarted node must catch up to the decided state via
//! log replay (the runner re-asks for the missing suffix from its peers
//! once it sees their `decided_idx > local_decided_idx`).

use tsoracle_driver_paxos::HighWaterCommand;

#[path = "common/mod.rs"]
mod common;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_recovers_decided_state_from_storage() {
    let mut cluster = common::build_rocksdb_cluster(3);
    cluster.start_all();

    cluster
        .drive_until(common::some_leader_elected(), 2_000)
        .await;
    let leader_id = cluster.leader();

    cluster
        .node(leader_id)
        .omnipaxos()
        .lock()
        .append(HighWaterCommand::Advance { at_least: 10 })
        .expect("first append succeeds on leader");
    cluster
        .node(leader_id)
        .omnipaxos()
        .lock()
        .append(HighWaterCommand::Advance { at_least: 50 })
        .expect("second append succeeds on leader");

    cluster
        .drive_until(
            |state| {
                state
                    .nodes
                    .iter()
                    .all(|node| state.high_water_on(node.node_id) >= 50)
            },
            2_000,
        )
        .await;

    // Pick a follower to bounce.
    let follower_id = cluster
        .nodes
        .iter()
        .map(|node| node.node_id)
        .find(|id| *id != leader_id)
        .expect("at least one follower");

    cluster.stop_node(follower_id).await;

    // Decide one more entry on the remaining majority. The stopped
    // follower's RocksDB still has the first two entries persisted but
    // is unreachable for this round.
    cluster
        .node(leader_id)
        .omnipaxos()
        .lock()
        .append(HighWaterCommand::Advance { at_least: 100 })
        .expect("third append succeeds on leader");
    cluster
        .drive_until(
            |state| {
                state
                    .nodes
                    .iter()
                    .filter(|node| node.node_id != follower_id)
                    .all(|node| state.high_water_on(node.node_id) >= 100)
            },
            2_000,
        )
        .await;

    // Re-open the follower's storage and rebuild its host. Restart its
    // runner + pump; the OmniPaxos handle should recover the persisted
    // promise and replay the missing log suffix from its peers.
    cluster.rebuild_rocksdb_node(follower_id);

    // Sanity: the recovered handle has the durably-persisted first two
    // entries' decided_idx >= 2 (the durable decided_idx written by
    // set_decided_idx during the original session).
    let recovered_decided = cluster.decided_idx_on(follower_id);
    assert!(
        recovered_decided >= 2,
        "follower's recovered decided_idx must reflect the first two entries (saw {recovered_decided})",
    );

    cluster.start_node(follower_id);

    // Catch-up: the restarted follower must converge to high_water == 100.
    cluster
        .drive_until(|state| state.high_water_on(follower_id) >= 100, 3_000)
        .await;

    assert_eq!(cluster.high_water_on(follower_id), 100);

    cluster.stop_all().await;
}
