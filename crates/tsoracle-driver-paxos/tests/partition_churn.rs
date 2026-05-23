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

//! Partition + heal coverage.
//!
//! Boots a 3-node cluster, decides an Advance, isolates the current
//! leader via [`PartitionController::isolate`], drives until the
//! remaining majority elects a new leader, decides a higher Advance,
//! heals the partition, and asserts the entire cluster converges to the
//! higher value. Confirms the driver's monotonic-advance contract under
//! a leader churn event with no consensus regressions.

use tsoracle_driver_paxos::HighWaterCommand;

#[path = "common/mod.rs"]
mod common;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partition_then_heal_preserves_monotonic_high_water() {
    let mut cluster = common::build_mem_cluster(3);
    cluster.start_all();

    cluster
        .drive_until(common::some_leader_elected(), 1_000)
        .await;
    let original_leader = cluster.leader();

    cluster
        .node(original_leader)
        .omnipaxos
        .lock()
        .append(HighWaterCommand::Advance { at_least: 100 })
        .expect("first append succeeds on leader");

    // Wait for cluster-wide convergence on 100.
    cluster
        .drive_until(
            |state| {
                state
                    .nodes
                    .iter()
                    .all(|node| state.high_water_on(node.node_id) >= 100)
            },
            1_500,
        )
        .await;

    // Isolate the original leader. Outbound + inbound traffic for that
    // node is now dropped on the shared MemNetwork.
    cluster.network.partition().isolate(original_leader);

    // Drive until a new leader emerges among the two reachable nodes.
    // Election timeouts add latency under the in-memory transport, so
    // give this plenty of headroom.
    cluster
        .drive_until(
            |state| {
                state
                    .nodes
                    .iter()
                    .filter(|node| node.node_id != original_leader)
                    .any(|node| {
                        node.omnipaxos
                            .lock()
                            .get_current_leader()
                            .is_some_and(|leader| leader != original_leader)
                    })
            },
            3_000,
        )
        .await;
    let new_leader = cluster
        .nodes
        .iter()
        .filter(|node| node.node_id != original_leader)
        .find_map(|node| {
            node.omnipaxos
                .lock()
                .get_current_leader()
                .filter(|leader| *leader != original_leader)
        })
        .expect("new leader elected among the remaining majority");
    assert_ne!(new_leader, original_leader);

    cluster
        .node(new_leader)
        .omnipaxos
        .lock()
        .append(HighWaterCommand::Advance { at_least: 200 })
        .expect("second append succeeds on the new leader");

    // Wait for the two reachable nodes to commit the new value.
    cluster
        .drive_until(
            |state| {
                state
                    .nodes
                    .iter()
                    .filter(|node| node.node_id != original_leader)
                    .all(|node| state.high_water_on(node.node_id) >= 200)
            },
            3_000,
        )
        .await;

    // Heal the partition. The isolated old leader rejoins the cluster
    // and must catch up to high_water = 200.
    cluster.network.partition().heal();

    cluster
        .drive_until(
            |state| {
                state
                    .nodes
                    .iter()
                    .all(|node| state.high_water_on(node.node_id) >= 200)
            },
            3_000,
        )
        .await;

    for node in &cluster.nodes {
        let high_water = cluster.high_water_on(node.node_id);
        assert!(
            high_water >= 200,
            "node {} converged to at least 200 (saw {high_water}); the heal must preserve monotonic-advance",
            node.node_id,
        );
    }

    cluster.stop_all().await;
}
