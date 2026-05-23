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

//! Single-runner sanity coverage.
//!
//! OmniPaxos 0.2 rejects a [`ClusterConfig`] with fewer than 3 nodes, so
//! "single node" here means a 3-node config in which only one runner is
//! ticking. Without quorum the runner cannot elect a leader, cannot decide
//! anything, and must not panic — this test confirms it stays inert.
//!
//! Then the same node count under `start_all` reaches a stable leader,
//! decides an `Advance`, and converges across all replicas.

use std::time::Duration;

use tsoracle_driver_paxos::HighWaterCommand;

#[path = "common/mod.rs"]
mod common;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_started_runner_stays_inert_without_quorum() {
    let mut cluster = common::build_mem_cluster(3);

    // Start only node 1's runner + pump. Nodes 2 and 3 exist in the
    // ClusterConfig but never tick, so no quorum is reachable.
    let sink = std::sync::Arc::new(common::MeshSink::new(cluster.network.clone()));
    let host = cluster.node_mut(1).host.as_mut().expect("host present");
    host.start(sink);

    // Give the single runner a healthy window to flap around. Without peer
    // responses no entry can be decided and `decided_idx` stays at 0; the
    // apply state machine never observes a folded `Advance`. OmniPaxos's
    // BLE may unilaterally observe itself as a leader candidate before a
    // remote promise lands, so we do NOT assert leadership is absent —
    // the contract under test is "runner stays in a sane state, nothing
    // decides, no panic."
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_eq!(cluster.decided_idx_on(1), 0, "decided_idx stays at 0");
    assert_eq!(
        cluster.high_water_on(1),
        0,
        "the apply state never observes a decided entry"
    );

    // Tear down the lone running host without touching the others.
    let mut host = cluster.node_mut(1).host.take().expect("host present");
    host.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_runners_advance_then_converge() {
    let mut cluster = common::build_mem_cluster(3);
    cluster.start_all();

    cluster
        .drive_until(common::some_leader_elected(), 1_000)
        .await;
    let leader_id = cluster.leader();

    // Append directly on the leader's OmniPaxos handle. The harness's
    // apply task picks it up via `apply_notify` after the runner ticks.
    cluster
        .node(leader_id)
        .omnipaxos()
        .lock()
        .append(HighWaterCommand::Advance { at_least: 25 })
        .expect("append succeeds on leader");

    cluster
        .drive_until(common::all_decided_at_least(1), 1_000)
        .await;

    // Give every apply task a couple of ticks to drain (the apply task
    // wakes on the runner's tick — not on the decided-idx transition
    // directly — so a one-tick window after decided_idx advances is
    // enough for the in-memory state to catch up on every node).
    cluster
        .drive_until(
            |state| {
                state
                    .nodes
                    .iter()
                    .all(|node| state.high_water_on(node.node_id) >= 25)
            },
            1_000,
        )
        .await;

    for node in &cluster.nodes {
        assert_eq!(
            cluster.high_water_on(node.node_id),
            25,
            "node {} converged to current_value 25",
            node.node_id
        );
    }

    cluster.stop_all().await;
}
