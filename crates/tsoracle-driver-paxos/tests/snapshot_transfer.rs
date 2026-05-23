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

//! Snapshot transfer to a joining node.
//!
//! Boots a 3-node cluster with `SnapshotPolicy::every(10)`, isolates one
//! node from the get-go so the other two form a quorum and decide 20
//! Advances + trigger snapshots. The isolated node's OmniPaxos has
//! been ticking but seeing no incoming messages, so its decided_idx
//! stays at 0 and its log stays empty.
//!
//! On heal, the cluster's runners replicate to the recovering node.
//! Because the leader's log has been trimmed past the joining node's
//! decided_idx, the only way forward is for the leader to ship a
//! snapshot — OmniPaxos's snapshot transfer machinery. The joining
//! node's storage must end up with a non-empty `get_snapshot()` and
//! the in-memory high-water must converge to the cluster's value.

use tsoracle_driver_paxos::{HighWaterCommand, SnapshotPolicy};

#[path = "common/mod.rs"]
mod common;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn isolated_node_catches_up_via_snapshot_transfer() {
    let mut cluster = common::build_mem_cluster_with_policy(3, SnapshotPolicy::every(10));

    // Isolate node 3 BEFORE start_all so it never participates in the
    // initial election or in any of the 20 decisions.
    let joining_id: u64 = 3;
    cluster.network.partition().isolate(joining_id);

    cluster.start_all();

    // Drive until a leader emerges among the reachable 2-node majority
    // (nodes 1 and 2).
    cluster
        .drive_until(
            |state| {
                state
                    .nodes
                    .iter()
                    .filter(|node| node.node_id != joining_id)
                    .any(|node| node.omnipaxos().lock().get_current_leader().is_some())
            },
            3_000,
        )
        .await;
    let leader_id = cluster
        .nodes
        .iter()
        .filter(|node| node.node_id != joining_id)
        .find_map(|node| node.omnipaxos().lock().get_current_leader())
        .expect("leader elected on the 2-node majority");
    assert_ne!(leader_id, joining_id);

    // Append 20 Advances. The reachable majority decides each one and
    // the snapshot policy fires at decided_idx >= 10 and >= 20.
    for n in 1..=20u64 {
        cluster
            .node(leader_id)
            .omnipaxos()
            .lock()
            .append(HighWaterCommand::Advance { at_least: n })
            .expect("append succeeds on leader");
    }

    cluster
        .drive_until(
            |state| {
                state
                    .nodes
                    .iter()
                    .filter(|node| node.node_id != joining_id)
                    .all(|node| state.high_water_on(node.node_id) >= 20)
            },
            3_000,
        )
        .await;

    // Sanity: the leader's log has been compacted past index 10 — there
    // is no way to replicate the first ten entries by log replay alone.
    let leader_compacted = cluster
        .node(leader_id)
        .omnipaxos()
        .lock()
        .get_compacted_idx();
    assert!(
        leader_compacted >= 10,
        "leader's compacted_idx must reflect at least one snapshot trigger (saw {leader_compacted})",
    );

    // Joining node must still be at decided_idx 0 — it heard nothing
    // during the isolation window.
    let joining_decided_before_heal = cluster.decided_idx_on(joining_id);
    assert_eq!(
        joining_decided_before_heal, 0,
        "isolated node sees no decisions before heal",
    );

    // Heal the partition. The leader resends to the joining node and
    // OmniPaxos's snapshot-transfer path kicks in.
    cluster.network.partition().heal();

    cluster
        .drive_until(|state| state.high_water_on(joining_id) >= 20, 5_000)
        .await;

    // Snapshot transfer observable: the joining node's storage now has
    // a snapshot installed. (Plain log replay would never write the
    // snapshot slot.)
    let joining_snapshot = cluster
        .node(joining_id)
        .omnipaxos()
        .lock()
        .get_compacted_idx();
    assert!(
        joining_snapshot >= 10,
        "joining node's compacted_idx must advance via a transferred snapshot (saw {joining_snapshot})",
    );

    assert_eq!(cluster.high_water_on(joining_id), 20);

    cluster.stop_all().await;
}
