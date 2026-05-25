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

//! End-to-end smoke: a 3-node `MemNetwork`-routed cluster reaches consensus
//! on a single appended command and converges on a single decided_idx.

#[path = "common/mod.rs"]
mod common;

use common::{TestCommand, build_mem_cluster, drive_cluster_until};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_cluster_reaches_consensus() {
    let mut cluster = build_mem_cluster(3);

    // Step the cluster until a leader emerges (any node observes one).
    drive_cluster_until(
        &mut cluster,
        |state| {
            state
                .nodes
                .iter()
                .any(|node| node.omnipaxos.lock().get_current_leader().is_some())
        },
        500,
    )
    .await;

    // Append a command via whichever node is leader.
    let leader_id = cluster
        .nodes
        .iter()
        .find_map(|node| node.omnipaxos.lock().get_current_leader())
        .expect("leader exists after election");
    let leader_node = cluster
        .nodes
        .iter()
        .find(|node| node.node_id == leader_id)
        .expect("leader node in topology");
    leader_node
        .omnipaxos
        .lock()
        .append(TestCommand(42))
        .expect("append");

    // Step until decided_idx >= 1 on every node.
    drive_cluster_until(
        &mut cluster,
        |state| {
            state
                .nodes
                .iter()
                .all(|node| node.omnipaxos.lock().get_decided_idx() >= 1)
        },
        500,
    )
    .await;

    for node in &cluster.nodes {
        assert!(
            node.omnipaxos.lock().get_decided_idx() >= 1,
            "node {} should have observed the commit",
            node.node_id,
        );
    }
}
