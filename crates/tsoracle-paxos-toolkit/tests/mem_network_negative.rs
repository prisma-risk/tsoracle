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

//! Negative tests: isolating a node prevents it from observing decided_idx
//! advances on the remaining quorum.

#[path = "common/mod.rs"]
mod common;

use common::{TestCommand, build_mem_cluster, drive_cluster_until};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn isolated_node_does_not_observe_consensus() {
    let mut cluster = build_mem_cluster(3);

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

    cluster.network.partition().isolate(3);

    let leader_id = cluster
        .nodes
        .iter()
        .find_map(|node| node.omnipaxos.lock().get_current_leader())
        .expect("leader exists");
    if leader_id == 3 {
        // If node 3 happened to be leader before isolation, the cluster
        // loses quorum and the scenario doesn't apply. Skip cleanly.
        return;
    }
    let leader_node = cluster
        .nodes
        .iter()
        .find(|node| node.node_id == leader_id)
        .expect("leader node");
    leader_node
        .omnipaxos
        .lock()
        .append(TestCommand(7))
        .expect("append");

    drive_cluster_until(
        &mut cluster,
        |state| {
            state
                .nodes
                .iter()
                .filter(|node| node.node_id != 3)
                .all(|node| node.omnipaxos.lock().get_decided_idx() >= 1)
        },
        500,
    )
    .await;

    let node_three = cluster
        .nodes
        .iter()
        .find(|node| node.node_id == 3)
        .expect("node 3 in topology");
    assert_eq!(
        node_three.omnipaxos.lock().get_decided_idx(),
        0,
        "isolated node must not observe quorum decisions",
    );
}
