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

//! Single-runner sanity coverage.
//!
//! OmniPaxos 0.2 rejects a [`ClusterConfig`] with fewer than 3 nodes, so
//! "single node" here means a 3-node config in which only one runner is
//! ticking. Without quorum the runner cannot elect a leader, cannot decide
//! anything, and must not panic — this test confirms it stays inert.
//!
//! Then the full 3-node cluster reaches a stable leader, decides an
//! `Advance`, and converges across all replicas.
//!
//! Both tests use the deterministic step-driver (`step` / `step_until`)
//! instead of async runner/pump tasks + real-time `drive_until`, so the
//! liveness contracts are exercised without wall-clock variance.

use tsoracle_consensus::AdvancePayload;
use tsoracle_driver_paxos::HighWaterCommand;

#[path = "common/mod.rs"]
mod common;

#[tokio::test]
async fn single_started_runner_stays_inert_without_quorum() {
    let mut cluster = common::build_mem_cluster(3);

    // Take nodes 2 and 3's hosts out so only node 1 steps. They remain in
    // the ClusterConfig but never tick (the step-driver skips host-less
    // nodes), so no quorum is reachable and node 1's prepares go
    // unanswered.
    cluster.node_mut(2).host.take();
    cluster.node_mut(3).host.take();

    // Step the lone node well past an election timeout. Without peer
    // responses no entry can be decided and `decided_idx` stays at 0; the
    // apply state machine never observes a folded `Advance`. OmniPaxos's
    // BLE may unilaterally observe itself as a leader candidate before a
    // remote promise lands, so we do NOT assert leadership is absent —
    // the contract under test is "runner stays in a sane state, nothing
    // decides, no panic."
    for _ in 0..200 {
        cluster.step();
    }

    assert_eq!(cluster.decided_idx_on(1), 0, "decided_idx stays at 0");
    assert_eq!(
        cluster.high_water_on(1),
        0,
        "the apply state never observes a decided entry"
    );
}

#[tokio::test]
async fn three_runners_advance_then_converge() {
    let mut cluster = common::build_mem_cluster(3);

    cluster.step_until(common::some_leader_elected(), 1_000);
    let leader_id = cluster.leader();

    // Append directly on the leader's OmniPaxos handle. `step` folds it via
    // `apply_once` once consensus decides it.
    cluster
        .node(leader_id)
        .omnipaxos()
        .lock()
        .append(HighWaterCommand::Advance(AdvancePayload { at_least: 25 }))
        .expect("append succeeds on leader");

    cluster.step_until(common::all_decided_at_least(1), 1_000);

    cluster.step_until(
        |state| {
            state
                .nodes
                .iter()
                .all(|node| state.high_water_on(node.node_id) >= 25)
        },
        1_000,
    );

    for node in &cluster.nodes {
        assert_eq!(
            cluster.high_water_on(node.node_id),
            25,
            "node {} converged to current_value 25",
            node.node_id
        );
    }
}
