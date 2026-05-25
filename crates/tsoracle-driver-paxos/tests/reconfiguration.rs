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

//! Reconfiguration (stopsign) coverage.
//!
//! Boots a 3-node cluster, decides a few Advances, then triggers a
//! `reconfigure(...)` to a 5-node config via OmniPaxos's stopsign
//! mechanism. Verifies:
//!
//! - The stopsign decides on every existing node (each node's
//!   `is_reconfigured()` returns `Some(_)`).
//! - The stopsign's `next_config.configuration_id` matches the new
//!   config_id and `next_config.nodes` lists the 5-node membership.
//! - Append on the old config after the stopsign decides is rejected
//!   with a `ProposeErr` — the contract that the old config is sealed.
//! - The in-memory high-water still reflects the pre-stopsign Advances
//!   on every existing node (the apply task continues to process
//!   already-decided entries).
//!
//! Full 3-to-5 node bring-up under the new configuration_id requires
//! state transfer machinery the driver does not yet wire up (the new
//! nodes would need a snapshot of the pre-stopsign log to begin). The
//! intra-config behavior verified here is the load-bearing contract
//! for the stopsign path.

use tsoracle_driver_paxos::AdvancePayload;
use tsoracle_driver_paxos::HighWaterCommand;

use omnipaxos::ClusterConfig;
use omnipaxos::util::LogEntry;

#[path = "common/mod.rs"]
mod common;

// Driven by the deterministic step-driver (`step_until`).
#[tokio::test]
async fn stopsign_decides_and_seals_old_configuration() {
    let mut cluster = common::build_mem_cluster(3);

    cluster.step_until(common::some_leader_elected(), 2_000);
    let leader_id = cluster.leader();

    // Decide a few Advances under the original 3-node config.
    cluster
        .node(leader_id)
        .omnipaxos()
        .lock()
        .append(HighWaterCommand::Advance(AdvancePayload { at_least: 5 }))
        .expect("pre-stopsign append succeeds");
    cluster
        .node(leader_id)
        .omnipaxos()
        .lock()
        .append(HighWaterCommand::Advance(AdvancePayload { at_least: 17 }))
        .expect("pre-stopsign append succeeds");
    cluster.step_until(
        |state| {
            state
                .nodes
                .iter()
                .all(|node| state.high_water_on(node.node_id) >= 17)
        },
        2_000,
    );

    // Issue the reconfiguration. The new 5-node config takes effect once
    // the stopsign is decided cluster-wide.
    let new_config = ClusterConfig {
        configuration_id: 2,
        nodes: vec![1, 2, 3, 4, 5],
        flexible_quorum: None,
    };
    cluster
        .node(leader_id)
        .omnipaxos()
        .lock()
        .reconfigure(new_config.clone(), None)
        .expect("reconfigure submits a stopsign proposal");

    // Drive until every node has decided the stopsign.
    cluster.step_until(
        |state| {
            state
                .nodes
                .iter()
                .all(|node| node.omnipaxos().lock().is_reconfigured().is_some())
        },
        3_000,
    );

    // Every node must agree on the next configuration's metadata.
    for node in &cluster.nodes {
        let stopsign = node
            .omnipaxos()
            .lock()
            .is_reconfigured()
            .expect("stopsign decided on this node");
        assert_eq!(
            stopsign.next_config.configuration_id, new_config.configuration_id,
            "next configuration_id matches on node {}",
            node.node_id
        );
        assert_eq!(
            stopsign.next_config.nodes, new_config.nodes,
            "next config node list matches on node {}",
            node.node_id
        );
    }

    // The leader's last decided entry must be the stopsign.
    let leader_last_entry = cluster
        .node(leader_id)
        .omnipaxos()
        .lock()
        .read_decided_suffix(0)
        .expect("decided suffix readable")
        .into_iter()
        .last()
        .expect("at least one decided entry");
    match leader_last_entry {
        LogEntry::StopSign(stopsign, decided) => {
            assert!(decided, "the last decided entry is a decided stopsign");
            assert_eq!(stopsign.next_config, new_config);
        }
        other => panic!("expected StopSign as last decided entry, got {other:?}"),
    }

    // Appending after the stopsign decides must be rejected on every
    // existing node — the old configuration is sealed.
    for node in &cluster.nodes {
        let result = node
            .omnipaxos()
            .lock()
            .append(HighWaterCommand::Advance(AdvancePayload { at_least: 99 }));
        assert!(
            result.is_err(),
            "append on node {} after stopsign must be rejected",
            node.node_id
        );
    }

    // The pre-stopsign high-water remains visible — the apply task
    // continues to process the entries decided before the stopsign.
    for node in &cluster.nodes {
        assert_eq!(
            cluster.high_water_on(node.node_id),
            17,
            "node {} preserves pre-stopsign high-water across the reconfiguration",
            node.node_id
        );
    }

    cluster.stop_all().await;
}
