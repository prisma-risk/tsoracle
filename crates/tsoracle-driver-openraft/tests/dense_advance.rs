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

//! Integration tests for `advance_dense` / `load_dense_seq` on the openraft
//! driver.
//!
//! Covers:
//! - Gate: before activation (write version 4), `advance_dense` returns
//!   `DenseNotActivated { required: 5, active: 4 }`.
//! - Post-activation gapless per-key advance: consecutive calls return
//!   gapless block starts; `load_dense_seq` reflects the committed counter.
//! - Independent keys start at 0.
//! - Three-node: after leader activation, a follower converges on
//!   `load_dense_seq`.
//!
//! Cardinality (`SeqKeyCardinalityExceeded`) and overflow (`SeqOverflow`) are
//! covered deterministically by the state-machine unit tests in
//! `state_machine.rs` (which can inject a small genesis cap via the
//! `#[cfg(test)]` `new_with_dense_cap` constructor). The `DEFAULT_DENSE_CARDINALITY_CAP`
//! (10 000) cannot be exhausted in an integration test without prohibitive
//! runtime, so the driver's routing of those outcomes to the correct
//! `ConsensusError` variants is verified at the unit level instead.

mod common;

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use tokio::time::timeout;
use tsoracle_consensus::{ConsensusDriver, ConsensusError, LeaderState};
use tsoracle_core::{Epoch, SeqKey};
use tsoracle_driver_openraft::{
    CapabilitySource, NodeCapabilities, OpenraftHighWaterHost, OpenraftPeer, StandaloneHost,
};
use tsoracle_openraft_toolkit::DENSE_WRITE_VERSION;

use common::{TestCluster, build_single_node, build_three_node, eventually_eq};

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// A `CapabilitySource` for single-node clusters that must never be called:
/// the local node short-circuit answers entirely from itself without any peer
/// RPCs.
struct UnusedSource;

#[async_trait]
impl CapabilitySource for UnusedSource {
    type Node = OpenraftPeer;

    async fn query(
        &self,
        node_id: u64,
        _member: &OpenraftPeer,
    ) -> Result<NodeCapabilities, String> {
        panic!("single-node gate must not query remote node {node_id}");
    }
}

/// A `CapabilitySource` that answers every queried node with the compile-time
/// local capabilities at the given `active_write_version`. Used for three-node
/// tests where every node runs the same binary.
struct AllCapableSource {
    active_versions: HashMap<u64, u8>,
}

impl AllCapableSource {
    fn uniform(active_write_version: u8, node_ids: &[u64]) -> Self {
        Self {
            active_versions: node_ids
                .iter()
                .copied()
                .map(|id| (id, active_write_version))
                .collect(),
        }
    }
}

#[async_trait]
impl CapabilitySource for AllCapableSource {
    type Node = OpenraftPeer;

    async fn query(
        &self,
        node_id: u64,
        _member: &OpenraftPeer,
    ) -> Result<NodeCapabilities, String> {
        let active = self
            .active_versions
            .get(&node_id)
            .copied()
            .unwrap_or(tsoracle_openraft_toolkit::BASELINE_WRITE_VERSION);
        Ok(NodeCapabilities::local(active))
    }
}

/// Wait until the cluster has elected a leader; return the leader node index.
async fn find_leader_idx(cluster: &TestCluster) -> usize {
    timeout(Duration::from_secs(10), async {
        loop {
            for (idx, node) in cluster.nodes.iter().enumerate() {
                if let Some(l) = node.raft.current_leader().await {
                    if l == node.id {
                        return idx;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("some node became leader within 10s")
}

/// Build a single-node cluster, wait for leadership, and return the
/// `StandaloneHost` handle together with the cluster guard.
async fn single_node_leader_host() -> (StandaloneHost, TestCluster) {
    let cluster = build_single_node().await;
    let driver = &cluster.drivers[0];

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
    .expect("single-node openraft did not elect itself within 5s");

    let host = StandaloneHost::new(cluster.nodes[0].raft.clone(), cluster.nodes[0].sm.clone());
    (host, cluster)
}

// ---------------------------------------------------------------------------
// Tests: gate — before activation
// ---------------------------------------------------------------------------

/// Before write version 5 is activated, `advance_dense` must return
/// `DenseNotActivated { required: 5, active: 4 }`. `load_dense_seq` is not
/// gated (reads are always safe; the key simply does not exist yet and returns
/// 0).
#[tokio::test(start_paused = true)]
async fn advance_dense_before_activation_returns_dense_not_activated() {
    let cluster = build_single_node().await;
    let driver = &cluster.drivers[0];

    // Wait for leadership so the driver is usable.
    let mut events = driver.leadership_events();
    timeout(Duration::from_secs(5), async {
        loop {
            let s = events.next().await.expect("event stream alive");
            if matches!(s, LeaderState::Leader { .. }) {
                break;
            }
        }
    })
    .await
    .expect("became leader within 5s");

    let key = SeqKey::try_new("orders").unwrap();

    // `advance_dense` must be refused before activation.
    let err = driver
        .advance_dense(&key, 1, Epoch::ZERO)
        .await
        .expect_err("advance_dense before activation must return an error");

    assert!(
        matches!(
            err,
            ConsensusError::DenseNotActivated { required, active }
            if required == DENSE_WRITE_VERSION && active < DENSE_WRITE_VERSION
        ),
        "expected DenseNotActivated{{required=5, active<5}}, got: {err:?}"
    );
}

// ---------------------------------------------------------------------------
// Tests: post-activation — gapless advance, independent keys
// ---------------------------------------------------------------------------

/// After activating write version 5, `advance_dense` returns gapless block
/// starts; `load_dense_seq` reflects the committed counter; an independent
/// key starts at 0.
#[tokio::test(start_paused = true)]
async fn advance_dense_after_activation_is_gapless() {
    let (host, cluster) = single_node_leader_host().await;
    let driver = &cluster.drivers[0];

    // Activate write version 5 through the real gate. Single-node
    // short-circuits the all-members check, so `UnusedSource` is never called.
    host.initiate_format_activation(DENSE_WRITE_VERSION, &UnusedSource)
        .await
        .expect("activation must succeed on a single-node cluster");
    assert_eq!(
        host.active_write_version(),
        DENSE_WRITE_VERSION,
        "cell must read DENSE_WRITE_VERSION after activation"
    );

    let key = SeqKey::try_new("orders").unwrap();

    // First advance of 5: start = 0 (key did not exist).
    let start0 = driver
        .advance_dense(&key, 5, Epoch::ZERO)
        .await
        .expect("advance after activation");
    assert_eq!(start0, 0, "first advance starts at 0");

    // Second advance of 3: start = 5 (previous end).
    let start1 = driver
        .advance_dense(&key, 3, Epoch::ZERO)
        .await
        .expect("second advance after activation");
    assert_eq!(start1, 5, "second advance starts where first ended");

    // `load_dense_seq` must reflect the committed counter (0 + 5 + 3 = 8).
    let seq = driver
        .load_dense_seq(&key)
        .await
        .expect("load_dense_seq must succeed");
    assert_eq!(seq, 8, "load_dense_seq reflects cumulative count");

    // An independent key must start at 0, gapless from the beginning.
    let key2 = SeqKey::try_new("users").unwrap();
    let start2 = driver
        .advance_dense(&key2, 1, Epoch::ZERO)
        .await
        .expect("advance for independent key");
    assert_eq!(start2, 0, "independent key starts at 0");

    // A second call on the independent key starts at 1.
    let start3 = driver
        .advance_dense(&key2, 4, Epoch::ZERO)
        .await
        .expect("second advance for independent key");
    assert_eq!(start3, 1, "second advance on independent key starts at 1");
}

// ---------------------------------------------------------------------------
// Tests: three-node follower convergence
// ---------------------------------------------------------------------------

/// After the leader activates write version 5 and advances a key, a follower
/// converges on the dense counter (read directly from its SM, since followers
/// cannot issue a linearizable read barrier without being the leader).
#[tokio::test(start_paused = true)]
async fn three_node_follower_converges_on_dense_seq() {
    let cluster = build_three_node().await;

    let leader_idx = find_leader_idx(&cluster).await;
    let follower_idx = (0..3)
        .find(|i| *i != leader_idx)
        .expect("at least one follower");

    let leader_node = &cluster.nodes[leader_idx];
    let leader_driver = &cluster.drivers[leader_idx];
    let follower_sm = cluster.nodes[follower_idx].sm.clone();

    // Activate write version 5 on the leader. In a three-node cluster, the
    // all-members gate requires all three nodes to report
    // max_readable_version >= 5. Every node runs the same binary
    // (MAX_READABLE_VERSION = 5), so the AllCapableSource answers
    // truthfully for every remote peer.
    let host = StandaloneHost::new(leader_node.raft.clone(), leader_node.sm.clone());
    let source = AllCapableSource::uniform(
        tsoracle_openraft_toolkit::BASELINE_WRITE_VERSION,
        &[1, 2, 3],
    );
    host.initiate_format_activation(DENSE_WRITE_VERSION, &source)
        .await
        .expect("three-node activation must succeed when all members can read v5");

    // Advance "orders" by 7 through the leader.
    let key = SeqKey::try_new("orders").unwrap();
    let start = leader_driver
        .advance_dense(&key, 7, Epoch::ZERO)
        .await
        .expect("advance_dense on leader after activation");
    assert_eq!(start, 0, "first advance starts at 0");

    // Wait for the follower's SM to replicate and apply the dense entry.
    let key_str = key.as_str().to_owned();
    eventually_eq(7u64, Duration::from_secs(5), || {
        let sm = follower_sm.clone();
        let k = key_str.clone();
        async move { sm.dense_value(&k) }
    })
    .await;

    // The leader's `load_dense_seq` (linearizable) returns 7.
    let seq = leader_driver
        .load_dense_seq(&key)
        .await
        .expect("load_dense_seq on leader");
    assert_eq!(seq, 7, "leader load_dense_seq = 7 after advance by 7");
}

// ---------------------------------------------------------------------------
// Tests: advance_dense_batch — post-activation via StandaloneHost
// ---------------------------------------------------------------------------

/// After activating write version 6 (BATCH_WRITE_VERSION), `advance_dense_batch`
/// on a single-node cluster must return gapless block starts for each key in
/// the batch. `load_dense_seq` must reflect the committed counters.
///
/// The test activates version 5 first (required before version 6), then
/// activates version 6, and issues a two-key batch. Each key's start must be
/// gapless from its own counter (starting at 0 for new keys), and a second
/// batch call must produce contiguous block starts.
#[tokio::test(start_paused = true)]
async fn advance_dense_batch_after_activation_is_gapless() {
    use tsoracle_openraft_toolkit::BATCH_WRITE_VERSION;

    let (host, cluster) = single_node_leader_host().await;
    let driver = &cluster.drivers[0];

    // Activate DENSE_WRITE_VERSION (5) first — required stepping-stone.
    host.initiate_format_activation(DENSE_WRITE_VERSION, &UnusedSource)
        .await
        .expect("dense (v5) activation must succeed on a single-node cluster");

    // Now activate BATCH_WRITE_VERSION (6).
    host.initiate_format_activation(BATCH_WRITE_VERSION, &UnusedSource)
        .await
        .expect("batch (v6) activation must succeed on a single-node cluster");

    assert_eq!(
        host.active_write_version(),
        BATCH_WRITE_VERSION,
        "cell must read BATCH_WRITE_VERSION after activation"
    );

    let key_orders = SeqKey::try_new("orders").unwrap();
    let key_invoices = SeqKey::try_new("invoices").unwrap();

    // First batch: advance "orders" by 5 and "invoices" by 3, both new keys.
    let starts = host
        .submit_advance_dense_batch(&[(key_orders.clone(), 5), (key_invoices.clone(), 3)])
        .await
        .expect("batch advance after batch activation");
    assert_eq!(starts.len(), 2, "one start per batch entry");
    assert_eq!(starts[0], 0, "orders: first advance starts at 0");
    assert_eq!(
        starts[1], 0,
        "invoices: first advance starts at 0 (independent)"
    );

    // `load_dense_seq` must reflect the committed counters.
    let seq_orders = driver
        .load_dense_seq(&key_orders)
        .await
        .expect("load_dense_seq for orders");
    assert_eq!(seq_orders, 5, "orders counter = 5 after batch advance");
    let seq_invoices = driver
        .load_dense_seq(&key_invoices)
        .await
        .expect("load_dense_seq for invoices");
    assert_eq!(seq_invoices, 3, "invoices counter = 3 after batch advance");

    // Second batch on the same keys: starts must be contiguous with the first.
    let starts2 = host
        .submit_advance_dense_batch(&[(key_orders.clone(), 2), (key_invoices.clone(), 4)])
        .await
        .expect("second batch advance");
    assert_eq!(starts2.len(), 2);
    assert_eq!(
        starts2[0], 5,
        "orders: second advance starts where first ended"
    );
    assert_eq!(
        starts2[1], 3,
        "invoices: second advance starts where first ended"
    );
}
