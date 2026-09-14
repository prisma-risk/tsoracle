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

//! Integration tests for `load_leases` / `persist_leases` on the openraft driver.
//!
//! Covers:
//! - Gate: before write version 7 is activated, `persist_leases` returns `LeasesNotActivated` and `load_leases` returns an empty set.
//! - After activation, a write at the leader's epoch replaces the durable set wholesale and `load_leases` reads it back.
//! - A write carrying an epoch other than the term the entry commits in is fenced, returns `Fenced`, and leaves the set untouched.
//! - The set survives a restart through log replay, and a snapshot plus log purge through the snapshot.
//! - Three nodes: followers converge, a newly elected leader loads the committed set, and a write projected under the previous leader's term is fenced on the new leader.

mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use openraft::async_runtime::watch::WatchReceiver;
use openraft::storage::RaftLogStorage;
use openraft::{Config, SnapshotPolicy};
use tokio::time::timeout;
use tsoracle_consensus::{ConsensusDriver, ConsensusError, LeaderState};
use tsoracle_core::{Epoch, LeaseRecord};
use tsoracle_driver_openraft::{
    CapabilitySource, NodeCapabilities, OpenraftDriver, OpenraftLogCodec, OpenraftPeer,
    StandaloneHost, TypeConfig,
};
use tsoracle_openraft_toolkit::{
    BASELINE_WRITE_VERSION, Flat, LEASE_WRITE_VERSION, RocksdbLogStore,
};

use common::{
    TestCluster, build_single_node, build_single_node_with_config, build_three_node, eventually_eq,
    reopen_node, reopen_node_with_config,
};

/// A `CapabilitySource` for single-node clusters: the local node answers for itself, so any remote query is a test bug.
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

/// Every member runs this binary, so each answers with the local read range at its own active version.
struct SameBinarySource {
    active_versions: HashMap<u64, u8>,
}

#[async_trait]
impl CapabilitySource for SameBinarySource {
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
            .unwrap_or(BASELINE_WRITE_VERSION);
        Ok(NodeCapabilities::local(active))
    }
}

fn lease(lease_id: u64, holder: &[u8]) -> LeaseRecord {
    LeaseRecord {
        lease_id,
        holder: holder.to_vec(),
        holder_epoch: 1,
        ttl_ms: 20_000,
        ts_upper_bound: lease_id,
        expires_at_ms: lease_id + 20_000,
        superseded: false,
    }
}

/// Wait until `driver` reports leadership and return the epoch it reports, which is what the server would pass to `persist_leases`.
async fn leader_epoch(driver: &OpenraftDriver<StandaloneHost>) -> Epoch {
    let mut events = driver.leadership_events();
    timeout(Duration::from_secs(10), async {
        loop {
            if let LeaderState::Leader { epoch } = events.next().await.expect("event stream alive")
            {
                return epoch;
            }
        }
    })
    .await
    .expect("node became leader within 10s")
}

async fn activate_leases_single_node(cluster: &TestCluster) {
    let host = StandaloneHost::new(cluster.nodes[0].raft.clone(), cluster.nodes[0].sm.clone());
    host.initiate_format_activation(LEASE_WRITE_VERSION, &UnusedSource)
        .await
        .expect("lease activation must succeed on a single-node cluster");
    assert_eq!(host.active_write_version(), LEASE_WRITE_VERSION);
}

async fn find_leader_idx(cluster: &TestCluster, exclude: Option<usize>) -> usize {
    timeout(Duration::from_secs(10), async {
        loop {
            for (idx, node) in cluster.nodes.iter().enumerate() {
                if Some(idx) == exclude {
                    continue;
                }
                if let Some(leader) = node.raft.current_leader().await
                    && leader == node.id
                {
                    return idx;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("a leader was elected within 10s")
}

#[tokio::test(start_paused = true)]
async fn leases_are_refused_until_activation_then_replace_the_set() {
    let cluster = build_single_node().await;
    let driver = cluster.drivers[0].clone();
    let epoch = leader_epoch(&driver).await;

    // Before activation: reads are safe and empty, writes are refused with the operator-actionable variant.
    assert_eq!(
        driver.load_leases().await.expect("load before activation"),
        vec![]
    );
    let err = driver
        .persist_leases(&[lease(100, b"g1")], epoch)
        .await
        .expect_err("persist_leases before activation must be refused");
    assert!(
        matches!(
            err,
            ConsensusError::LeasesNotActivated { required, active }
            if required == LEASE_WRITE_VERSION && active < LEASE_WRITE_VERSION
        ),
        "expected LeasesNotActivated, got {err:?}"
    );

    activate_leases_single_node(&cluster).await;

    // Supplied out of order; stored and read back ordered by lease id.
    driver
        .persist_leases(&[lease(200, b"g2"), lease(100, b"g1")], epoch)
        .await
        .expect("persist at the leader's epoch");
    assert_eq!(
        driver.load_leases().await.expect("load"),
        vec![lease(100, b"g1"), lease(200, b"g2")]
    );

    // Absolute state, not a delta: the next write drops what it omits.
    driver
        .persist_leases(&[lease(300, b"g3")], epoch)
        .await
        .expect("replace");
    assert_eq!(
        driver.load_leases().await.expect("load"),
        vec![lease(300, b"g3")]
    );

    driver
        .persist_leases(&[], epoch)
        .await
        .expect("release all");
    assert_eq!(driver.load_leases().await.expect("load"), vec![]);
}

#[tokio::test(start_paused = true)]
async fn a_lease_write_under_another_term_is_fenced_and_changes_nothing() {
    let cluster = build_single_node().await;
    let driver = cluster.drivers[0].clone();
    let epoch = leader_epoch(&driver).await;
    activate_leases_single_node(&cluster).await;

    driver
        .persist_leases(&[lease(100, b"g1"), lease(200, b"g2")], epoch)
        .await
        .expect("persist at the leader's epoch");

    // The entry commits in the leader's term, so any other expected term is refused at apply. A later term stands for a set projected by a leadership that does not exist yet; an earlier one for a set projected before a flap.
    let term = u64::try_from(epoch.0).expect("raft term");
    for stale in [Epoch(u128::from(term + 1)), Epoch(u128::from(term - 1))] {
        let err = driver
            .persist_leases(&[lease(300, b"g3")], stale)
            .await
            .expect_err("a write under another term must be fenced");
        assert!(
            matches!(
                err,
                ConsensusError::Fenced { expected, current }
                if expected == stale && current == epoch
            ),
            "expected Fenced {{ expected: {stale:?}, current: {epoch:?} }}, got {err:?}"
        );
    }
    assert_eq!(
        driver.load_leases().await.expect("load"),
        vec![lease(100, b"g1"), lease(200, b"g2")],
        "a fenced write must leave the durable set untouched"
    );
}

#[tokio::test(start_paused = true)]
async fn the_lease_set_survives_restart_replay() {
    let cluster = build_single_node().await;
    let TestCluster {
        mut nodes, drivers, ..
    } = cluster;
    let driver = drivers[0].clone();
    let epoch = leader_epoch(&driver).await;

    let host = StandaloneHost::new(nodes[0].raft.clone(), nodes[0].sm.clone());
    host.initiate_format_activation(LEASE_WRITE_VERSION, &UnusedSource)
        .await
        .expect("activate");
    driver
        .persist_leases(&[lease(100, b"g1"), lease(200, b"g2")], epoch)
        .await
        .expect("persist");

    drop(host);
    drop(driver);
    drop(drivers);
    let reopened = reopen_node(nodes.remove(0)).await;
    let reopened_driver = OpenraftDriver::new(StandaloneHost::new(
        reopened.raft.clone(),
        reopened.sm.clone(),
    ));
    let new_epoch = leader_epoch(&reopened_driver).await;
    // A lone voter may resume leadership in the same term after a restart; it never comes back in an earlier one.
    assert!(new_epoch >= epoch, "a restart never regresses the term");

    // Replay restores both the set and the lease format, so the new leadership can keep writing.
    assert_eq!(
        reopened_driver
            .load_leases()
            .await
            .expect("load after restart"),
        vec![lease(100, b"g1"), lease(200, b"g2")]
    );
    reopened_driver
        .persist_leases(&[lease(300, b"g3")], new_epoch)
        .await
        .expect("persist after restart at the new epoch");
    assert_eq!(
        reopened_driver.load_leases().await.expect("load"),
        vec![lease(300, b"g3")]
    );
}

fn aggressive_snapshot_config() -> Arc<Config> {
    Arc::new(
        Config {
            heartbeat_interval: 50,
            election_timeout_min: 150,
            election_timeout_max: 300,
            // Snapshot every 4 entries and purge everything the snapshot covers.
            snapshot_policy: SnapshotPolicy::LogsSinceLast(4),
            max_in_snapshot_log_to_keep: 0,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    )
}

#[tokio::test(start_paused = true)]
async fn the_lease_set_survives_snapshot_and_log_purge() {
    let cluster = build_single_node_with_config(aggressive_snapshot_config()).await;
    let TestCluster {
        mut nodes, drivers, ..
    } = cluster;
    let driver = drivers[0].clone();
    let epoch = leader_epoch(&driver).await;

    let host = StandaloneHost::new(nodes[0].raft.clone(), nodes[0].sm.clone());
    host.initiate_format_activation(LEASE_WRITE_VERSION, &UnusedSource)
        .await
        .expect("activate");

    // Interleave lease writes with high-water advances so enough entries flow to trigger a snapshot and purge the log under it.
    for round in 1u64..=8 {
        driver
            .persist_high_water(round * 1_000, epoch)
            .await
            .expect("persist_high_water");
        driver
            .persist_leases(&[lease(100, b"g1"), lease(100 + round, b"g2")], epoch)
            .await
            .expect("persist_leases");
    }
    let expected = vec![lease(100, b"g1"), lease(108, b"g2")];
    assert_eq!(driver.load_leases().await.expect("load"), expected);

    let metrics = nodes[0].raft.metrics();
    let snapshot_log_id = timeout(Duration::from_secs(5), async {
        loop {
            if let Some(snapshot) = metrics.borrow_watched().snapshot {
                return snapshot;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("snapshot built within 5s");

    // Without a purge the reopened node could rebuild the set from the log alone and this test would not prove the snapshot carries it.
    let mut inspector: RocksdbLogStore<TypeConfig, Flat, OpenraftLogCodec> =
        RocksdbLogStore::open(nodes[0].db.clone(), "raft_log", "raft_meta", Flat).unwrap();
    timeout(Duration::from_secs(5), async {
        loop {
            let state = inspector.get_log_state().await.unwrap();
            if let Some(purged) = state.last_purged_log_id
                && purged.index >= snapshot_log_id.index
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("log purged up to the snapshot within 5s");
    drop(inspector);

    drop(host);
    drop(driver);
    drop(drivers);
    let reopened = reopen_node_with_config(nodes.remove(0), aggressive_snapshot_config()).await;
    // Rehydrated from the snapshot before any leadership or replay.
    assert!(
        reopened.sm.leases().contains(&lease(100, b"g1")),
        "the lease set must be restored from the snapshot"
    );

    let reopened_driver = OpenraftDriver::new(StandaloneHost::new(
        reopened.raft.clone(),
        reopened.sm.clone(),
    ));
    leader_epoch(&reopened_driver).await;
    assert_eq!(
        reopened_driver
            .load_leases()
            .await
            .expect("load after restart"),
        expected
    );
}

#[tokio::test(start_paused = true)]
async fn a_new_leader_loads_the_committed_set_and_fences_the_old_term() {
    let cluster = build_three_node().await;
    let partitions = cluster
        .partitions
        .as_ref()
        .expect("three-node cluster has partitions")
        .clone();

    let old_idx = find_leader_idx(&cluster, None).await;
    // openraft accepts writes only once a quorum has confirmed the new leader's lease.
    common::wait_until_writable(&cluster.nodes[old_idx].raft).await;
    let old_id = cluster.nodes[old_idx].id;
    let old_epoch = leader_epoch(&cluster.drivers[old_idx]).await;

    let source = SameBinarySource {
        active_versions: cluster
            .nodes
            .iter()
            .map(|node| (node.id, BASELINE_WRITE_VERSION))
            .collect(),
    };
    StandaloneHost::new(
        cluster.nodes[old_idx].raft.clone(),
        cluster.nodes[old_idx].sm.clone(),
    )
    .initiate_format_activation(LEASE_WRITE_VERSION, &source)
    .await
    .expect("all members can read the lease format");

    let committed = vec![lease(100, b"g1"), lease(200, b"g2")];
    cluster.drivers[old_idx]
        .persist_leases(&committed, old_epoch)
        .await
        .expect("persist on the leader");
    for node in &cluster.nodes {
        let sm = node.sm.clone();
        let expected = committed.clone();
        eventually_eq(expected, Duration::from_secs(5), || {
            let sm = sm.clone();
            async move { sm.leases() }
        })
        .await;
    }

    // Fail over: isolate the leader and let the majority elect a new one in a later term.
    partitions.isolate(old_id);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let new_idx = find_leader_idx(&cluster, Some(old_idx)).await;
    common::wait_until_writable(&cluster.nodes[new_idx].raft).await;
    let new_epoch = leader_epoch(&cluster.drivers[new_idx]).await;
    assert!(new_epoch > old_epoch, "the new leader serves a later term");

    // What the server's fence reads on the new leader: exactly the committed set.
    assert_eq!(
        cluster.drivers[new_idx]
            .load_leases()
            .await
            .expect("load on the new leader"),
        committed
    );

    // A set projected under the old term reaching the new leader is refused, whatever it contains.
    let err = cluster.drivers[new_idx]
        .persist_leases(&[lease(300, b"g3")], old_epoch)
        .await
        .expect_err("a write under the previous leader's term must be fenced");
    assert!(
        matches!(err, ConsensusError::Fenced { expected, current } if expected == old_epoch && current == new_epoch),
        "expected Fenced, got {err:?}"
    );

    cluster.drivers[new_idx]
        .persist_leases(&[lease(100, b"g1")], new_epoch)
        .await
        .expect("persist at the new epoch");

    // The old leader rejoins as a follower and converges on the new leader's set.
    partitions.heal(old_id);
    let sm = cluster.nodes[old_idx].sm.clone();
    eventually_eq(vec![lease(100, b"g1")], Duration::from_secs(10), || {
        let sm = sm.clone();
        async move { sm.leases() }
    })
    .await;
}
