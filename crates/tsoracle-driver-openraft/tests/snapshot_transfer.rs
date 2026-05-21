//! Forces a full-snapshot install over `MemNetwork`.
//!
//! Stands up a 3-voter cluster with an aggressive snapshot policy
//! (`LogsSinceLast(4)` + `max_in_snapshot_log_to_keep = 0`), isolates a
//! follower, bumps the high-water value on the leader several times, and
//! triggers a snapshot + log purge. After healing the partition the trailing
//! follower's log range no longer exists on the leader, so openraft must
//! stream a full snapshot to catch it up.
//!
//! This is the only place in the suite that exercises the snapshot RPC path:
//! `MemNetworkPeer::full_snapshot` → `RaftHandle::install_full_snapshot` →
//! `HighWaterStateMachine::{begin_receiving_snapshot, get_snapshot_builder,
//! install_snapshot}`.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use openraft::{Config, Raft, SnapshotPolicy};
use openraft_toolkit::test_fakes::MemNetwork;
use openraft_toolkit::{Flat, RocksdbLogStore};
use rocksdb::{ColumnFamilyDescriptor, DB, Options};
use tempfile::TempDir;
use tokio::time::timeout;
use tsoracle_driver_openraft::{
    HighWaterCommand, HighWaterStateMachine, OpenraftPeer, TypeConfig,
};

use common::eventually_eq;

const LOG_CF: &str = "raft_log";
const META_CF: &str = "raft_meta";

/// Heartbeat and election timeouts match the rest of the suite; the snapshot
/// knobs are deliberately tight so a handful of writes triggers a build and
/// the leader's log is purged immediately past the snapshot watermark.
fn snapshot_aggressive_config() -> Arc<Config> {
    Arc::new(
        Config {
            heartbeat_interval: 50,
            election_timeout_min: 150,
            election_timeout_max: 300,
            snapshot_policy: SnapshotPolicy::LogsSinceLast(4),
            max_in_snapshot_log_to_keep: 0,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    )
}

fn open_log_store(dir: &TempDir) -> RocksdbLogStore<TypeConfig, Flat> {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    let cfs = vec![
        ColumnFamilyDescriptor::new(LOG_CF, Options::default()),
        ColumnFamilyDescriptor::new(META_CF, Options::default()),
    ];
    let db = Arc::new(DB::open_cf_descriptors(&opts, dir.path(), cfs).unwrap());
    RocksdbLogStore::open(db, LOG_CF, META_CF, Flat).unwrap()
}

struct SnapshotNode {
    id: u64,
    raft: Raft<TypeConfig, HighWaterStateMachine>,
    sm: HighWaterStateMachine,
    _log_dir: TempDir,
}

async fn find_leader_idx(nodes: &[SnapshotNode]) -> usize {
    timeout(Duration::from_secs(10), async {
        loop {
            for (idx, node) in nodes.iter().enumerate() {
                if let Some(leader) = node.raft.current_leader().await {
                    if leader == node.id {
                        return idx;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("a leader elected within 10s")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn isolated_follower_catches_up_via_snapshot_transfer() {
    let net = MemNetwork::<TypeConfig>::new();
    let cfg = snapshot_aggressive_config();

    let mut nodes: Vec<SnapshotNode> = Vec::new();
    for id in [1u64, 2, 3] {
        let dir = TempDir::new().unwrap();
        let log_store = open_log_store(&dir);
        let sm = HighWaterStateMachine::new();
        let sm_clone = sm.clone();
        let raft = Raft::<TypeConfig, HighWaterStateMachine>::new(
            id,
            cfg.clone(),
            net.factory_for(id),
            log_store,
            sm,
        )
        .await
        .expect("Raft::new");
        net.register(id, raft.clone());
        nodes.push(SnapshotNode {
            id,
            raft,
            sm: sm_clone,
            _log_dir: dir,
        });
    }

    let mut mem = BTreeMap::new();
    for id in [1u64, 2, 3] {
        mem.insert(
            id,
            OpenraftPeer {
                addr: format!("snapshot-node-{id}"),
            },
        );
    }
    nodes[0].raft.initialize(mem).await.expect("initialize");

    let leader_idx = find_leader_idx(&nodes).await;
    let leader_id = nodes[leader_idx].id;
    let follower_idx = (0..3).find(|i| *i != leader_idx).unwrap();
    let follower_id = nodes[follower_idx].id;

    // Baseline: bump once and let everyone — including the to-be-isolated
    // follower — converge so we know the test's starting state.
    nodes[leader_idx]
        .raft
        .client_write(HighWaterCommand::Bump { target: 10 })
        .await
        .expect("baseline bump");
    for node in &nodes {
        let sm = node.sm.clone();
        eventually_eq(10u64, Duration::from_secs(5), move || {
            let sm = sm.clone();
            async move { sm.current_value().await }
        })
        .await;
    }

    // Isolate the follower. After this point the leader can replicate
    // entries only to itself + the other follower, which still forms a
    // quorum so writes succeed.
    net.partitions().isolate(follower_id);

    // Apply enough Bumps to comfortably cross LogsSinceLast(4) and grow the
    // committed-log distance between the leader and the trailing follower.
    let final_target = 80u64;
    for next_target in [20u64, 30, 40, 50, 60, 70, final_target] {
        nodes[leader_idx]
            .raft
            .client_write(HighWaterCommand::Bump { target: next_target })
            .await
            .expect("partition-side bump");
    }

    // Force a snapshot on every reachable node. Triggering on the isolated
    // follower is a no-op for our purposes (it has nothing new to snapshot),
    // but on the leader this kicks off the build that — combined with
    // `max_in_snapshot_log_to_keep = 0` — purges the log past the trailing
    // follower's last replicated index.
    for node in &nodes {
        if node.id == follower_id {
            continue;
        }
        node.raft
            .trigger()
            .snapshot()
            .await
            .expect("trigger snapshot");
    }

    // Wait until the leader's snapshot covers at least the last write we
    // did. Without this, healing too eagerly lets append_entries catch the
    // follower up via the log and we never touch the snapshot path.
    let leader_raft = nodes[leader_idx].raft.clone();
    let leader_snapshot_log_id = timeout(Duration::from_secs(10), async move {
        loop {
            let metrics = leader_raft.metrics().borrow().clone();
            if let Some(snapshot_log_id) = metrics.snapshot.clone() {
                if snapshot_log_id.index >= 5 {
                    return snapshot_log_id;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("leader built a snapshot within 10s");

    // Belt-and-braces: explicitly request the leader purge its log up to the
    // snapshot index. `max_in_snapshot_log_to_keep = 0` already implies this
    // but purge runs on a delay, and we want the trailing follower's next
    // append_entries probe to see a log that no longer covers its range.
    nodes[leader_idx]
        .raft
        .trigger()
        .purge_log(leader_snapshot_log_id.index)
        .await
        .expect("trigger purge_log");

    // Heal — the follower's only path back to consistency is a streamed
    // snapshot.
    net.partitions().heal(follower_id);

    let follower_sm = nodes[follower_idx].sm.clone();
    eventually_eq(final_target, Duration::from_secs(15), move || {
        let sm = follower_sm.clone();
        async move { sm.current_value().await }
    })
    .await;

    // Defense in depth: the trailing follower must show a snapshot in its
    // metrics whose log id matches (or follows) the leader's snapshot. A
    // follower that caught up via log replication alone would have
    // `snapshot == None` here, because it never had enough committed
    // entries pre-isolation to satisfy `LogsSinceLast(4)` on its own.
    let follower_snapshot_log_id = nodes[follower_idx]
        .raft
        .metrics()
        .borrow()
        .snapshot
        .clone()
        .expect("trailing follower must have installed a snapshot");
    assert!(
        follower_snapshot_log_id.index >= leader_snapshot_log_id.index,
        "follower snapshot {:?} must cover the leader's snapshot {:?}",
        follower_snapshot_log_id,
        leader_snapshot_log_id,
    );
}
