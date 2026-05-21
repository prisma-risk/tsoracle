//! Shared test scaffolding for `tsoracle-driver-openraft` integration tests.
//!
//! Each `tests/*.rs` declares `mod common;` and imports via `use common::*;`.
//! Rust compiles this module per integration-test binary (a known minor
//! duplication; negligible at our scale).

#![allow(dead_code)] // each test binary uses a subset; allow the rest

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use openraft::Config;
use openraft::OptionalSend;
use openraft::Raft;
use openraft::error::{NetworkError, RPCError, ReplicationClosed, StreamingError};
use openraft::network::{RPCOption, RaftNetworkFactory, RaftNetworkV2};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::type_config::alias::{SnapshotOf, VoteOf};
use rocksdb::{ColumnFamilyDescriptor, DB, Options};
use tempfile::TempDir;
use tokio::time::Instant;
use tsoracle_driver_openraft::{
    HighWaterStateMachine, OpenraftDriver, OpenraftPeer, StandaloneHost, TypeConfig,
};
use tsoracle_openraft_toolkit::test_fakes::{MemNetwork, PartitionController};
use tsoracle_openraft_toolkit::{Flat, RocksdbLogStore};

/// One node in a test cluster. Holds the raft handle, a clone of the state
/// machine for direct reads, the rocksdb `Arc<DB>` (so subsequent reopens can
/// reuse the same db without fighting tokio drop ordering — see
/// [`reopen_node_with_config`]), the tempdir (so files outlive the test),
/// and the node id.
pub struct TestNode {
    pub id: u64,
    pub raft: Raft<TypeConfig, HighWaterStateMachine>,
    pub sm: HighWaterStateMachine,
    pub db: Arc<DB>,
    pub log_dir: TempDir,
}

/// A built test cluster. `network` and `partitions` are `None` for
/// single-node clusters (those use a panicking network). `drivers[i]`
/// corresponds to `nodes[i]`.
pub struct TestCluster {
    pub nodes: Vec<TestNode>,
    pub network: Option<Arc<MemNetwork<TypeConfig>>>,
    pub partitions: Option<Arc<PartitionController<u64>>>,
    pub drivers: Vec<Arc<OpenraftDriver<StandaloneHost>>>,
}

/// Poll `f` on a 50ms cadence until it yields `expected` or `timeout`
/// elapses. Panics with a descriptive message on timeout.
pub async fn eventually_eq<T, F, Fut>(expected: T, timeout: Duration, mut f: F)
where
    T: PartialEq + Debug,
    F: FnMut() -> Fut,
    Fut: Future<Output = T>,
{
    let deadline = Instant::now() + timeout;
    let mut last = f().await;
    while last != expected && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
        last = f().await;
    }
    assert_eq!(
        last, expected,
        "eventually_eq timed out after {timeout:?}: last={last:?} expected={expected:?}",
    );
}

// ---------------------------------------------------------------------------
// UnreachableNetwork: panicking `RaftNetworkV2` for single-node clusters.
// ---------------------------------------------------------------------------

/// A `RaftNetworkFactory` whose generated clients panic on any RPC.
/// Suitable only for single-voter clusters that never replicate.
pub struct UnreachableNetwork;

impl RaftNetworkFactory<TypeConfig> for UnreachableNetwork {
    type Network = UnreachablePeer;

    async fn new_client(&mut self, target: u64, _node: &OpenraftPeer) -> Self::Network {
        UnreachablePeer { target }
    }
}

pub struct UnreachablePeer {
    target: u64,
}

impl RaftNetworkV2<TypeConfig> for UnreachablePeer {
    async fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>> {
        Err(RPCError::Network(NetworkError::from_string(format!(
            "unreachable network: append_entries to node {} in single-node test",
            self.target
        ))))
    }

    async fn vote(
        &mut self,
        _rpc: VoteRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        Err(RPCError::Network(NetworkError::from_string(format!(
            "unreachable network: vote to node {} in single-node test",
            self.target
        ))))
    }

    async fn full_snapshot(
        &mut self,
        _vote: VoteOf<TypeConfig>,
        _snapshot: SnapshotOf<TypeConfig>,
        _cancel: impl std::future::Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        Err(StreamingError::Network(NetworkError::from_string(format!(
            "unreachable network: snapshot to node {} in single-node test",
            self.target
        ))))
    }
}

const LOG_CF: &str = "raft_log";
const META_CF: &str = "raft_meta";
const SNAP_CF: &str = "raft_snapshot";

fn test_raft_config() -> Arc<openraft::Config> {
    Arc::new(
        Config {
            heartbeat_interval: 50,
            election_timeout_min: 150,
            election_timeout_max: 300,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    )
}

/// Open a fresh rocksdb at `dir` with the three column families this crate's
/// tests use:
///
/// - `raft_log` / `raft_meta` — backing `tsoracle_openraft_toolkit::RocksdbLogStore`.
/// - `raft_snapshot` — backing `tsoracle_driver_openraft::RocksdbSnapshotStore`.
fn open_rocksdb(dir: &TempDir) -> Arc<DB> {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    let cfs = vec![
        ColumnFamilyDescriptor::new(LOG_CF, Options::default()),
        ColumnFamilyDescriptor::new(META_CF, Options::default()),
        ColumnFamilyDescriptor::new(SNAP_CF, Options::default()),
    ];
    Arc::new(DB::open_cf_descriptors(&opts, dir.path(), cfs).unwrap())
}

/// Open log + snapshot stores against an already-opened `Arc<DB>`. Returning
/// stores that share the same `Arc<DB>` matches the wiring real deployments
/// use: one rocksdb instance covers both halves of openraft's durable state,
/// so a single `set_sync(true)` write crosses the same fsync boundary for
/// log and snapshot.
fn open_log_and_snapshot_stores(
    db: Arc<DB>,
) -> (
    RocksdbLogStore<TypeConfig, Flat>,
    Arc<dyn tsoracle_driver_openraft::SnapshotStore>,
) {
    let log_store = RocksdbLogStore::open(db.clone(), LOG_CF, META_CF, Flat).unwrap();
    let snapshot_store: Arc<dyn tsoracle_driver_openraft::SnapshotStore> =
        Arc::new(tsoracle_driver_openraft::RocksdbSnapshotStore::open(db, SNAP_CF).unwrap());
    (log_store, snapshot_store)
}

// Builders below are stubs filled in by subsequent tasks:
// - build_three_node: cluster constructor
// - reopen_node: restart-replay primitive

pub async fn build_single_node() -> TestCluster {
    build_single_node_with_config(test_raft_config()).await
}

/// Like [`build_single_node`] but lets the caller override the raft `Config`.
///
/// Used by the snapshot-restart test to force aggressive snapshot + purge
/// behavior. Default callers should use [`build_single_node`].
pub async fn build_single_node_with_config(config: Arc<Config>) -> TestCluster {
    let dir = TempDir::new().unwrap();
    let db = open_rocksdb(&dir);
    let (log_store, snapshot_store) = open_log_and_snapshot_stores(db.clone());
    let sm = HighWaterStateMachine::with_store(snapshot_store).expect("hydrate SM");
    let sm_clone = sm.clone();

    let raft = Raft::<TypeConfig, HighWaterStateMachine>::new(
        1u64,
        config,
        UnreachableNetwork,
        log_store,
        sm,
    )
    .await
    .expect("Raft::new");

    let mut mem = BTreeMap::new();
    mem.insert(
        1u64,
        OpenraftPeer {
            addr: "self".into(),
        },
    );
    raft.initialize(mem).await.expect("initialize");

    let host = StandaloneHost::new(raft.clone(), sm_clone.clone());
    let driver = OpenraftDriver::new(host);

    TestCluster {
        nodes: vec![TestNode {
            id: 1,
            raft,
            sm: sm_clone,
            db,
            log_dir: dir,
        }],
        network: None,
        partitions: None,
        drivers: vec![driver],
    }
}

pub async fn build_three_node() -> TestCluster {
    let net = MemNetwork::<TypeConfig>::new();
    let cfg = test_raft_config();

    let mut nodes: Vec<TestNode> = Vec::new();
    let mut drivers: Vec<Arc<OpenraftDriver<StandaloneHost>>> = Vec::new();

    for id in [1u64, 2, 3] {
        let dir = TempDir::new().unwrap();
        let db = open_rocksdb(&dir);
        let (log_store, snapshot_store) = open_log_and_snapshot_stores(db.clone());
        let sm = HighWaterStateMachine::with_store(snapshot_store).expect("hydrate SM");
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

        let host = StandaloneHost::new(raft.clone(), sm_clone.clone());
        drivers.push(OpenraftDriver::new(host));
        nodes.push(TestNode {
            id,
            raft,
            sm: sm_clone,
            db,
            log_dir: dir,
        });
    }

    // Initialize membership on node 1.
    let mut mem = BTreeMap::new();
    for id in [1u64, 2, 3] {
        mem.insert(
            id,
            OpenraftPeer {
                addr: format!("mem-node-{id}"),
            },
        );
    }
    nodes[0].raft.initialize(mem).await.expect("initialize");

    let partitions = net.partitions();
    TestCluster {
        nodes,
        network: Some(net),
        partitions: Some(partitions),
        drivers,
    }
}

pub async fn reopen_node(prior: TestNode) -> TestNode {
    reopen_node_with_config(prior, test_raft_config()).await
}

/// Like [`reopen_node`] but lets the caller override the raft `Config` on
/// reopen.
///
/// Shares the prior node's `Arc<DB>` rather than closing and reopening the
/// rocksdb at the same path. Closing the DB would require zero live
/// `Arc<DB>` handles, but openraft 0.10's snapshot-build task is detached
/// (`let _handle = C::spawn(...)` in `src/core/sm/worker.rs`), so an
/// `Arc<dyn SnapshotStore>` clone may survive `Raft::shutdown` for an
/// indeterminate time on the runtime's worker thread. Reusing the prior
/// `Arc<DB>` sidesteps the LOCK race without weakening what we're
/// validating: snapshot durability is established by rocksdb's
/// `set_sync(true)` semantics (the WAL+memtable is on disk once `save`
/// returns), independent of whether the open file handle is recycled.
pub async fn reopen_node_with_config(prior: TestNode, config: Arc<Config>) -> TestNode {
    let TestNode {
        id,
        raft,
        sm: _,
        db,
        log_dir,
    } = prior;

    // Shut down the prior Raft cleanly. Its log_store and SM clones drop on
    // shutdown; the snapshot subtask's clone drops whenever the runtime gets
    // around to it. `db` is retained, so the next `open_log_and_snapshot_stores`
    // call attaches to the same rocksdb without needing the lock to release.
    raft.shutdown().await.expect("prior raft shutdown");
    drop(raft);

    let (log_store, snapshot_store) = open_log_and_snapshot_stores(db.clone());
    let sm = HighWaterStateMachine::with_store(snapshot_store).expect("rehydrate SM on reopen");
    let sm_clone = sm.clone();
    let raft = Raft::<TypeConfig, HighWaterStateMachine>::new(
        id,
        config,
        UnreachableNetwork,
        log_store,
        sm,
    )
    .await
    .expect("Raft::new on reopen");

    TestNode {
        id,
        raft,
        sm: sm_clone,
        db,
        log_dir,
    }
}
