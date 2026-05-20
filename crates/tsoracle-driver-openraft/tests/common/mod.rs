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
use openraft_toolkit::test_fakes::{MemNetwork, PartitionController};
use openraft_toolkit::{Flat, RocksdbLogStore};
use rocksdb::{ColumnFamilyDescriptor, DB, Options};
use tempfile::TempDir;
use tokio::time::Instant;
use tsoracle_driver_openraft::{
    HighWaterStateMachine, OpenraftDriver, OpenraftPeer, StandaloneHost, TypeConfig,
};

/// One node in a test cluster. Holds the raft handle, a clone of the state
/// machine for direct reads, the rocksdb tempdir (so files outlive the test),
/// and the node id.
pub struct TestNode {
    pub id: u64,
    pub raft: Raft<TypeConfig, HighWaterStateMachine>,
    pub sm: HighWaterStateMachine,
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
        "eventually_eq timed out after {:?}: last={:?} expected={:?}",
        timeout, last, expected
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

fn open_rocksdb_log_store(dir: &TempDir) -> RocksdbLogStore<TypeConfig, Flat> {
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

// Builders below are stubs filled in by subsequent tasks:
// - build_three_node: cluster constructor
// - reopen_node: restart-replay primitive

pub async fn build_single_node() -> TestCluster {
    let dir = TempDir::new().unwrap();
    let log_store = open_rocksdb_log_store(&dir);
    let sm = HighWaterStateMachine::new();
    let sm_clone = sm.clone();

    let raft = Raft::<TypeConfig, HighWaterStateMachine>::new(
        1u64,
        test_raft_config(),
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
        let log_store = open_rocksdb_log_store(&dir);
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

        let host = StandaloneHost::new(raft.clone(), sm_clone.clone());
        drivers.push(OpenraftDriver::new(host));
        nodes.push(TestNode {
            id,
            raft,
            sm: sm_clone,
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
    let TestNode {
        id,
        raft,
        sm: _,
        log_dir,
    } = prior;

    // Shut down the prior Raft cleanly so RocksDB files are released.
    raft.shutdown().await.expect("prior raft shutdown");
    drop(raft);

    // Reopen RocksDB at the same path with a fresh state machine. openraft
    // will re-apply committed log entries during Raft::new.
    let log_store = open_rocksdb_log_store(&log_dir);
    let sm = HighWaterStateMachine::new();
    let sm_clone = sm.clone();
    let raft = Raft::<TypeConfig, HighWaterStateMachine>::new(
        id,
        test_raft_config(),
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
        log_dir,
    }
}
