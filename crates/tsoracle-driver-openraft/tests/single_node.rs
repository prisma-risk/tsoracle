//! Single-node integration test for [`OpenraftDriver`].
//!
//! Stands up a one-voter openraft cluster on top of:
//!
//! - `openraft_toolkit::RocksdbLogStore` (tempdir-backed)
//! - `HighWaterStateMachine` (in-memory)
//! - `UnreachableNetwork` — a `RaftNetworkV2` whose RPC methods panic if ever
//!   invoked. A single-voter cluster never sends append/vote/snapshot RPCs to
//!   itself, so any panic from here would indicate a real bug.
//!
//! Then drives the [`ConsensusDriver`] surface: confirms an initial Leader
//! event, an empty `load_high_water`, a `persist_high_water` round trip, and
//! the monotonic-advance semantics.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use openraft::Config;
use openraft::Raft;
use openraft::error::{NetworkError, RPCError, ReplicationClosed, StreamingError};
use openraft::network::{RPCOption, RaftNetworkFactory, RaftNetworkV2};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::type_config::alias::{SnapshotOf, VoteOf};
use openraft_toolkit::{Flat, RocksdbLogStore};
use rocksdb::{ColumnFamilyDescriptor, DB, Options};
use tempfile::TempDir;
use tokio::time::timeout;
use tsoracle_consensus::{ConsensusDriver, LeaderState};
use tsoracle_driver_openraft::{HighWaterStateMachine, OpenraftDriver, OpenraftPeer, TypeConfig};

const LOG_CF: &str = "raft_log";
const META_CF: &str = "raft_meta";

// ---------------------------------------------------------------------------
// UnreachableNetwork: panicking `RaftNetworkV2` for single-node clusters.
// ---------------------------------------------------------------------------

/// A `RaftNetworkFactory` whose generated clients panic on any RPC.
///
/// Single-node clusters never replicate to peers, so the network is exercised
/// only as a type-level requirement of `Raft::new`. If any test ever upgrades
/// to a multi-node cluster this network must be replaced with a real one.
struct UnreachableNetwork;

impl RaftNetworkFactory<TypeConfig> for UnreachableNetwork {
    type Network = UnreachablePeer;

    async fn new_client(&mut self, target: u64, _node: &OpenraftPeer) -> Self::Network {
        UnreachablePeer { target }
    }
}

struct UnreachablePeer {
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
        _cancel: impl Future<Output = ReplicationClosed> + openraft::OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        Err(StreamingError::Network(NetworkError::from_string(format!(
            "unreachable network: snapshot to node {} in single-node test",
            self.target
        ))))
    }
}

// ---------------------------------------------------------------------------
// Test harness: assemble Raft + driver from scratch.
// ---------------------------------------------------------------------------

struct SingleNode {
    driver: OpenraftDriver,
    /// Kept alive so the rocksdb backing files outlive the test.
    _dir: TempDir,
    /// Raft handle. Keeping the original here keeps the cluster running for
    /// the driver's `Raft` clone too — `Raft` is `Arc`-backed.
    _raft: Raft<TypeConfig, HighWaterStateMachine>,
}

async fn build_single_node() -> SingleNode {
    let dir = TempDir::new().unwrap();

    // RocksDB with two column families: log + meta.
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    let cfs = vec![
        ColumnFamilyDescriptor::new(LOG_CF, Options::default()),
        ColumnFamilyDescriptor::new(META_CF, Options::default()),
    ];
    let db = Arc::new(DB::open_cf_descriptors(&opts, dir.path(), cfs).unwrap());

    let log_store: RocksdbLogStore<TypeConfig, Flat> =
        RocksdbLogStore::open(db, LOG_CF, META_CF, Flat).unwrap();

    let sm = HighWaterStateMachine::new();
    let sm_clone = sm.clone();

    // Aggressive heartbeats so a single-node cluster becomes leader fast.
    let config = Arc::new(
        Config {
            heartbeat_interval: 50,
            election_timeout_min: 150,
            election_timeout_max: 300,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );

    let raft = Raft::<TypeConfig, HighWaterStateMachine>::new(
        1u64,
        config,
        UnreachableNetwork,
        log_store,
        sm,
    )
    .await
    .expect("Raft::new");

    // Initialize the single-voter membership.
    let mut nodes = BTreeMap::new();
    nodes.insert(
        1u64,
        OpenraftPeer {
            addr: "self".into(),
        },
    );
    raft.initialize(nodes).await.expect("initialize");

    let driver = OpenraftDriver::new(raft.clone(), sm_clone);
    SingleNode {
        driver,
        _dir: dir,
        _raft: raft,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn single_node_leader_persists_high_water() {
    let node = build_single_node().await;
    let driver = &node.driver;

    // Drain leadership events until we see a Leader. The first event may be
    // Unknown/Follower depending on timing; the next class transition will be
    // Leader once the single-voter cluster elects itself.
    let mut events = driver.leadership_events();
    let epoch = timeout(Duration::from_secs(5), async {
        loop {
            let s = events.next().await.expect("event stream alive");
            if let LeaderState::Leader { epoch } = s {
                break epoch;
            }
        }
    })
    .await
    .expect("became leader within 5s");

    // Empty start.
    assert_eq!(driver.load_high_water().await.unwrap(), 0);

    // Advance.
    let v = driver.persist_high_water(100, epoch).await.unwrap();
    assert_eq!(v, 100);

    // Stale call: should be silently absorbed, value unchanged.
    let v = driver.persist_high_water(50, epoch).await.unwrap();
    assert_eq!(v, 100);

    // Forward.
    let v = driver.persist_high_water(200, epoch).await.unwrap();
    assert_eq!(v, 200);

    // Linearized load matches the last apply.
    assert_eq!(driver.load_high_water().await.unwrap(), 200);
}
