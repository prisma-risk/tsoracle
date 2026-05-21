//! Negative-path coverage for `MemNetwork` peers: partitioned reachability and
//! unknown-target dispatch, for each of the three `RaftNetworkV2` methods.
//!
//! The happy path is covered by `mem_network_smoke.rs`; this file exists to
//! exercise the partition / unknown-peer error formatting on every method so a
//! future refactor that drops one branch is caught by coverage rather than by
//! a future surprise during a real partition test.

use std::sync::Arc;
use std::time::Duration;

use openraft::Vote;
use openraft::error::{RPCError, StreamingError};
use openraft::network::{RPCOption, RaftNetworkFactory, RaftNetworkV2};
use openraft::raft::{AppendEntriesRequest, VoteRequest};
use openraft::storage::{Snapshot, SnapshotMeta};
use openraft::type_config::alias::{SnapshotOf, VoteOf};
use tsoracle_openraft_toolkit::test_fakes::MemNetwork;

mod common;

use common::{TestPeer, TestTypeConfig};

fn rpc_opt() -> RPCOption {
    RPCOption::new(Duration::from_secs(1))
}

fn new_peer_targeting(
    net: &Arc<MemNetwork<TestTypeConfig>>,
    from: u64,
    to: u64,
) -> impl RaftNetworkV2<TestTypeConfig> {
    let mut factory = net.factory_for(from);
    futures::executor::block_on(factory.new_client(
        to,
        &TestPeer {
            addr: format!("peer-{to}"),
        },
    ))
}

fn sample_vote() -> VoteOf<TestTypeConfig> {
    Vote::new(1, 1)
}

fn sample_snapshot() -> SnapshotOf<TestTypeConfig> {
    Snapshot {
        meta: SnapshotMeta::default(),
        snapshot: std::io::Cursor::new(Vec::new()),
    }
}

#[tokio::test]
async fn vote_to_unknown_peer_returns_network_error() {
    let net = MemNetwork::<TestTypeConfig>::new();
    let mut peer = new_peer_targeting(&net, 1, 99);
    let req = VoteRequest::<TestTypeConfig>::new(sample_vote(), None);
    let err = peer
        .vote(req, rpc_opt())
        .await
        .expect_err("unknown peer must surface as a Network error");
    let RPCError::Network(net_err) = err else {
        panic!("expected RPCError::Network, got {err:?}");
    };
    assert!(net_err.to_string().contains("unknown peer"));
}

#[tokio::test]
async fn append_entries_to_unknown_peer_returns_network_error() {
    let net = MemNetwork::<TestTypeConfig>::new();
    let mut peer = new_peer_targeting(&net, 1, 99);
    let req = AppendEntriesRequest::<TestTypeConfig> {
        vote: sample_vote(),
        prev_log_id: None,
        entries: Vec::new(),
        leader_commit: None,
    };
    let err = peer
        .append_entries(req, rpc_opt())
        .await
        .expect_err("unknown peer must surface as a Network error");
    let RPCError::Network(net_err) = err else {
        panic!("expected RPCError::Network, got {err:?}");
    };
    assert!(net_err.to_string().contains("unknown peer"));
}

#[tokio::test]
async fn full_snapshot_to_unknown_peer_returns_streaming_network_error() {
    let net = MemNetwork::<TestTypeConfig>::new();
    let mut peer = new_peer_targeting(&net, 1, 99);
    let cancel = futures::future::pending::<openraft::error::ReplicationClosed>();
    let err = peer
        .full_snapshot(sample_vote(), sample_snapshot(), cancel, rpc_opt())
        .await
        .expect_err("unknown peer must surface as a Network error");
    let StreamingError::Network(net_err) = err else {
        panic!("expected StreamingError::Network, got {err:?}");
    };
    assert!(net_err.to_string().contains("unknown peer"));
}

#[tokio::test]
async fn vote_blocked_by_partition_returns_network_error() {
    let net = MemNetwork::<TestTypeConfig>::new();
    net.partitions().isolate(1);
    let mut peer = new_peer_targeting(&net, 1, 2);
    let req = VoteRequest::<TestTypeConfig>::new(sample_vote(), None);
    let err = peer
        .vote(req, rpc_opt())
        .await
        .expect_err("isolated source must surface as a Network error");
    let RPCError::Network(net_err) = err else {
        panic!("expected RPCError::Network, got {err:?}");
    };
    assert!(net_err.to_string().contains("partitioned"));
}

#[tokio::test]
async fn append_entries_blocked_by_partition_returns_network_error() {
    let net = MemNetwork::<TestTypeConfig>::new();
    net.partitions().cut_edge(1, 2);
    let mut peer = new_peer_targeting(&net, 1, 2);
    let req = AppendEntriesRequest::<TestTypeConfig> {
        vote: sample_vote(),
        prev_log_id: None,
        entries: Vec::new(),
        leader_commit: None,
    };
    let err = peer
        .append_entries(req, rpc_opt())
        .await
        .expect_err("cut edge must surface as a Network error");
    let RPCError::Network(net_err) = err else {
        panic!("expected RPCError::Network, got {err:?}");
    };
    assert!(net_err.to_string().contains("partitioned"));
}

#[tokio::test]
async fn full_snapshot_blocked_by_partition_returns_streaming_network_error() {
    let net = MemNetwork::<TestTypeConfig>::new();
    net.partitions().isolate(2);
    let mut peer = new_peer_targeting(&net, 1, 2);
    let cancel = futures::future::pending::<openraft::error::ReplicationClosed>();
    let err = peer
        .full_snapshot(sample_vote(), sample_snapshot(), cancel, rpc_opt())
        .await
        .expect_err("isolated target must surface as a Network error");
    let StreamingError::Network(net_err) = err else {
        panic!("expected StreamingError::Network, got {err:?}");
    };
    assert!(net_err.to_string().contains("partitioned"));
}
