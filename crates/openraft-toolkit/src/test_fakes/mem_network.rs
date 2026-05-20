//! In-memory `RaftNetworkFactory` for multi-node test harnesses.
//!
//! Routes append-entries / vote / install-full-snapshot RPCs through direct
//! method calls on the receiver's `Raft<C, SM>` handle, gated by a shared
//! [`PartitionController`]. No sockets, no channels per RPC — just a
//! lock-protected registry of receiver-side dispatch closures.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use openraft::error::{Fatal, NetworkError, RPCError, RaftError, ReplicationClosed, StreamingError};
use openraft::network::{RPCOption, RaftNetworkFactory, RaftNetworkV2};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::storage::RaftStateMachine;
use openraft::type_config::alias::{SnapshotOf, VoteOf};
use openraft::{OptionalSend, Raft, RaftTypeConfig};

use crate::test_fakes::partition::PartitionController;

/// Receiver-side dispatch trait. Wraps a concrete `Raft<C, SM>` so the
/// network registry doesn't need to be parameterized over `SM`.
#[async_trait]
trait RaftAdapter<C: RaftTypeConfig>: Send + Sync + 'static {
    async fn append_entries(
        &self,
        req: AppendEntriesRequest<C>,
    ) -> Result<AppendEntriesResponse<C>, RaftError<C>>;

    async fn vote(&self, req: VoteRequest<C>) -> Result<VoteResponse<C>, RaftError<C>>;

    async fn install_full_snapshot(
        &self,
        vote: VoteOf<C>,
        snapshot: SnapshotOf<C>,
    ) -> Result<SnapshotResponse<C>, Fatal<C>>;
}

struct RaftHandle<C: RaftTypeConfig, SM: RaftStateMachine<C>> {
    raft: Raft<C, SM>,
}

#[async_trait]
impl<C, SM> RaftAdapter<C> for RaftHandle<C, SM>
where
    C: RaftTypeConfig,
    SM: RaftStateMachine<C> + 'static,
{
    async fn append_entries(
        &self,
        req: AppendEntriesRequest<C>,
    ) -> Result<AppendEntriesResponse<C>, RaftError<C>> {
        self.raft.append_entries(req).await
    }

    async fn vote(&self, req: VoteRequest<C>) -> Result<VoteResponse<C>, RaftError<C>> {
        self.raft.vote(req).await
    }

    async fn install_full_snapshot(
        &self,
        vote: VoteOf<C>,
        snapshot: SnapshotOf<C>,
    ) -> Result<SnapshotResponse<C>, Fatal<C>> {
        self.raft.install_full_snapshot(vote, snapshot).await
    }
}

/// In-memory network registry. One per cluster.
pub struct MemNetwork<C: RaftTypeConfig> {
    nodes: RwLock<HashMap<C::NodeId, Arc<dyn RaftAdapter<C>>>>,
    partitions: Arc<PartitionController<C::NodeId>>,
}

impl<C: RaftTypeConfig> MemNetwork<C>
where
    C::NodeId: Copy,
{
    /// Build a fresh, empty in-memory network with no peers registered and no
    /// partitions installed.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            nodes: RwLock::new(HashMap::new()),
            partitions: Arc::new(PartitionController::new()),
        })
    }

    /// Mint a `RaftNetworkFactory` whose `new_client` calls will route to peers
    /// registered on this network, tagging outgoing RPCs as originating from
    /// `self_id`.
    pub fn factory_for(self: &Arc<Self>, self_id: C::NodeId) -> MemNetworkFactory<C> {
        MemNetworkFactory {
            net: Arc::clone(self),
            self_id,
        }
    }

    /// Register a node's `Raft` handle under `id`. Subsequent RPCs from any
    /// factory to `id` dispatch into this handle.
    pub fn register<SM>(&self, id: C::NodeId, raft: Raft<C, SM>)
    where
        SM: RaftStateMachine<C> + 'static,
    {
        let handle: Arc<dyn RaftAdapter<C>> = Arc::new(RaftHandle { raft });
        self.nodes.write().unwrap().insert(id, handle);
    }

    /// Borrow the partition controller. Cloning the `Arc` is the intended way
    /// for tests to drive partition state during a run.
    pub fn partitions(&self) -> Arc<PartitionController<C::NodeId>> {
        Arc::clone(&self.partitions)
    }

    fn dispatch(&self, target: &C::NodeId) -> Option<Arc<dyn RaftAdapter<C>>> {
        self.nodes.read().unwrap().get(target).cloned()
    }
}

/// Factory handed to `Raft::new`. One per node; carries the node's own id so
/// partition checks know which side of the wire the RPC is leaving from.
pub struct MemNetworkFactory<C: RaftTypeConfig> {
    net: Arc<MemNetwork<C>>,
    self_id: C::NodeId,
}

impl<C: RaftTypeConfig> RaftNetworkFactory<C> for MemNetworkFactory<C>
where
    C::NodeId: Copy,
{
    type Network = MemNetworkPeer<C>;

    async fn new_client(&mut self, target: C::NodeId, _node: &C::Node) -> Self::Network {
        MemNetworkPeer {
            net: Arc::clone(&self.net),
            from: self.self_id,
            to: target,
        }
    }
}

/// Per-target client. Looks the target up in the shared registry on every RPC
/// so a node can be reopened mid-test and have its replacement picked up.
pub struct MemNetworkPeer<C: RaftTypeConfig> {
    net: Arc<MemNetwork<C>>,
    from: C::NodeId,
    to: C::NodeId,
}

impl<C: RaftTypeConfig> RaftNetworkV2<C> for MemNetworkPeer<C>
where
    C::NodeId: Copy,
{
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<C>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<C>, RPCError<C>> {
        if !self.net.partitions.is_reachable(self.from, self.to) {
            return Err(RPCError::Network(NetworkError::from_string(format!(
                "mem-network: partitioned {:?} -> {:?}",
                self.from, self.to
            ))));
        }
        let target = self.net.dispatch(&self.to).ok_or_else(|| {
            RPCError::Network(NetworkError::from_string(format!(
                "mem-network: unknown peer {:?}",
                self.to
            )))
        })?;
        target.append_entries(rpc).await.map_err(|e| {
            RPCError::Network(NetworkError::from_string(format!("mem-network remote: {e}")))
        })
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<C>,
        _option: RPCOption,
    ) -> Result<VoteResponse<C>, RPCError<C>> {
        if !self.net.partitions.is_reachable(self.from, self.to) {
            return Err(RPCError::Network(NetworkError::from_string(format!(
                "mem-network: partitioned {:?} -> {:?}",
                self.from, self.to
            ))));
        }
        let target = self.net.dispatch(&self.to).ok_or_else(|| {
            RPCError::Network(NetworkError::from_string(format!(
                "mem-network: unknown peer {:?}",
                self.to
            )))
        })?;
        target.vote(rpc).await.map_err(|e| {
            RPCError::Network(NetworkError::from_string(format!("mem-network remote: {e}")))
        })
    }

    async fn full_snapshot(
        &mut self,
        vote: VoteOf<C>,
        snapshot: SnapshotOf<C>,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<C>, StreamingError<C>> {
        if !self.net.partitions.is_reachable(self.from, self.to) {
            return Err(StreamingError::Network(NetworkError::from_string(format!(
                "mem-network: partitioned {:?} -> {:?}",
                self.from, self.to
            ))));
        }
        let target = self.net.dispatch(&self.to).ok_or_else(|| {
            StreamingError::Network(NetworkError::from_string(format!(
                "mem-network: unknown peer {:?}",
                self.to
            )))
        })?;
        target
            .install_full_snapshot(vote, snapshot)
            .await
            .map_err(|e| {
                StreamingError::Network(NetworkError::from_string(format!(
                    "mem-network remote: {e}"
                )))
            })
    }
}
