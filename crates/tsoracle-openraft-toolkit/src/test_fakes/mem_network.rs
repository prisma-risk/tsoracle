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
use openraft::error::{
    Fatal, NetworkError, RPCError, RaftError, ReplicationClosed, StreamingError,
};
use openraft::network::{RPCOption, RaftNetworkFactory, RaftNetworkV2};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, TransferLeaderRequest,
    TransferLeaderResponse, VoteRequest, VoteResponse,
};
use openraft::storage::RaftStateMachine;
use openraft::type_config::alias::{SnapshotOf, VoteOf};
use openraft::{OptionalSend, Raft, RaftTypeConfig};

use crate::test_fakes::partition::PartitionController;

/// Receiver-side dispatch trait. Wraps a concrete `Raft<C, SM>` so the
/// network registry doesn't need to be parameterized over `SM`.
#[async_trait]
trait RaftAdapter<C: RaftTypeConfig, SD: OptionalSend + 'static>: Send + Sync + 'static {
    async fn append_entries(
        &self,
        req: AppendEntriesRequest<C>,
    ) -> Result<AppendEntriesResponse<C>, RaftError<C>>;

    async fn vote(&self, req: VoteRequest<C>) -> Result<VoteResponse<C>, RaftError<C>>;

    async fn install_full_snapshot(
        &self,
        vote: VoteOf<C>,
        snapshot: SnapshotOf<C, SD>,
    ) -> Result<SnapshotResponse<C>, Fatal<C>>;

    async fn transfer_leader(
        &self,
        req: TransferLeaderRequest<C>,
    ) -> Result<TransferLeaderResponse<C>, Fatal<C>>;
}

type RaftAdapterHandle<C, SD> = Arc<dyn RaftAdapter<C, SD>>;

struct RaftHandle<C: RaftTypeConfig, SM: RaftStateMachine<C>> {
    raft: Raft<C, SM>,
}

#[async_trait]
impl<C, SM> RaftAdapter<C, SM::SnapshotData> for RaftHandle<C, SM>
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
        snapshot: SnapshotOf<C, SM::SnapshotData>,
    ) -> Result<SnapshotResponse<C>, Fatal<C>> {
        self.raft.install_full_snapshot(vote, snapshot).await
    }

    async fn transfer_leader(
        &self,
        req: TransferLeaderRequest<C>,
    ) -> Result<TransferLeaderResponse<C>, Fatal<C>> {
        self.raft.handle_transfer_leader(req).await
    }
}

/// In-memory network registry. One per cluster.
pub struct MemNetwork<C: RaftTypeConfig, SD: OptionalSend + 'static = std::io::Cursor<Vec<u8>>> {
    nodes: RwLock<HashMap<C::NodeId, RaftAdapterHandle<C, SD>>>,
    partitions: Arc<PartitionController<C::NodeId>>,
}

impl<C, SD> MemNetwork<C, SD>
where
    C: RaftTypeConfig,
    C::NodeId: Copy,
    SD: OptionalSend + 'static,
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
    pub fn factory_for(self: &Arc<Self>, self_id: C::NodeId) -> MemNetworkFactory<C, SD> {
        MemNetworkFactory {
            net: Arc::clone(self),
            self_id,
        }
    }

    /// Register a node's `Raft` handle under `id`. Subsequent RPCs from any
    /// factory to `id` dispatch into this handle.
    pub fn register<SM>(&self, id: C::NodeId, raft: Raft<C, SM>)
    where
        SM: RaftStateMachine<C, SnapshotData = SD> + 'static,
    {
        let handle: RaftAdapterHandle<C, SD> = Arc::new(RaftHandle { raft });
        self.nodes.write().unwrap().insert(id, handle);
    }

    /// Borrow the partition controller. Cloning the `Arc` is the intended way
    /// for tests to drive partition state during a run.
    pub fn partitions(&self) -> Arc<PartitionController<C::NodeId>> {
        Arc::clone(&self.partitions)
    }

    fn dispatch(&self, target: &C::NodeId) -> Option<RaftAdapterHandle<C, SD>> {
        self.nodes.read().unwrap().get(target).cloned()
    }
}

/// Factory handed to `Raft::new`. One per node; carries the node's own id so
/// partition checks know which side of the wire the RPC is leaving from.
pub struct MemNetworkFactory<
    C: RaftTypeConfig,
    SD: OptionalSend + 'static = std::io::Cursor<Vec<u8>>,
> {
    net: Arc<MemNetwork<C, SD>>,
    self_id: C::NodeId,
}

impl<C, SD> RaftNetworkFactory<C> for MemNetworkFactory<C, SD>
where
    C: RaftTypeConfig,
    C::NodeId: Copy,
    SD: OptionalSend + 'static,
{
    type Network = MemNetworkPeer<C, SD>;

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
pub struct MemNetworkPeer<C: RaftTypeConfig, SD: OptionalSend + 'static = std::io::Cursor<Vec<u8>>>
{
    net: Arc<MemNetwork<C, SD>>,
    from: C::NodeId,
    to: C::NodeId,
}

impl<C, SD> RaftNetworkV2<C> for MemNetworkPeer<C, SD>
where
    C: RaftTypeConfig,
    C::NodeId: Copy,
    SD: OptionalSend + 'static,
{
    type SnapshotData = SD;

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
            RPCError::Network(NetworkError::from_string(format!(
                "mem-network remote: {e}"
            )))
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
            RPCError::Network(NetworkError::from_string(format!(
                "mem-network remote: {e}"
            )))
        })
    }

    async fn full_snapshot(
        &mut self,
        vote: VoteOf<C>,
        snapshot: SnapshotOf<C, Self::SnapshotData>,
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

    async fn transfer_leader(
        &mut self,
        req: TransferLeaderRequest<C>,
        _option: RPCOption,
    ) -> Result<TransferLeaderResponse<C>, RPCError<C>> {
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
        target.transfer_leader(req).await.map_err(|e| {
            RPCError::Network(NetworkError::from_string(format!(
                "mem-network remote: {e}"
            )))
        })
    }
}
