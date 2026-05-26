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

//! tonic-based peer transport for openraft 0.10.
//!
//! Uses `RaftNetworkV2` (the modern API in 0.10). `RaftNetwork` (V1) was
//! removed in 0.10.0-alpha.20 and redirects to `openraft-legacy`.
//!
//! Wire format:
//!   - `AppendEntries` / `Vote`: a `RaftMessage { bytes payload }` where
//!     `payload` is a postcard-encoded openraft request or response.
//!   - `Snapshot`: a *client-streaming* RPC of `SnapshotChunk` messages.
//!     One `header` chunk (postcard-encoded vote + meta), then `SNAPSHOT_CHUNK_SIZE`
//!     byte `data` chunks until end-of-stream. The receiver reassembles the
//!     data buffer and calls `Raft::install_full_snapshot`.
//!
//! The chunked snapshot path is what makes this transport safe to use with a
//! state machine that grows past the default gRPC unary frame limit (4 MiB).
//! See `proto/raft_peer.proto` for the framing rules.

use std::collections::HashMap;
use std::future::Future;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use openraft::error::{NetworkError, RPCError, StreamingError, Unreachable};
use openraft::errors::ReplicationClosed;
use openraft::network::{RPCOption, RaftNetworkFactory, RaftNetworkV2};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, TransferLeaderRequest,
    VoteRequest, VoteResponse,
};
use openraft::type_config::alias::{SnapshotOf, VoteOf};
use tokio::sync::Mutex;
use tonic::transport::{Channel, ClientTlsConfig};

use tsoracle_driver_openraft::{NodeCapabilities, OpenraftPeer as Node, TypeConfig};
use tsoracle_openraft_toolkit::{
    BASELINE_WRITE_VERSION, MAX_READABLE_VERSION, MIN_READABLE_VERSION,
};
type NodeId = u64;

pub mod proto {
    tonic::include_proto!("tsoracle.raft.peer.v1");
}

use proto::RaftMessage;
use proto::SnapshotChunk;
use proto::SnapshotHeader;
use proto::raft_peer_service_client::RaftPeerServiceClient;
use proto::raft_peer_service_server::{RaftPeerService, RaftPeerServiceServer};
use proto::snapshot_chunk::Kind as ChunkKind;

/// Wire-envelope versioning. The `format_version` protobuf field carries the
/// format version of the postcard body alongside it. proto3's default `0`
/// (a pre-feature sender that has no such field) is interpreted as
/// [`BASELINE_WRITE_VERSION`]; any present value must fall inside the readable
/// range `[MIN_READABLE_VERSION, MAX_READABLE_VERSION]` or the body is refused
/// before it is parsed. Senders stamp their node's active write version.
mod wire {
    use super::{BASELINE_WRITE_VERSION, MAX_READABLE_VERSION, MIN_READABLE_VERSION};

    /// Stamp an outbound `format_version` from the node's active write `version`.
    /// A plain widening to the protobuf `uint32`; isolated as a function so every
    /// send site reads identically and a future stamping policy has one home.
    pub(super) fn stamp(version: u8) -> u32 {
        u32::from(version)
    }

    /// Normalize and range-check an inbound `format_version`. `0` (proto3 default
    /// from a pre-feature sender) maps to [`BASELINE_WRITE_VERSION`]; a present
    /// value must be inside `[MIN_READABLE_VERSION, MAX_READABLE_VERSION]`.
    /// Returns the `u8` version to decode at, or an error string the caller wraps
    /// in a transport-appropriate error (`tonic::Status` server-side, `RPCError`
    /// client-side). Fails loud rather than guessing a parser.
    pub(super) fn readable_version(format_version: u32) -> Result<u8, String> {
        let version = if format_version == 0 {
            BASELINE_WRITE_VERSION
        } else {
            u8::try_from(format_version).map_err(|_| {
                format!(
                    "format_version {format_version} outside readable range \
                     [{MIN_READABLE_VERSION}, {MAX_READABLE_VERSION}]"
                )
            })?
        };
        if version < MIN_READABLE_VERSION || version > MAX_READABLE_VERSION {
            return Err(format!(
                "format_version {version} outside readable range \
                 [{MIN_READABLE_VERSION}, {MAX_READABLE_VERSION}]"
            ));
        }
        Ok(version)
    }
}

/// Snapshot data is shipped in chunks of this many bytes. Sized to fit
/// comfortably inside the default gRPC max-frame limit (4 MiB) with room
/// for proto overhead and to keep per-RPC memory bounded on both sides.
/// The header chunk is sent separately and is small (a Vote + SnapshotMeta).
pub const SNAPSHOT_CHUNK_SIZE: usize = 1024 * 1024;

/// Upper bound on the *total* bytes the snapshot handler will reassemble from a
/// single client-streaming RPC. The handler refuses (with `ResourceExhausted`)
/// any stream whose cumulative `data` chunks would cross this line, so a peer
/// that can reach the raft port cannot drive the receiver to OOM by sending an
/// endless run of chunks. Sized generously for the small high-water state
/// machine; real deployments should size this against the largest realistic
/// state-machine snapshot.
pub const MAX_SNAPSHOT_BYTES: usize = 64 * 1024 * 1024;

/// Per-message decode/encode cap applied to the peer server. It must stay
/// strictly above `SNAPSHOT_CHUNK_SIZE`: every snapshot `Data` chunk is one
/// decoded protobuf message of up to that size, so a smaller cap would reject
/// legitimate chunks. Deriving it from the chunk size (plus headroom for the
/// proto and postcard framing) keeps that invariant impossible to break by
/// accident. `append_entries`/`vote` messages are far smaller, so this bounds
/// them too.
pub const MAX_PEER_MESSAGE_BYTES: usize = SNAPSHOT_CHUNK_SIZE + 256 * 1024;

/// Wall-clock ceiling on a single snapshot-install stream. Bounds a slow-loris
/// peer that opens a stream and dribbles bytes to keep the reassembly buffer
/// alive; sized against `MAX_SNAPSHOT_BYTES` arriving over a slow-but-legitimate
/// link.
const SNAPSHOT_STREAM_TIMEOUT: Duration = Duration::from_secs(60);

// ---------------------------------------------------------------------------
// Pool type + eviction helper.
// ---------------------------------------------------------------------------

type Pool = Arc<Mutex<HashMap<(NodeId, String), RaftPeerServiceClient<Channel>>>>;

/// A cheap, thread-safe reader of the node's current active write version.
/// Read at each RPC (not cached) so a future activation flip takes effect on the
/// next message without rebuilding the transport. Backed by the driver's
/// `active_write_version()` accessor at the construction site (see `mod.rs`).
pub type WriteVersionSource = Arc<dyn Fn() -> u8 + Send + Sync>;

// Generic over the value type so the keying/eviction logic is unit-testable
// without constructing a live RaftPeerServiceClient.
async fn evict<V>(pool: &Arc<Mutex<HashMap<(NodeId, String), V>>>, target: NodeId, addr: &str) {
    pool.lock().await.remove(&(target, addr.to_string()));
}

/// Drive a unary peer RPC under the caller's `RPCOption` hard-TTL deadline,
/// returning the decoded response body.
///
/// Why this exists: openraft only enforces `RPCOption` itself for some call
/// sites. `vote` and `transfer_leader` are wrapped in openraft's own
/// `C::timeout` (see `raft_core::broadcast_*`), but the replication
/// `append_entries` path is not — `stream_append_sequential` simply awaits
/// `network.append_entries(req, option)` and relies on the *transport* to honor
/// `option.hard_ttl()`. A transport that ignored the option left append on a
/// silently black-holed connection (no RST — NAT/firewall drop) wedged until TCP
/// keepalive eventually tripped (~2h by default), stalling replication to that
/// follower. Applying the deadline here closes that gap and keeps all three
/// unary RPCs uniformly bounded; for `vote`/`transfer_leader` it is harmless
/// belt-and-suspenders that simply fires no earlier than openraft's own timeout.
///
/// On any failure the pooled client for `(target, addr)` is evicted so the next
/// attempt reconnects fresh. A deadline elapse is surfaced as `Unreachable`
/// (openraft backs off before retrying) rather than `Network` (retry at once),
/// which is the right posture for a connection we just gave up on. Generic over
/// the pool value and response types so it is unit-testable without a live
/// `RaftPeerServiceClient`, mirroring [`evict`].
async fn unary_call<ClientHandle, Body>(
    pool: &Arc<Mutex<HashMap<(NodeId, String), ClientHandle>>>,
    target: NodeId,
    addr: &str,
    deadline: Duration,
    call: impl Future<Output = Result<tonic::Response<Body>, tonic::Status>>,
) -> Result<Body, RPCError<TypeConfig>> {
    match tokio::time::timeout(deadline, call).await {
        Ok(Ok(resp)) => Ok(resp.into_inner()),
        Ok(Err(status)) => {
            evict(pool, target, addr).await;
            Err(RPCError::Network(NetworkError::new(&status)))
        }
        Err(_elapsed) => {
            evict(pool, target, addr).await;
            let timed_out = std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("peer RPC exceeded {deadline:?} deadline"),
            );
            Err(RPCError::Unreachable(Unreachable::new(&timed_out)))
        }
    }
}

// ---------------------------------------------------------------------------
// PeerFactory — constructs PeerNetwork instances for each target node.
// ---------------------------------------------------------------------------

pub struct PeerFactory {
    pool: Pool,
    tls: Option<ClientTlsConfig>,
    active_write_version: WriteVersionSource,
}

impl PeerFactory {
    pub fn new(tls: Option<ClientTlsConfig>, active_write_version: WriteVersionSource) -> Self {
        Self {
            pool: Arc::new(Mutex::new(HashMap::new())),
            tls,
            active_write_version,
        }
    }
}

impl RaftNetworkFactory<TypeConfig> for PeerFactory {
    type Network = PeerNetwork;

    async fn new_client(&mut self, target: NodeId, node: &Node) -> Self::Network {
        PeerNetwork {
            target,
            addr: node.addr.clone(),
            pool: self.pool.clone(),
            tls: self.tls.clone(),
            active_write_version: self.active_write_version.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// PeerNetwork — implements RaftNetworkV2<TypeConfig> for one target peer.
// ---------------------------------------------------------------------------

pub struct PeerNetwork {
    target: NodeId,
    addr: String,
    pool: Pool,
    tls: Option<ClientTlsConfig>,
    active_write_version: WriteVersionSource,
}

/// Deadline for a single capability query. The gate is operator-initiated and
/// not on the hot path, so a generous fixed bound is fine; it exists only to
/// fail closed on a black-holed peer rather than hang the gate forever.
const CAPABILITY_QUERY_TIMEOUT: Duration = Duration::from_secs(10);

/// Concrete [`CapabilitySource`](tsoracle_driver_openraft::CapabilitySource)
/// that dials a member via the `Capabilities` peer RPC. Constructed in the
/// transport crate (where `PeerNetwork` lives) and handed to
/// `StandaloneHost::run_activation_gate`; the driver crate stays free of a
/// dependency on this crate.
pub struct PeerCapabilitySource {
    pool: Pool,
    tls: Option<ClientTlsConfig>,
}

impl PeerCapabilitySource {
    // The only in-workspace caller today is the round-trip test; a later
    // phase wires this into the production `initiate_format_activation`
    // path. `pub` is the intended surface — this `allow` documents that and
    // unblocks the workspace build's `-D warnings` lint.
    #[allow(dead_code)]
    pub fn new(tls: Option<ClientTlsConfig>) -> Self {
        Self {
            pool: Arc::new(Mutex::new(HashMap::new())),
            tls,
        }
    }
}

#[async_trait::async_trait]
impl tsoracle_driver_openraft::CapabilitySource for PeerCapabilitySource {
    type Node = Node;

    async fn query(&self, node_id: NodeId, member: &Node) -> Result<NodeCapabilities, String> {
        // The `Capabilities` request payload is empty and the response body
        // is not version-framed, so the outbound carrier's `format_version`
        // is informational only. Stamping `BASELINE_WRITE_VERSION` is correct
        // for any pre-activation node (and any post-activation node that has
        // not yet handled the bump apply) — the gate is exactly the operator
        // step that precedes a target bump.
        let network = PeerNetwork {
            target: node_id,
            addr: member.addr.clone(),
            pool: self.pool.clone(),
            tls: self.tls.clone(),
            active_write_version: Arc::new(|| tsoracle_openraft_toolkit::BASELINE_WRITE_VERSION),
        };
        network
            .capabilities(CAPABILITY_QUERY_TIMEOUT)
            .await
            .map_err(|err| format!("{err:?}"))
    }
}

/// Build the postcard body of a `Capabilities` reply for the local node with
/// the given active write version. Split out from the tonic handler so the
/// decode→build→encode contract is unit-testable without a live `Raft`.
fn capabilities_response(active_write_version: u8) -> Vec<u8> {
    let capabilities = NodeCapabilities::local(active_write_version);
    // `NodeCapabilities` is three `u8`s; postcard encoding is infallible for
    // it, but we surface a definite `Vec` either way to keep the handler total.
    postcard::to_stdvec(&capabilities).unwrap_or_default()
}

impl PeerNetwork {
    /// Return a cached (or freshly connected) tonic client to the target.
    async fn client(&self) -> Result<RaftPeerServiceClient<Channel>, RPCError<TypeConfig>> {
        let key = (self.target, self.addr.clone());
        {
            let pool = self.pool.lock().await;
            if let Some(client) = pool.get(&key) {
                return Ok(client.clone());
            }
        }
        let channel = match &self.tls {
            Some(tls) => Channel::from_shared(format!("https://{}", self.addr))
                .map_err(|err| RPCError::Unreachable(Unreachable::new(&err)))?
                .tls_config(tls.clone())
                .map_err(|err| RPCError::Unreachable(Unreachable::new(&err)))?
                .connect()
                .await
                .map_err(|err| RPCError::Unreachable(Unreachable::new(&err)))?,
            None => Channel::from_shared(format!("http://{}", self.addr))
                .map_err(|err| RPCError::Unreachable(Unreachable::new(&err)))?
                .connect()
                .await
                .map_err(|err| RPCError::Unreachable(Unreachable::new(&err)))?,
        };
        let client = RaftPeerServiceClient::new(channel);
        self.pool.lock().await.insert(key, client.clone());
        Ok(client)
    }

    /// Query the target peer for its format-migration capabilities. Not part
    /// of `RaftNetworkV2` (openraft has no such RPC); used only by the
    /// leader-side activation gate. The request payload is empty; the reply
    /// is a postcard `NodeCapabilities`.
    ///
    /// `format_version` on the carrier `RaftMessage` is stamped from the
    /// node's active write version (same as every other outbound message),
    /// but the response body itself is NOT version-framed — its scalar-u8
    /// shape never changes, it is the bootstrap message that tells the caller
    /// what versions the peer supports.
    pub async fn capabilities(
        &self,
        deadline: Duration,
    ) -> Result<NodeCapabilities, RPCError<TypeConfig>> {
        let mut client = self.client().await?;
        let reply = unary_call(
            &self.pool,
            self.target,
            &self.addr,
            deadline,
            client.capabilities(RaftMessage {
                payload: Vec::new(),
                format_version: wire::stamp((self.active_write_version)()),
            }),
        )
        .await?;
        let capabilities: NodeCapabilities = postcard::from_bytes(&reply.payload)
            .map_err(|err| RPCError::Network(NetworkError::new(&err)))?;
        Ok(capabilities)
    }
}

impl RaftNetworkV2<TypeConfig> for PeerNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>> {
        let mut c = self.client().await?;
        let payload =
            postcard::to_stdvec(&rpc).map_err(|err| RPCError::Network(NetworkError::new(&err)))?;
        let reply = unary_call(
            &self.pool,
            self.target,
            &self.addr,
            option.hard_ttl(),
            c.append_entries(RaftMessage {
                payload,
                format_version: wire::stamp((self.active_write_version)()),
            }),
        )
        .await?;
        let _version = wire::readable_version(reply.format_version)
            .map_err(|err| RPCError::Network(NetworkError::new(&std::io::Error::other(err))))?;
        let body: AppendEntriesResponse<TypeConfig> = postcard::from_bytes(&reply.payload)
            .map_err(|err| RPCError::Network(NetworkError::new(&err)))?;
        Ok(body)
    }

    /// Forward a leadership-transfer request to the target peer. openraft calls
    /// this on the outgoing leader when `trigger().transfer_leader` fires; the
    /// receiver hands the request to `Raft::handle_transfer_leader`. Without
    /// this override the default no-op drops the request and leadership only
    /// moves on the next election timeout.
    async fn transfer_leader(
        &mut self,
        req: TransferLeaderRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<(), RPCError<TypeConfig>> {
        let mut c = self.client().await?;
        let payload =
            postcard::to_stdvec(&req).map_err(|err| RPCError::Network(NetworkError::new(&err)))?;
        // The reply payload is empty by contract; we only care that the RPC
        // landed within the deadline.
        unary_call(
            &self.pool,
            self.target,
            &self.addr,
            option.hard_ttl(),
            c.transfer_leader(RaftMessage {
                payload,
                format_version: wire::stamp((self.active_write_version)()),
            }),
        )
        .await?;
        Ok(())
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        let mut c = self.client().await?;
        let payload =
            postcard::to_stdvec(&rpc).map_err(|err| RPCError::Network(NetworkError::new(&err)))?;
        let reply = unary_call(
            &self.pool,
            self.target,
            &self.addr,
            option.hard_ttl(),
            c.vote(RaftMessage {
                payload,
                format_version: wire::stamp((self.active_write_version)()),
            }),
        )
        .await?;
        let _version = wire::readable_version(reply.format_version)
            .map_err(|err| RPCError::Network(NetworkError::new(&std::io::Error::other(err))))?;
        let body: VoteResponse<TypeConfig> = postcard::from_bytes(&reply.payload)
            .map_err(|err| RPCError::Network(NetworkError::new(&err)))?;
        Ok(body)
    }

    /// Send a snapshot to the target as a stream of `SnapshotChunk`s.
    ///
    /// Stream layout: one `header` chunk (postcard vote + postcard meta),
    /// followed by `ceil(data.len() / SNAPSHOT_CHUNK_SIZE)` `data` chunks.
    /// An empty data buffer is permitted and results in zero `data` chunks.
    ///
    /// Cancellation: if `cancel` resolves before the server responds, the
    /// in-flight stream is dropped (closing the client side of the stream)
    /// and we return `StreamingError::Closed`.
    async fn full_snapshot(
        &mut self,
        vote: VoteOf<TypeConfig>,
        snapshot: SnapshotOf<TypeConfig>,
        cancel: impl Future<Output = ReplicationClosed> + openraft::OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        // Pre-encode the header fields. Both are small (Vote + SnapshotMeta).
        let vote_bytes = postcard::to_stdvec(&vote)
            .map_err(|e| StreamingError::Network(NetworkError::new(&e)))?;
        let meta_bytes = postcard::to_stdvec(&snapshot.meta)
            .map_err(|e| StreamingError::Network(NetworkError::new(&e)))?;
        let data_bytes = snapshot.snapshot.into_inner();

        // Build the chunk stream lazily. We materialize chunks into owned
        // Vecs (a copy per chunk) because prost generates `bytes` fields as
        // `Vec<u8>` by default. The original `data_bytes` is freed when the
        // iterator is exhausted; total transient memory is ≈ data_bytes plus
        // one in-flight chunk.
        let header_chunk = SnapshotChunk {
            kind: Some(ChunkKind::Header(SnapshotHeader {
                vote: vote_bytes,
                meta: meta_bytes,
                format_version: wire::stamp((self.active_write_version)()),
            })),
        };

        let data_chunks = data_bytes
            .chunks(SNAPSHOT_CHUNK_SIZE)
            .map(|c| SnapshotChunk {
                kind: Some(ChunkKind::Data(c.to_vec())),
            })
            .collect::<Vec<_>>();

        let outbound =
            futures::stream::iter(std::iter::once(header_chunk).chain(data_chunks.into_iter()));

        let mut c = self.client().await.map_err(|e| match e {
            RPCError::Network(n) => StreamingError::Network(n),
            RPCError::Unreachable(u) => StreamingError::Unreachable(u),
            RPCError::Timeout(t) => StreamingError::Timeout(t),
            // RPCError::RemoteError is Infallible in the default type param
        })?;

        // Drive the streaming RPC concurrently with the cancellation future.
        // If `cancel` fires first, dropping the in-flight future closes the
        // outbound stream and aborts on the server side.
        tokio::select! {
            result = c.snapshot(outbound) => {
                let raw = match result {
                    Ok(resp) => resp,
                    Err(err) => {
                        evict(&self.pool, self.target, &self.addr).await;
                        return Err(StreamingError::Network(NetworkError::new(&err)));
                    }
                };
                let inner = raw.into_inner();
                let _version = wire::readable_version(inner.format_version).map_err(|err| {
                    StreamingError::Network(NetworkError::new(&std::io::Error::other(err)))
                })?;
                let resp: SnapshotResponse<TypeConfig> = postcard::from_bytes(&inner.payload)
                    .map_err(|err| StreamingError::Network(NetworkError::new(&err)))?;
                Ok(resp)
            }
            closed = cancel => {
                Err(StreamingError::Closed(closed))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// PeerServiceImpl — tonic server, demuxes RaftMessage → Raft API calls.
// ---------------------------------------------------------------------------

pub struct PeerServiceImpl<SM = ()> {
    pub raft: openraft::Raft<TypeConfig, SM>,
    pub active_write_version: WriteVersionSource,
}

#[tonic::async_trait]
impl<SM: Send + Sync + 'static> RaftPeerService for PeerServiceImpl<SM> {
    async fn append_entries(
        &self,
        request: tonic::Request<RaftMessage>,
    ) -> Result<tonic::Response<RaftMessage>, tonic::Status> {
        let message = request.into_inner();
        let _version = wire::readable_version(message.format_version)
            .map_err(tonic::Status::invalid_argument)?;
        let body: AppendEntriesRequest<TypeConfig> = postcard::from_bytes(&message.payload)
            .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;
        let resp = self
            .raft
            .append_entries(body)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;
        let payload =
            postcard::to_stdvec(&resp).map_err(|e| tonic::Status::internal(e.to_string()))?;
        Ok(tonic::Response::new(RaftMessage {
            payload,
            format_version: wire::stamp((self.active_write_version)()),
        }))
    }

    async fn vote(
        &self,
        request: tonic::Request<RaftMessage>,
    ) -> Result<tonic::Response<RaftMessage>, tonic::Status> {
        let message = request.into_inner();
        let _version = wire::readable_version(message.format_version)
            .map_err(tonic::Status::invalid_argument)?;
        let body: VoteRequest<TypeConfig> = postcard::from_bytes(&message.payload)
            .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;
        let resp = self
            .raft
            .vote(body)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;
        let payload =
            postcard::to_stdvec(&resp).map_err(|e| tonic::Status::internal(e.to_string()))?;
        Ok(tonic::Response::new(RaftMessage {
            payload,
            format_version: wire::stamp((self.active_write_version)()),
        }))
    }

    async fn transfer_leader(
        &self,
        request: tonic::Request<RaftMessage>,
    ) -> Result<tonic::Response<RaftMessage>, tonic::Status> {
        let message = request.into_inner();
        let _version = wire::readable_version(message.format_version)
            .map_err(tonic::Status::invalid_argument)?;
        let body: TransferLeaderRequest<TypeConfig> = postcard::from_bytes(&message.payload)
            .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;
        self.raft
            .handle_transfer_leader(body)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;
        Ok(tonic::Response::new(RaftMessage {
            payload: Vec::new(),
            format_version: wire::stamp((self.active_write_version)()),
        }))
    }

    /// Reassemble a streamed snapshot and hand it to `install_full_snapshot`.
    ///
    /// Framing and the byte/time bounds live in [`reassemble_snapshot`]; this
    /// handler just enforces the per-stream wall-clock limit and forwards the
    /// result to the local `Raft`.
    async fn snapshot(
        &self,
        request: tonic::Request<tonic::Streaming<SnapshotChunk>>,
    ) -> Result<tonic::Response<RaftMessage>, tonic::Status> {
        let assembled = tokio::time::timeout(
            SNAPSHOT_STREAM_TIMEOUT,
            reassemble_snapshot(request.into_inner(), MAX_SNAPSHOT_BYTES),
        )
        .await
        .map_err(|_| tonic::Status::deadline_exceeded("snapshot stream timed out"))??;

        let snapshot = openraft::storage::Snapshot {
            meta: assembled.meta,
            snapshot: Cursor::new(assembled.data),
        };

        let resp = self
            .raft
            .install_full_snapshot(assembled.vote, snapshot)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        let payload =
            postcard::to_stdvec(&resp).map_err(|e| tonic::Status::internal(e.to_string()))?;
        Ok(tonic::Response::new(RaftMessage {
            payload,
            format_version: wire::stamp((self.active_write_version)()),
        }))
    }

    /// Answer with the local node's format-migration capabilities. The
    /// request payload is ignored (the query takes no arguments); the
    /// envelope's `format_version` is ignored too because the response body
    /// itself is not version-framed (its scalar-u8 shape is the bootstrap
    /// message that tells the caller which versions to use). The reply
    /// carrier still stamps `format_version` for symmetry with the rest of
    /// the peer transport.
    async fn capabilities(
        &self,
        _request: tonic::Request<RaftMessage>,
    ) -> Result<tonic::Response<RaftMessage>, tonic::Status> {
        let payload = capabilities_response((self.active_write_version)());
        Ok(tonic::Response::new(RaftMessage {
            payload,
            format_version: wire::stamp((self.active_write_version)()),
        }))
    }
}

/// A snapshot reassembled from a peer's client-streaming RPC.
#[derive(Debug)]
struct AssembledSnapshot {
    vote: VoteOf<TypeConfig>,
    meta: openraft::type_config::alias::SnapshotMetaOf<TypeConfig>,
    data: Vec<u8>,
}

/// Parse the leading header chunk, then concatenate trailing `data` chunks into
/// a single buffer, bounding the total at `max_bytes`.
///
/// Framing contract (mirrors `proto/raft_peer.proto`):
///   - exactly one `header` chunk at the start;
///   - zero or more `data` chunks afterwards;
///   - any other ordering is rejected as `InvalidArgument`.
///
/// The running total is checked *before* each chunk is buffered, so a stream
/// that would cross `max_bytes` is refused with `ResourceExhausted` without the
/// buffer ever exceeding the limit — this is what keeps a reachable peer from
/// driving the receiver to OOM by sending an unbounded run of chunks. Generic
/// over the stream so the bound is unit-testable: `tonic::Streaming` is not
/// constructible in a test, but a `futures::stream::iter` is.
async fn reassemble_snapshot<S>(
    mut stream: S,
    max_bytes: usize,
) -> Result<AssembledSnapshot, tonic::Status>
where
    S: futures::Stream<Item = Result<SnapshotChunk, tonic::Status>> + Unpin,
{
    // The first chunk must be the header.
    let first = stream
        .next()
        .await
        .ok_or_else(|| tonic::Status::invalid_argument("snapshot stream ended before header"))?
        .map_err(|e| tonic::Status::internal(format!("snapshot stream error: {e}")))?;
    let header = match first.kind {
        Some(ChunkKind::Header(h)) => h,
        Some(ChunkKind::Data(_)) => {
            return Err(tonic::Status::invalid_argument(
                "first snapshot chunk must be a header",
            ));
        }
        None => {
            return Err(tonic::Status::invalid_argument(
                "snapshot chunk missing kind",
            ));
        }
    };

    // The header's format_version covers both the vote and meta postcard
    // bodies. The streamed `data` chunks are self-describing on-disk blobs
    // and are NOT re-checked here (see proto/raft_peer.proto SnapshotHeader).
    let _version =
        wire::readable_version(header.format_version).map_err(tonic::Status::invalid_argument)?;

    let vote: VoteOf<TypeConfig> = postcard::from_bytes(&header.vote)
        .map_err(|e| tonic::Status::invalid_argument(format!("bad vote: {e}")))?;
    let meta: openraft::type_config::alias::SnapshotMetaOf<TypeConfig> =
        postcard::from_bytes(&header.meta)
            .map_err(|e| tonic::Status::invalid_argument(format!("bad meta: {e}")))?;

    // Reassemble subsequent data chunks, refusing the stream the moment its
    // cumulative size would exceed `max_bytes`.
    let mut data: Vec<u8> = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk =
            chunk.map_err(|e| tonic::Status::internal(format!("snapshot stream error: {e}")))?;
        match chunk.kind {
            Some(ChunkKind::Data(bytes)) => {
                if data.len() + bytes.len() > max_bytes {
                    return Err(tonic::Status::resource_exhausted(format!(
                        "snapshot exceeds {max_bytes}-byte reassembly limit"
                    )));
                }
                data.extend_from_slice(&bytes);
            }
            Some(ChunkKind::Header(_)) => {
                return Err(tonic::Status::invalid_argument(
                    "unexpected header chunk after first",
                ));
            }
            None => {
                return Err(tonic::Status::invalid_argument(
                    "snapshot chunk missing kind",
                ));
            }
        }
    }

    Ok(AssembledSnapshot { vote, meta, data })
}

/// Construct the tonic server-side handler for the RaftPeerService. The
/// `active_write_version` source is consulted to stamp `format_version` on
/// every response body (read per-RPC so a future activation flip is picked up
/// live).
pub fn server<SM: Send + Sync + 'static>(
    raft: openraft::Raft<TypeConfig, SM>,
    active_write_version: WriteVersionSource,
) -> RaftPeerServiceServer<PeerServiceImpl<SM>> {
    RaftPeerServiceServer::new(PeerServiceImpl {
        raft,
        active_write_version,
    })
}

#[cfg(test)]
mod tls_tests {
    use super::*;
    use crate::config::PeerTlsConfig;
    use crate::peer_tls::build_peer_tls;
    use std::sync::Arc;
    use tonic::transport::{Certificate, ClientTlsConfig, Identity};

    // --- cert helpers (rcgen 0.13) ---
    struct Certs {
        ca_pem: String,
        node_cert: String,
        node_key: String,
        other_leaf_cert: String,
        other_leaf_key: String,
    }

    fn mint() -> Certs {
        use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
        let mk_ca = |name: &str| {
            let key = KeyPair::generate().unwrap();
            let mut p = CertificateParams::new(vec![name.to_string()]).unwrap();
            p.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
            let cert = p.self_signed(&key).unwrap();
            (cert, key)
        };
        let (ca, ca_key) = mk_ca("tso-ca");
        let leaf_key = KeyPair::generate().unwrap();
        let leaf_params =
            CertificateParams::new(vec!["localhost".to_string(), "127.0.0.1".to_string()]).unwrap();
        let leaf = leaf_params.signed_by(&leaf_key, &ca, &ca_key).unwrap();
        let (other_ca, other_ca_key) = mk_ca("other-ca");
        let other_key = KeyPair::generate().unwrap();
        let other_params = CertificateParams::new(vec!["127.0.0.1".to_string()]).unwrap();
        let other_leaf = other_params
            .signed_by(&other_key, &other_ca, &other_ca_key)
            .unwrap();
        Certs {
            ca_pem: ca.pem(),
            node_cert: leaf.pem(),
            node_key: leaf_key.serialize_pem(),
            other_leaf_cert: other_leaf.pem(),
            other_leaf_key: other_key.serialize_pem(),
        }
    }

    fn node_material(c: &Certs, dir: &std::path::Path) -> crate::peer_tls::PeerTlsMaterial {
        let cert = dir.join("n.crt");
        let key = dir.join("n.key");
        let ca = dir.join("ca.crt");
        std::fs::write(&cert, &c.node_cert).unwrap();
        std::fs::write(&key, &c.node_key).unwrap();
        std::fs::write(&ca, &c.ca_pem).unwrap();
        build_peer_tls(&PeerTlsConfig { cert, key, ca }).unwrap()
    }

    // Minimal stub server (handlers never called — only the TLS handshake is).
    #[derive(Clone)]
    struct Stub;

    #[tonic::async_trait]
    impl proto::raft_peer_service_server::RaftPeerService for Stub {
        async fn append_entries(
            &self,
            _: tonic::Request<proto::RaftMessage>,
        ) -> Result<tonic::Response<proto::RaftMessage>, tonic::Status> {
            Err(tonic::Status::unimplemented("stub"))
        }
        async fn vote(
            &self,
            _: tonic::Request<proto::RaftMessage>,
        ) -> Result<tonic::Response<proto::RaftMessage>, tonic::Status> {
            Err(tonic::Status::unimplemented("stub"))
        }
        async fn snapshot(
            &self,
            _: tonic::Request<tonic::Streaming<proto::SnapshotChunk>>,
        ) -> Result<tonic::Response<proto::RaftMessage>, tonic::Status> {
            Err(tonic::Status::unimplemented("stub"))
        }
        async fn transfer_leader(
            &self,
            _: tonic::Request<proto::RaftMessage>,
        ) -> Result<tonic::Response<proto::RaftMessage>, tonic::Status> {
            Err(tonic::Status::unimplemented("stub"))
        }
        async fn capabilities(
            &self,
            _: tonic::Request<proto::RaftMessage>,
        ) -> Result<tonic::Response<proto::RaftMessage>, tonic::Status> {
            Err(tonic::Status::unimplemented("stub"))
        }
    }

    async fn spawn_stub(server_tls: tonic::transport::ServerTlsConfig) -> std::net::SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .tls_config(server_tls)
                .unwrap()
                .add_service(proto::raft_peer_service_server::RaftPeerServiceServer::new(
                    Stub,
                ))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .ok();
        });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        addr
    }

    fn make_net(addr: std::net::SocketAddr, tls: Option<ClientTlsConfig>) -> PeerNetwork {
        PeerNetwork {
            target: 2,
            addr: addr.to_string(),
            pool: Arc::new(Mutex::new(HashMap::new())),
            tls,
            active_write_version: Arc::new(|| tsoracle_openraft_toolkit::BASELINE_WRITE_VERSION),
        }
    }

    // `Channel::connect()` is lazy — the TLS handshake happens on the first RPC.
    // We therefore attempt a real RPC (append_entries) and check whether it fails
    // at the transport level (tonic Status) vs. at the stub handler (Unimplemented).
    // A successful mTLS handshake produces Unimplemented; a rejected one produces
    // a connection-level error (Unavailable / Unknown).
    async fn probe(net: PeerNetwork) -> tonic::Code {
        match net.client().await {
            Err(_) => tonic::Code::Unavailable,
            Ok(mut c) => {
                match c
                    .append_entries(tonic::Request::new(proto::RaftMessage {
                        payload: Vec::new(),
                        format_version: 0,
                    }))
                    .await
                {
                    Ok(_) => tonic::Code::Ok,
                    Err(s) => s.code(),
                }
            }
        }
    }

    #[tokio::test]
    async fn valid_node_cert_connects() {
        let dir = tempfile::tempdir().unwrap();
        let c = mint();
        let m = node_material(&c, dir.path());
        let addr = spawn_stub(m.server.clone()).await;
        // Stub returns Unimplemented — handshake succeeded.
        assert_eq!(
            probe(make_net(addr, Some(m.client.clone()))).await,
            tonic::Code::Unimplemented
        );
    }

    #[tokio::test]
    async fn no_client_cert_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let c = mint();
        let m = node_material(&c, dir.path());
        let addr = spawn_stub(m.server.clone()).await;
        let no_id = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(&c.ca_pem))
            .domain_name("localhost");
        let code = probe(make_net(addr, Some(no_id))).await;
        assert_ne!(
            code,
            tonic::Code::Unimplemented,
            "server must reject a client with no cert"
        );
    }

    #[tokio::test]
    async fn wrong_ca_client_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let c = mint();
        let m = node_material(&c, dir.path());
        let addr = spawn_stub(m.server.clone()).await;
        let wrong = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(&c.ca_pem))
            .identity(Identity::from_pem(&c.other_leaf_cert, &c.other_leaf_key))
            .domain_name("localhost");
        let code = probe(make_net(addr, Some(wrong))).await;
        assert_ne!(
            code,
            tonic::Code::Unimplemented,
            "server must reject a cert from a foreign CA"
        );
    }

    #[tokio::test]
    async fn plaintext_against_tls_fails() {
        let dir = tempfile::tempdir().unwrap();
        let c = mint();
        let m = node_material(&c, dir.path());
        let addr = spawn_stub(m.server.clone()).await;
        let code = probe(make_net(addr, None)).await;
        assert_ne!(
            code,
            tonic::Code::Unimplemented,
            "plaintext must not reach a TLS-only server"
        );
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use openraft::Vote;
    use openraft::type_config::alias::SnapshotMetaOf;
    use tokio::sync::Mutex;

    use super::*;

    #[tokio::test]
    async fn pool_key_distinguishes_addr_changes() {
        // Same NodeId, different addr, must not collide in the pool.
        let mut map: HashMap<(u64, String), u8> = HashMap::new();
        map.insert((1, "old:1".to_string()), 0);
        assert!(!map.contains_key(&(1, "new:1".to_string())));
        assert!(map.contains_key(&(1, "old:1".to_string())));
    }

    #[tokio::test]
    async fn evict_removes_only_the_targeted_entry() {
        let pool: Arc<Mutex<HashMap<(u64, String), u8>>> = Arc::new(Mutex::new(HashMap::new()));
        {
            let mut guard = pool.lock().await;
            guard.insert((1, "a:1".to_string()), 0);
            guard.insert((2, "b:2".to_string()), 0);
        }
        evict(&pool, 1, "a:1").await;
        let guard = pool.lock().await;
        assert!(!guard.contains_key(&(1, "a:1".to_string())));
        assert!(guard.contains_key(&(2, "b:2".to_string())));
    }

    fn seeded_pool() -> Arc<Mutex<HashMap<(u64, String), u8>>> {
        let pool = Arc::new(Mutex::new(HashMap::new()));
        pool.try_lock().unwrap().insert((1, "a:1".to_string()), 0);
        pool
    }

    // A call that never resolves must be cut at the hard-TTL deadline and
    // surfaced as `Unreachable` (so openraft backs off), with the pooled client
    // evicted so the next attempt reconnects. A never-resolving future against a
    // short real deadline is deterministic: the timer is the only thing that can
    // complete the call.
    #[tokio::test]
    async fn unary_call_deadline_elapse_evicts_and_reports_unreachable() {
        let pool = seeded_pool();
        let never = std::future::pending::<Result<tonic::Response<u8>, tonic::Status>>();
        let err = unary_call(&pool, 1, "a:1", Duration::from_millis(10), never)
            .await
            .expect_err("a never-resolving call must hit the deadline");
        assert!(matches!(err, RPCError::Unreachable(_)));
        assert!(!pool.lock().await.contains_key(&(1, "a:1".to_string())));
    }

    // A transport error inside the deadline propagates as `Network` (retry at
    // once) and also evicts the client.
    #[tokio::test]
    async fn unary_call_transport_error_evicts_and_reports_network() {
        let pool = seeded_pool();
        let failed = async { Err::<tonic::Response<u8>, _>(tonic::Status::unavailable("down")) };
        let err = unary_call(&pool, 1, "a:1", Duration::from_secs(5), failed)
            .await
            .expect_err("a transport error must propagate");
        assert!(matches!(err, RPCError::Network(_)));
        assert!(!pool.lock().await.contains_key(&(1, "a:1".to_string())));
    }

    // A successful call returns the decoded body and must NOT evict the client.
    #[tokio::test]
    async fn unary_call_success_returns_body_and_keeps_client() {
        let pool = seeded_pool();
        let ok = async { Ok(tonic::Response::new(42u8)) };
        let body = unary_call(&pool, 1, "a:1", Duration::from_secs(5), ok)
            .await
            .expect("a successful call returns its body");
        assert_eq!(body, 42);
        assert!(pool.lock().await.contains_key(&(1, "a:1".to_string())));
    }

    fn header_chunk() -> SnapshotChunk {
        let vote: VoteOf<TypeConfig> = Vote::new(1, 1);
        let meta = SnapshotMetaOf::<TypeConfig> {
            last_log_id: None,
            last_membership: Default::default(),
            snapshot_id: "test-snap".to_string(),
        };
        SnapshotChunk {
            kind: Some(ChunkKind::Header(SnapshotHeader {
                vote: postcard::to_stdvec(&vote).expect("encode vote"),
                meta: postcard::to_stdvec(&meta).expect("encode meta"),
                format_version: wire::stamp(tsoracle_openraft_toolkit::BASELINE_WRITE_VERSION),
            })),
        }
    }

    fn data_chunk(bytes: &[u8]) -> SnapshotChunk {
        SnapshotChunk {
            kind: Some(ChunkKind::Data(bytes.to_vec())),
        }
    }

    fn ok_stream(
        chunks: Vec<SnapshotChunk>,
    ) -> impl futures::Stream<Item = Result<SnapshotChunk, tonic::Status>> + Unpin {
        futures::stream::iter(chunks.into_iter().map(Ok))
    }

    #[tokio::test]
    async fn snapshot_over_limit_is_resource_exhausted() {
        // Two 600-byte data chunks (1200 bytes total) against a 1 KiB ceiling:
        // the second chunk pushes the running total past the limit and must be
        // refused before it is buffered.
        let chunks = vec![
            header_chunk(),
            data_chunk(&[0u8; 600]),
            data_chunk(&[0u8; 600]),
        ];
        let err = reassemble_snapshot(ok_stream(chunks), 1024)
            .await
            .expect_err("over-limit stream must be rejected");
        assert_eq!(err.code(), tonic::Code::ResourceExhausted);
    }

    #[tokio::test]
    async fn snapshot_under_limit_assembles() {
        let chunks = vec![header_chunk(), data_chunk(b"hello "), data_chunk(b"world")];
        let assembled = reassemble_snapshot(ok_stream(chunks), 1024)
            .await
            .expect("under-limit stream assembles");
        assert_eq!(assembled.data, b"hello world");
    }

    #[tokio::test]
    async fn data_before_header_is_invalid_argument() {
        let chunks = vec![data_chunk(b"premature")];
        let err = reassemble_snapshot(ok_stream(chunks), 1024)
            .await
            .expect_err("data before header must be rejected");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    // ---- wire envelope helpers ----

    #[test]
    fn absent_format_version_reads_as_baseline() {
        // proto3 default 0 (a pre-feature sender) is interpreted as BASELINE.
        let version = wire::readable_version(0).expect("0 normalizes to baseline");
        assert_eq!(version, tsoracle_openraft_toolkit::BASELINE_WRITE_VERSION);
    }

    #[test]
    fn in_range_format_version_passes_through() {
        // A stamped in-range version is returned unchanged. Asserted against
        // the constant so this survives a future MAX bump.
        let stamped = tsoracle_openraft_toolkit::BASELINE_WRITE_VERSION;
        let version = wire::readable_version(u32::from(stamped)).expect("in-range");
        assert_eq!(version, stamped);
    }

    #[test]
    fn out_of_range_format_version_is_rejected() {
        // Above MAX_READABLE_VERSION: no parser, must fail loud.
        let too_new = u32::from(tsoracle_openraft_toolkit::MAX_READABLE_VERSION) + 1;
        let err = wire::readable_version(too_new).expect_err("out-of-range rejected");
        assert!(
            err.contains("format_version"),
            "message names the field: {err}"
        );
    }

    #[test]
    fn stamp_widens_to_u32() {
        // The send side widens the active u8 write version to the protobuf
        // uint32. The stamp is a plain widening; no transformation.
        assert_eq!(wire::stamp(3), 3u32);
        assert_eq!(wire::stamp(255), 255u32);
    }

    #[tokio::test]
    async fn peer_network_holds_the_active_write_version_source() {
        // The factory's version source feeds PeerNetwork; a node at BASELINE
        // reads BASELINE through the source.
        let factory_version = Arc::new(std::sync::atomic::AtomicU8::new(
            tsoracle_openraft_toolkit::BASELINE_WRITE_VERSION,
        ));
        let version_for_source = factory_version.clone();
        let mut factory = PeerFactory::new(
            None,
            Arc::new(move || version_for_source.load(std::sync::atomic::Ordering::Relaxed)),
        );
        let node = Node {
            addr: "127.0.0.1:1".to_string(),
            service_endpoint: String::new(),
            admin_endpoint: String::new(),
        };
        let net = factory.new_client(7, &node).await;
        assert_eq!(
            (net.active_write_version)(),
            tsoracle_openraft_toolkit::BASELINE_WRITE_VERSION
        );
        // The source is shared: flipping the underlying atomic shows through.
        factory_version.store(5, std::sync::atomic::Ordering::Relaxed);
        assert_eq!((net.active_write_version)(), 5);
    }

    // ---- single-node Raft test helper ----
    //
    // The round-trip and out-of-range tests want a real `PeerServiceImpl` on a
    // loopback listener so they exercise the actual stamp/read code, not a
    // stub. That requires a live `Raft<TypeConfig, HighWaterStateMachine>`.
    // This helper builds the minimum: a temp RocksDB, a state machine with
    // the shared cell, and a Raft initialized as a single-voter cluster.
    mod test_support {
        use super::*;
        use openraft::{Config, Raft};
        use rocksdb::{ColumnFamilyDescriptor, DB, Options};
        use std::collections::BTreeMap;
        use tempfile::TempDir;
        use tsoracle_driver_openraft::{
            HighWaterStateMachine, OpenraftLogCodec, OpenraftPeer, RocksdbSnapshotStore,
            SnapshotStore,
        };
        use tsoracle_openraft_toolkit::{ActiveWriteVersion, Flat, RocksdbLogStore};

        pub(super) async fn single_node_raft() -> (Raft<TypeConfig, HighWaterStateMachine>, TempDir)
        {
            let dir = tempfile::tempdir().expect("tempdir");
            let mut opts = Options::default();
            opts.create_if_missing(true);
            opts.create_missing_column_families(true);
            let cfs = vec![
                ColumnFamilyDescriptor::new("raft_log", Options::default()),
                ColumnFamilyDescriptor::new("raft_meta", Options::default()),
                ColumnFamilyDescriptor::new("raft_snapshot", Options::default()),
            ];
            let db =
                Arc::new(DB::open_cf_descriptors(&opts, dir.path(), cfs).expect("open rocksdb"));
            let cell = ActiveWriteVersion::default();
            let log_store: RocksdbLogStore<TypeConfig, Flat, OpenraftLogCodec> =
                RocksdbLogStore::open(db.clone(), "raft_log", "raft_meta", Flat)
                    .expect("open log store")
                    .with_active_write_version(cell.clone());
            let snapshot_store: Arc<dyn SnapshotStore> = Arc::new(
                RocksdbSnapshotStore::open(db, "raft_snapshot").expect("open snapshot store"),
            );
            let state_machine =
                HighWaterStateMachine::with_store_and_active_version(snapshot_store, cell)
                    .expect("state machine");
            let config = Arc::new(
                Config {
                    heartbeat_interval: 50,
                    election_timeout_min: 150,
                    election_timeout_max: 300,
                    ..Default::default()
                }
                .validate()
                .expect("validate config"),
            );
            let version_source: WriteVersionSource =
                Arc::new(|| tsoracle_openraft_toolkit::BASELINE_WRITE_VERSION);
            let network = PeerFactory::new(None, version_source);
            let raft = Raft::<TypeConfig, HighWaterStateMachine>::new(
                1,
                config,
                network,
                log_store,
                state_machine,
            )
            .await
            .expect("raft new");
            let mut members: BTreeMap<u64, OpenraftPeer> = BTreeMap::new();
            members.insert(
                1,
                OpenraftPeer {
                    addr: "127.0.0.1:1".to_string(),
                    service_endpoint: String::new(),
                    admin_endpoint: String::new(),
                },
            );
            let _ = raft.initialize(members).await;
            (raft, dir)
        }
    }

    // ---- end-to-end round-trip + out-of-range rejection ----

    #[tokio::test]
    async fn vote_round_trips_with_baseline_format_version() {
        use openraft::Vote;

        let (raft, _temp) = test_support::single_node_raft().await;
        let version_source: WriteVersionSource =
            Arc::new(|| tsoracle_openraft_toolkit::BASELINE_WRITE_VERSION);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let service = server(raft, version_source.clone());
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .ok();
        });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let mut net = PeerNetwork {
            target: 1,
            addr: addr.to_string(),
            pool: Arc::new(Mutex::new(HashMap::new())),
            tls: None,
            active_write_version: version_source,
        };
        let request = openraft::raft::VoteRequest::<TypeConfig>::new(Vote::new(1, 1), None);
        let response = net
            .vote(request, RPCOption::new(Duration::from_secs(5)))
            .await
            .expect("vote round-trips at baseline");
        // The real openraft node answers; we only assert the body decoded
        // (proves: client stamp → server read → server stamp → client read).
        let _ = response;
    }

    #[tokio::test]
    async fn server_rejects_out_of_range_format_version() {
        let (raft, _temp) = test_support::single_node_raft().await;
        let version_source: WriteVersionSource =
            Arc::new(|| tsoracle_openraft_toolkit::BASELINE_WRITE_VERSION);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let service = server(raft, version_source);
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .ok();
        });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let mut client = proto::raft_peer_service_client::RaftPeerServiceClient::connect(format!(
            "http://{addr}"
        ))
        .await
        .unwrap();
        let too_new = u32::from(tsoracle_openraft_toolkit::MAX_READABLE_VERSION) + 1;
        let status = client
            .vote(tonic::Request::new(proto::RaftMessage {
                payload: Vec::new(),
                format_version: too_new,
            }))
            .await
            .expect_err("an out-of-range format_version must be refused");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    // ---- snapshot header framing ----

    #[tokio::test]
    async fn snapshot_header_format_version_is_read_and_range_checked() {
        // BASELINE-framed header reassembles fine.
        let ok_chunks = vec![header_chunk(), data_chunk(b"snap")];
        let assembled = reassemble_snapshot(ok_stream(ok_chunks), 1024)
            .await
            .expect("baseline-framed header assembles");
        assert_eq!(assembled.data, b"snap");

        // An out-of-range header format_version is refused before the
        // vote/meta postcard parse.
        let vote: VoteOf<TypeConfig> = openraft::Vote::new(1, 1);
        let meta = SnapshotMetaOf::<TypeConfig> {
            last_log_id: None,
            last_membership: Default::default(),
            snapshot_id: "bad".to_string(),
        };
        let bad_header = SnapshotChunk {
            kind: Some(ChunkKind::Header(SnapshotHeader {
                vote: postcard::to_stdvec(&vote).unwrap(),
                meta: postcard::to_stdvec(&meta).unwrap(),
                format_version: u32::from(tsoracle_openraft_toolkit::MAX_READABLE_VERSION) + 1,
            })),
        };
        let err = reassemble_snapshot(ok_stream(vec![bad_header]), 1024)
            .await
            .expect_err("out-of-range header rejected");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn snapshot_header_absent_format_version_reads_as_baseline() {
        // A pre-feature sender emits no SnapshotHeader.format_version (proto3
        // default 0). Reassembly must treat it as BASELINE and assemble
        // normally.
        let vote: VoteOf<TypeConfig> = Vote::new(1, 1);
        let meta = SnapshotMetaOf::<TypeConfig> {
            last_log_id: None,
            last_membership: Default::default(),
            snapshot_id: "legacy".to_string(),
        };
        let legacy_header = SnapshotChunk {
            kind: Some(ChunkKind::Header(SnapshotHeader {
                vote: postcard::to_stdvec(&vote).unwrap(),
                meta: postcard::to_stdvec(&meta).unwrap(),
                format_version: 0,
            })),
        };
        let assembled = reassemble_snapshot(ok_stream(vec![legacy_header, data_chunk(b"x")]), 1024)
            .await
            .expect("absent format_version assembles as baseline");
        assert_eq!(assembled.data, b"x");
    }

    // ---- Capabilities RPC ----

    #[test]
    fn capabilities_response_reports_local_node() {
        // The server handler answers Capabilities by encoding `NodeCapabilities::local`
        // of the supplied active write version. The decode→build→encode contract
        // lives in `capabilities_response`, exercised here without a live tonic
        // server.
        let payload = capabilities_response(7);
        let decoded: NodeCapabilities =
            postcard::from_bytes(&payload).expect("decode NodeCapabilities");
        assert_eq!(
            decoded,
            NodeCapabilities {
                min_readable_version: tsoracle_openraft_toolkit::MIN_READABLE_VERSION,
                max_readable_version: tsoracle_openraft_toolkit::MAX_READABLE_VERSION,
                active_write_version: 7,
            }
        );
    }

    #[test]
    fn capabilities_payload_round_trips_client_side() {
        // A client decoding the same payload over the RaftMessage envelope
        // recovers the struct — the wire contract `PeerCapabilitySource` relies
        // on. The envelope's `format_version` is unrelated to this body
        // (NodeCapabilities is the bootstrap message and is not version-framed).
        let server_payload = capabilities_response(4);
        let message = RaftMessage {
            payload: server_payload,
            format_version: 0,
        };
        let decoded: NodeCapabilities =
            postcard::from_bytes(&message.payload).expect("client decode");
        assert_eq!(decoded.active_write_version, 4);
    }

    // Stand up a real RaftPeerService whose `capabilities` handler reports a
    // fixed active write version, then dial it through the `PeerCapabilitySource`
    // adapter and assert the round-trip recovers the reported capabilities. The
    // server uses a `CapStub` rather than building a full `Raft`, so the test
    // proves the adapter + RPC wire contract in isolation.
    #[tokio::test]
    async fn peer_capability_source_round_trips_against_live_server() {
        use proto::raft_peer_service_server::{
            RaftPeerService as ProtoService, RaftPeerServiceServer,
        };
        use tsoracle_driver_openraft::CapabilitySource;

        struct CapStub {
            active_write_version: u8,
        }

        #[tonic::async_trait]
        impl ProtoService for CapStub {
            async fn append_entries(
                &self,
                _: tonic::Request<proto::RaftMessage>,
            ) -> Result<tonic::Response<proto::RaftMessage>, tonic::Status> {
                Err(tonic::Status::unimplemented("cap stub"))
            }
            async fn vote(
                &self,
                _: tonic::Request<proto::RaftMessage>,
            ) -> Result<tonic::Response<proto::RaftMessage>, tonic::Status> {
                Err(tonic::Status::unimplemented("cap stub"))
            }
            async fn snapshot(
                &self,
                _: tonic::Request<tonic::Streaming<proto::SnapshotChunk>>,
            ) -> Result<tonic::Response<proto::RaftMessage>, tonic::Status> {
                Err(tonic::Status::unimplemented("cap stub"))
            }
            async fn transfer_leader(
                &self,
                _: tonic::Request<proto::RaftMessage>,
            ) -> Result<tonic::Response<proto::RaftMessage>, tonic::Status> {
                Err(tonic::Status::unimplemented("cap stub"))
            }
            async fn capabilities(
                &self,
                _: tonic::Request<proto::RaftMessage>,
            ) -> Result<tonic::Response<proto::RaftMessage>, tonic::Status> {
                Ok(tonic::Response::new(proto::RaftMessage {
                    payload: capabilities_response(self.active_write_version),
                    format_version: 0,
                }))
            }
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(RaftPeerServiceServer::new(CapStub {
                    active_write_version: 5,
                }))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .ok();
        });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let source = PeerCapabilitySource::new(None);
        let node = Node {
            addr: addr.to_string(),
            service_endpoint: String::new(),
            admin_endpoint: String::new(),
        };
        let capabilities = source
            .query(2, &node)
            .await
            .expect("live capabilities query");
        assert_eq!(capabilities.active_write_version, 5);
        assert_eq!(
            capabilities.min_readable_version,
            tsoracle_openraft_toolkit::MIN_READABLE_VERSION
        );
        assert_eq!(
            capabilities.max_readable_version,
            tsoracle_openraft_toolkit::MAX_READABLE_VERSION
        );
    }
}
