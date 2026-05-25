//
//  ░▀█▀░█▀▀░█▀█░█▀▄░█▀█░█▀▀░█░░░█▀▀
//  ░░█░░▀▀█░█░█░█▀▄░█▀█░█░░░█░░░█▀▀
//  ░░▀░░▀▀▀░▀▀▀░▀░▀░▀░▀░▀▀▀░▀▀▀░▀▀▀
//
//  tsoracle — Distributed Timestamp Oracle
//
//  Copyright (c) 2026 Prisma Risk
//  Licensed under the Apache License, Version 2.0
//  https://github.com/prisma-risk/tsoracle
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
//! The chunked snapshot path is what makes this example safe to use with a
//! state machine that grows past the default gRPC unary frame limit (4 MiB).
//! See `proto/raft.proto` for the framing rules.

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
use tonic::transport::Channel;

use tsoracle_driver_openraft::{OpenraftPeer as Node, TypeConfig};
type NodeId = u64;

pub mod proto {
    tonic::include_proto!("raft.v1");
}

use proto::RaftMessage;
use proto::SnapshotChunk;
use proto::SnapshotHeader;
use proto::raft_peer_service_client::RaftPeerServiceClient;
use proto::raft_peer_service_server::{RaftPeerService, RaftPeerServiceServer};
use proto::snapshot_chunk::Kind as ChunkKind;

/// Snapshot data is shipped in chunks of this many bytes. Sized to fit
/// comfortably inside the default gRPC max-frame limit (4 MiB) with room
/// for proto overhead and to keep per-RPC memory bounded on both sides.
/// The header chunk is sent separately and is small (a Vote + SnapshotMeta).
pub const SNAPSHOT_CHUNK_SIZE: usize = 1024 * 1024;

/// Upper bound on the *total* bytes the snapshot handler will reassemble from a
/// single client-streaming RPC. The handler refuses (with `ResourceExhausted`)
/// any stream whose cumulative `data` chunks would cross this line, so a peer
/// that can reach the raft port cannot drive the receiver to OOM by sending an
/// endless run of chunks. Sized generously for the example's small high-water
/// state machine; real deployments should size this against the largest
/// realistic state-machine snapshot.
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

// Generic over the value type so the keying/eviction logic is unit-testable
// without constructing a live RaftPeerServiceClient.
async fn evict<V>(pool: &Arc<Mutex<HashMap<(NodeId, String), V>>>, target: NodeId, addr: &str) {
    pool.lock().await.remove(&(target, addr.to_string()));
}

// ---------------------------------------------------------------------------
// PeerFactory — constructs PeerNetwork instances for each target node.
// ---------------------------------------------------------------------------

pub struct PeerFactory {
    pool: Pool,
}

impl PeerFactory {
    pub fn new() -> Self {
        Self {
            pool: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Default for PeerFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl RaftNetworkFactory<TypeConfig> for PeerFactory {
    type Network = PeerNetwork;

    async fn new_client(&mut self, target: NodeId, node: &Node) -> Self::Network {
        PeerNetwork {
            target,
            addr: node.addr.clone(),
            pool: self.pool.clone(),
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
        let url = format!("http://{}", self.addr);
        let client = RaftPeerServiceClient::connect(url)
            .await
            .map_err(|err| RPCError::Unreachable(Unreachable::new(&err)))?;
        self.pool.lock().await.insert(key, client.clone());
        Ok(client)
    }
}

impl RaftNetworkV2<TypeConfig> for PeerNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>> {
        let mut c = self.client().await?;
        let payload =
            postcard::to_stdvec(&rpc).map_err(|err| RPCError::Network(NetworkError::new(&err)))?;
        let resp = match c.append_entries(RaftMessage { payload }).await {
            Ok(resp) => resp,
            Err(err) => {
                evict(&self.pool, self.target, &self.addr).await;
                return Err(RPCError::Network(NetworkError::new(&err)));
            }
        };
        let body: AppendEntriesResponse<TypeConfig> =
            postcard::from_bytes(&resp.into_inner().payload)
                .map_err(|err| RPCError::Network(NetworkError::new(&err)))?;
        Ok(body)
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        let mut c = self.client().await?;
        let payload =
            postcard::to_stdvec(&rpc).map_err(|err| RPCError::Network(NetworkError::new(&err)))?;
        let resp = match c.vote(RaftMessage { payload }).await {
            Ok(resp) => resp,
            Err(err) => {
                evict(&self.pool, self.target, &self.addr).await;
                return Err(RPCError::Network(NetworkError::new(&err)));
            }
        };
        let body: VoteResponse<TypeConfig> = postcard::from_bytes(&resp.into_inner().payload)
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
        _option: RPCOption,
    ) -> Result<(), RPCError<TypeConfig>> {
        let mut c = self.client().await?;
        let payload =
            postcard::to_stdvec(&req).map_err(|err| RPCError::Network(NetworkError::new(&err)))?;
        if let Err(err) = c.transfer_leader(RaftMessage { payload }).await {
            evict(&self.pool, self.target, &self.addr).await;
            return Err(RPCError::Network(NetworkError::new(&err)));
        }
        Ok(())
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
                let resp: SnapshotResponse<TypeConfig> =
                    postcard::from_bytes(&inner.payload)
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
}

#[tonic::async_trait]
impl<SM: Send + Sync + 'static> RaftPeerService for PeerServiceImpl<SM> {
    async fn append_entries(
        &self,
        request: tonic::Request<RaftMessage>,
    ) -> Result<tonic::Response<RaftMessage>, tonic::Status> {
        let body: AppendEntriesRequest<TypeConfig> =
            postcard::from_bytes(&request.into_inner().payload)
                .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;
        let resp = self
            .raft
            .append_entries(body)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;
        let payload =
            postcard::to_stdvec(&resp).map_err(|e| tonic::Status::internal(e.to_string()))?;
        Ok(tonic::Response::new(RaftMessage { payload }))
    }

    async fn vote(
        &self,
        request: tonic::Request<RaftMessage>,
    ) -> Result<tonic::Response<RaftMessage>, tonic::Status> {
        let body: VoteRequest<TypeConfig> = postcard::from_bytes(&request.into_inner().payload)
            .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;
        let resp = self
            .raft
            .vote(body)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;
        let payload =
            postcard::to_stdvec(&resp).map_err(|e| tonic::Status::internal(e.to_string()))?;
        Ok(tonic::Response::new(RaftMessage { payload }))
    }

    async fn transfer_leader(
        &self,
        request: tonic::Request<RaftMessage>,
    ) -> Result<tonic::Response<RaftMessage>, tonic::Status> {
        let body: TransferLeaderRequest<TypeConfig> =
            postcard::from_bytes(&request.into_inner().payload)
                .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;
        self.raft
            .handle_transfer_leader(body)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;
        Ok(tonic::Response::new(RaftMessage {
            payload: Vec::new(),
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
        Ok(tonic::Response::new(RaftMessage { payload }))
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
/// Framing contract (mirrors `proto/raft.proto`):
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

/// Construct the tonic server-side handler for the RaftPeerService.
pub fn server<SM: Send + Sync + 'static>(
    raft: openraft::Raft<TypeConfig, SM>,
) -> RaftPeerServiceServer<PeerServiceImpl<SM>> {
    RaftPeerServiceServer::new(PeerServiceImpl { raft })
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
}
