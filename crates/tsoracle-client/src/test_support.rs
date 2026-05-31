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

//! Shared test-only helpers for the client crate's retry/attempt/leader-hint
//! unit tests. Compiled only under `cfg(test)`.

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tsoracle_proto::v1::tso_service_server::{TsoService, TsoServiceServer};
use tsoracle_proto::v1::{
    GetCurrentMaxSafeRequest, GetCurrentMaxSafeResponse, GetSeqBatchRequest, GetSeqBatchResponse,
    GetSeqRequest, GetSeqResponse, GetTsRequest, GetTsResponse,
};

use crate::RetryPolicy;

/// Install a process-global TRACE subscriber so the retry/attempt code's
/// `tracing::{debug,warn}!` sites evaluate and format their fields under test.
/// Idempotent: `try_init` installs once and returns `Err` (ignored) thereafter.
pub(crate) fn enable_tracing() {
    use tracing_subscriber::filter::LevelFilter;
    let _ = tracing_subscriber::fmt()
        .with_max_level(LevelFilter::TRACE)
        .with_test_writer()
        .try_init();
}

/// Aggressive policy used by the unit tests to keep them fast.
pub(crate) fn short_policy() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 2,
        per_attempt_deadline: Duration::from_millis(100),
        overall_deadline: Duration::from_millis(300),
        base_backoff: Duration::from_millis(1),
        leader_ttl: Duration::from_secs(30),
    }
}

/// Build a `FAILED_PRECONDITION` status with a `LeaderHint` trailer encoded
/// under the same key the server uses, mirroring the production encoding in
/// `crates/tsoracle-server/src/leader_hint.rs`.
pub(crate) fn make_status_with_hint(hint: tsoracle_proto::v1::LeaderHint) -> tonic::Status {
    use prost::Message;
    use tonic::metadata::{BinaryMetadataValue, MetadataKey};
    let mut buf = Vec::new();
    hint.encode(&mut buf)
        .expect("LeaderHint encode is infallible");
    let mut status = tonic::Status::failed_precondition("not leader");
    let key = MetadataKey::from_bytes(tsoracle_proto::v1::LEADER_HINT_TRAILER_KEY.as_bytes())
        .expect("static ASCII key parses");
    status
        .metadata_mut()
        .insert_bin(key, BinaryMetadataValue::from_bytes(&buf));
    status
}

type BoxFut<T> = Pin<Box<dyn Future<Output = Result<T, tonic::Status>> + Send>>;
type TsHandler = Arc<dyn Fn(GetTsRequest) -> BoxFut<GetTsResponse> + Send + Sync>;
type SeqHandler = Arc<dyn Fn(GetSeqRequest) -> BoxFut<GetSeqResponse> + Send + Sync>;
type SeqBatchHandler = Arc<dyn Fn(GetSeqBatchRequest) -> BoxFut<GetSeqBatchResponse> + Send + Sync>;

/// A configurable fake [`TsoService`] for client tests. Per-call behavior is
/// injected as closures, so this single trait impl — one `get_ts`, one
/// `get_seq`, one `get_seq_batch` — replaces the many bespoke per-test server
/// structs. The shared handlers earn real coverage from the tests that configure
/// them, instead of leaving unreachable `unimplemented!()` stubs in every test
/// that never calls a given RPC.
///
/// All handlers default to returning `UNIMPLEMENTED`; a test overrides only the
/// RPC it exercises via [`on_get_ts`](Self::on_get_ts) /
/// [`on_get_seq`](Self::on_get_seq) / [`on_get_seq_batch`](Self::on_get_seq_batch).
/// `get_current_max_safe` returns the default response (every test that touches
/// it wants exactly that).
pub(crate) struct FakeTso {
    get_ts: TsHandler,
    get_seq: SeqHandler,
    get_seq_batch: SeqBatchHandler,
}

impl FakeTso {
    pub(crate) fn new() -> Self {
        FakeTso {
            get_ts: Arc::new(|_| {
                Box::pin(async { Err(tonic::Status::unimplemented("get_ts not configured")) })
            }),
            get_seq: Arc::new(|_| {
                Box::pin(async { Err(tonic::Status::unimplemented("get_seq not configured")) })
            }),
            get_seq_batch: Arc::new(|_| {
                Box::pin(async {
                    Err(tonic::Status::unimplemented("get_seq_batch not configured"))
                })
            }),
        }
    }

    /// Override the `get_ts` behavior with an async closure of the request.
    pub(crate) fn on_get_ts<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(GetTsRequest) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<GetTsResponse, tonic::Status>> + Send + 'static,
    {
        self.get_ts = Arc::new(move |req| Box::pin(f(req)));
        self
    }

    /// Override the `get_seq` behavior with an async closure of the request.
    pub(crate) fn on_get_seq<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(GetSeqRequest) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<GetSeqResponse, tonic::Status>> + Send + 'static,
    {
        self.get_seq = Arc::new(move |req| Box::pin(f(req)));
        self
    }

    /// Override the `get_seq_batch` behavior with an async closure of the request.
    pub(crate) fn on_get_seq_batch<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(GetSeqBatchRequest) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<GetSeqBatchResponse, tonic::Status>> + Send + 'static,
    {
        self.get_seq_batch = Arc::new(move |req| Box::pin(f(req)));
        self
    }

    /// Bind a fresh loopback listener, serve this fake on a background task, and
    /// return the bound address. The spawned task is detached; it ends when the
    /// test's runtime is torn down.
    pub(crate) async fn spawn(self) -> SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback listener");
        let addr = listener.local_addr().expect("local_addr");
        let incoming = tonic::transport::server::TcpIncoming::from(listener);
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(TsoServiceServer::new(self))
                .serve_with_incoming(incoming)
                .await
                .ok();
        });
        addr
    }
}

#[tonic::async_trait]
impl TsoService for FakeTso {
    async fn get_ts(
        &self,
        request: tonic::Request<GetTsRequest>,
    ) -> Result<tonic::Response<GetTsResponse>, tonic::Status> {
        (self.get_ts)(request.into_inner())
            .await
            .map(tonic::Response::new)
    }

    async fn get_current_max_safe(
        &self,
        _request: tonic::Request<GetCurrentMaxSafeRequest>,
    ) -> Result<tonic::Response<GetCurrentMaxSafeResponse>, tonic::Status> {
        Ok(tonic::Response::new(GetCurrentMaxSafeResponse::default()))
    }

    async fn get_seq(
        &self,
        request: tonic::Request<GetSeqRequest>,
    ) -> Result<tonic::Response<GetSeqResponse>, tonic::Status> {
        (self.get_seq)(request.into_inner())
            .await
            .map(tonic::Response::new)
    }

    async fn get_seq_batch(
        &self,
        request: tonic::Request<GetSeqBatchRequest>,
    ) -> Result<tonic::Response<GetSeqBatchResponse>, tonic::Status> {
        (self.get_seq_batch)(request.into_inner())
            .await
            .map(tonic::Response::new)
    }
}
