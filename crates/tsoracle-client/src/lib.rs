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

#![doc = include_str!("../README.md")]
// Panic policy (see CONTRIBUTING.md). `cfg_attr(not(test), ...)` skips the lint
// for the lib's own unit tests; integration tests are separate compilation units.
#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used))]

mod attempt;
mod budget;
mod channel_pool;
mod driver;
mod driver_supervisor;
mod error;
mod leader_hint;
mod response;
mod retry;
mod retry_policy;
mod seq_attempt;
mod transport;
mod worklist;

#[cfg(test)]
mod test_support;

pub use error::ClientError;
pub use retry_policy::RetryPolicy;
pub use seq_attempt::SeqBlock;
pub use transport::BoxError;

use std::sync::Arc;
use std::time::Duration;
use tsoracle_core::{Epoch, LOGICAL_MAX, Timestamp};

/// The server's per-call cap on requested timestamps, fixed by the 18-bit
/// logical width. Callers asking for more than this can't be served by any
/// single RPC; the client rejects them up-front rather than burning a queue
/// slot and round-trip to learn the same thing from the server.
pub(crate) const MAX_TIMESTAMPS_PER_RPC: u32 = LOGICAL_MAX + 1;

use crate::channel_pool::ChannelPool;

pub struct ClientBuilder {
    endpoints: Vec<String>,
    flush_interval: Duration,
    connector: Option<Arc<crate::transport::ChannelConnector>>,
    tls_required: bool,
    retry_policy: RetryPolicy,
}

impl ClientBuilder {
    pub fn endpoints(endpoints: Vec<String>) -> Self {
        ClientBuilder {
            endpoints,
            flush_interval: Duration::from_millis(1),
            connector: None,
            tls_required: false,
            retry_policy: RetryPolicy::default(),
        }
    }

    pub fn batch_flush_interval(mut self, flush_interval: Duration) -> Self {
        self.flush_interval = flush_interval;
        self
    }

    /// Override the default [`RetryPolicy`].
    ///
    /// The policy controls per-attempt deadlines, the overall deadline
    /// across all candidate endpoints, the cap on attempts, and the
    /// jittered backoff base. The per-attempt deadline is also pushed
    /// down to `tonic::transport::Endpoint::connect_timeout` and
    /// `Endpoint::timeout` for the built-in default and TLS transport
    /// paths so a blackholed peer fails fast at the transport layer.
    /// User-supplied [`Self::channel_connector`] closures own their
    /// own Endpoint config; the policy still bounds the retry loop's
    /// outer `tokio::time::timeout` around them.
    pub fn retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = policy;
        self
    }

    /// Configure the client to dial bare endpoints with TLS. Bare `host:port`
    /// becomes `https://host:port`; explicit `http://...` endpoints supplied
    /// in [`Self::endpoints`] remain plaintext; explicit `https://...`
    /// endpoints use the provided TLS config.
    ///
    /// Wire-supplied `http://...` leader-hint trailers are NOT honored under
    /// `tls_config` — they are dropped to prevent a contacted peer from
    /// downgrading the transport. Operator-supplied configuration still wins;
    /// untrusted wire input does not.
    ///
    /// Setting both [`Self::channel_connector`] and `tls_config` is allowed;
    /// the last call wins (standard builder semantics). Calling
    /// `channel_connector` after `tls_config` also clears the
    /// reject-plaintext-hint policy, since the caller-owned connector owns
    /// its own scheme policy.
    #[cfg(any(feature = "tls-rustls", feature = "tls-native"))]
    pub fn tls_config(mut self, cfg: tonic::transport::ClientTlsConfig) -> Self {
        self.connector = Some(crate::transport::tls_connector(
            cfg,
            self.retry_policy.clone(),
        ));
        self.tls_required = true;
        self
    }

    /// Replace the default plaintext channel construction with a caller-owned
    /// closure. The closure is invoked on first use of each endpoint —
    /// configured endpoints and leader-hint redirects alike. Errors returned
    /// from the closure surface as [`ClientError::Connector`].
    ///
    /// See module docs for the interaction with [`Self::tls_config`]
    /// (last-wins) and the scheme matrix. A caller-owned connector replaces
    /// the built-in TLS plumbing entirely, including the
    /// reject-plaintext-leader-hint policy — the closure is responsible for
    /// whatever scheme policy it wants to enforce.
    pub fn channel_connector<F, Fut>(mut self, connector: F) -> Self
    where
        F: Fn(&str) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<tonic::transport::Channel, crate::BoxError>>
            + Send
            + 'static,
    {
        let wrapped: Arc<crate::transport::ChannelConnector> = Arc::new(move |endpoint: &str| {
            let fut = connector(endpoint);
            Box::pin(async move { fut.await.map_err(ClientError::Connector) })
        });
        self.connector = Some(wrapped);
        self.tls_required = false;
        self
    }

    pub async fn build(self) -> Result<Client, ClientError> {
        if self.endpoints.is_empty() {
            return Err(ClientError::NoReachableEndpoints);
        }
        let pool = Arc::new(ChannelPool::new(
            self.endpoints,
            self.connector,
            self.tls_required,
            self.retry_policy,
        ));
        let pool_for_rpc = pool.clone();
        let driver = driver::Driver::spawn(
            move |count| {
                let pool = pool_for_rpc.clone();
                Box::pin(async move { retry::issue_rpc(&pool, count).await })
            },
            self.flush_interval,
        );
        Ok(Client { pool, driver })
    }
}

pub struct Client {
    pool: Arc<ChannelPool>,
    driver: driver::Driver,
}

impl Client {
    pub async fn connect(endpoints: Vec<String>) -> Result<Self, ClientError> {
        ClientBuilder::endpoints(endpoints).build().await
    }

    /// The endpoint the client currently believes is the leader, or `None`
    /// if no leader has been observed yet or the cached entry has aged past
    /// the configured `leader_ttl`.
    ///
    /// Read-only diagnostic surface for ops dashboards and integration tests
    /// asserting that a client has converged to the expected leader. It
    /// reflects the cache as last updated by a completed RPC — it neither
    /// triggers nor waits on any network round-trip, and the TTL check is
    /// lazy (an expired entry reads as `None`).
    pub fn cached_leader(&self) -> Option<String> {
        self.pool.cached_leader()
    }

    pub async fn get_ts(&self) -> Result<Timestamp, ClientError> {
        Ok(self.driver.request(1).await?[0])
    }

    pub async fn get_ts_batch(&self, count: u32) -> Result<Vec<Timestamp>, ClientError> {
        if count == 0 || count > MAX_TIMESTAMPS_PER_RPC {
            return Err(ClientError::InvalidCount(count));
        }
        self.driver.request(count).await
    }

    /// Request a contiguous block of `count` dense ordinals for the given
    /// sequence `key`.
    ///
    /// **Non-idempotent:** if this call returns `ClientError::SeqUncertain` it
    /// means a commit may or may not have occurred on the server — do NOT
    /// silently retry, as that would risk spending a second block. The caller
    /// must resolve the ambiguity (e.g. by reading back the current counter
    /// with a coordinator-level read) before re-issuing.
    ///
    /// All other errors are pre-commit-certain (the server rejected the request
    /// before any durable advance) and are safe to retry.
    pub async fn get_seq(&self, key: &str, count: u32) -> Result<SeqBlock, ClientError> {
        if key.is_empty() || key.len() > tsoracle_core::MAX_SEQ_KEY_LEN {
            return Err(ClientError::InvalidSeqKey);
        }
        if count == 0 || count > tsoracle_core::MAX_SEQ_COUNT {
            return Err(ClientError::InvalidCount(count));
        }
        retry::issue_seq_rpc(&self.pool, key, count).await
    }

    /// Read the leader's current safe-point in physical-millisecond units.
    ///
    /// Targets the cached leader if known, otherwise the first configured
    /// endpoint. Followers return `MaxSafe::max_safe_physical_ms == 0` rather
    /// than erroring, matching the proto contract; pollers needing freshness
    /// should target the leader endpoint.
    ///
    /// Single-shot by design — the proto contract is "followers return 0"
    /// rather than NOT_LEADER, so there is no hint to chase and no worklist
    /// to drain; a caller polling for freshness retries the next tick rather
    /// than the next endpoint. The one `(connect, RPC)` pair is bounded by
    /// [`RetryPolicy::per_attempt_deadline`] (shared across both phases via
    /// `PairBudget`, exactly like one `get_ts` attempt), and a transport-class
    /// failure evicts the cached channel so a half-open / black-holing
    /// connection is dropped before the next poll lands on it.
    pub async fn get_current_max_safe(&self) -> Result<MaxSafe, ClientError> {
        let endpoint = self
            .pool
            .cached_leader()
            .or_else(|| self.pool.iter_round_robin().into_iter().next())
            .ok_or(ClientError::NoReachableEndpoints)?;
        let budget = self.pool.retry_policy().per_attempt_deadline;
        // One budget shared across connect + RPC: a slow connect eats into
        // the RPC's time rather than each phase getting a fresh full budget,
        // so a single call never runs longer than ~`per_attempt_deadline`.
        let pair = crate::budget::PairBudget::start(budget);
        let (mut svc, cell) =
            match tokio::time::timeout(budget, self.pool.client_with_cell(&endpoint)).await {
                Ok(Ok(leased)) => leased,
                // `client_with_cell` already evicts its own cell on a failed
                // dial, so there is nothing to evict here.
                Ok(Err(err)) => return Err(err),
                Err(_) => {
                    return Err(ClientError::Rpc(tonic::Status::deadline_exceeded(format!(
                        "connect exceeded per_attempt_deadline of {budget:?}"
                    ))));
                }
            };
        let rpc_budget = pair.remaining();
        let rpc = svc.get_current_max_safe(tsoracle_proto::v1::GetCurrentMaxSafeRequest {});
        let err = match tokio::time::timeout(rpc_budget, rpc).await {
            Ok(Ok(response)) => {
                let inner = response.into_inner();
                return Ok(MaxSafe {
                    max_safe_physical_ms: inner.max_safe_physical_ms,
                    epoch: Epoch::from_wire(inner.epoch_hi, inner.epoch_lo),
                });
            }
            Ok(Err(status)) => ClientError::Rpc(status),
            // A timed-out RPC surfaces as `DeadlineExceeded` (transport-class
            // per `is_transport_failure`), so the eviction tail below drops the
            // possibly-half-open channel — matching the `get_ts` attempt path.
            Err(_) => ClientError::Rpc(tonic::Status::deadline_exceeded(format!(
                "rpc exceeded its share of per_attempt_deadline \
                 ({rpc_budget:?} of {budget:?})"
            ))),
        };
        if crate::retry_policy::is_transport_failure(&err) {
            self.pool.evict_if_current(&endpoint, &cell);
        }
        Err(err)
    }
}

/// The leader's view of the durable safe-point, returned by
/// [`Client::get_current_max_safe`].
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct MaxSafe {
    /// Safe-point in physical-millisecond units; `0` is the cold-start sentinel
    /// (also the value any follower returns).
    pub max_safe_physical_ms: u64,
    /// Leader epoch that issued this view.
    pub epoch: Epoch,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cached_leader_is_none_before_any_rpc() {
        // A freshly built client has issued no RPC, so the channel pool's
        // leader cache is empty and `cached_leader()` reports `None`. This
        // pins the "nothing observed yet" branch of the diagnostic accessor
        // without needing a server; the post-RPC `Some(addr)` case is covered
        // end-to-end in `tsoracle-tests`.
        let client = Client::connect(vec!["http://127.0.0.1:1".into()])
            .await
            .expect("build with a non-empty endpoint list must succeed");
        assert_eq!(client.cached_leader(), None);
    }

    #[tokio::test]
    async fn build_rejects_empty_endpoint_list() {
        // Validation prevents a Client whose `pool` has no endpoints to try;
        // every RPC would fail-fast with `NoReachableEndpoints` and burn no
        // network roundtrips at all, so reject up-front instead.
        match ClientBuilder::endpoints(Vec::new()).build().await {
            Err(ClientError::NoReachableEndpoints) => {}
            Err(other) => panic!("expected NoReachableEndpoints, got {other:?}"),
            Ok(_) => panic!("expected Err, got Ok(Client)"),
        }
    }

    #[tokio::test]
    async fn channel_connector_error_surfaces_as_connector_variant() {
        let builder = ClientBuilder::endpoints(vec!["a:1".into()]).channel_connector(
            |_endpoint: &str| async move {
                Err::<tonic::transport::Channel, crate::BoxError>(
                    std::io::Error::other("boom").into(),
                )
            },
        );
        let client = builder.build().await.expect("build must not fail");
        let result = client.get_ts().await;
        match result {
            Err(ClientError::Connector(inner)) => {
                assert!(inner.to_string().contains("boom"));
            }
            other => panic!("expected ClientError::Connector, got {other:?}"),
        }
    }

    // Marker payload used by both last-wins tests. The free function holds
    // the body in one source location so coverage credit flows from the test
    // where the closure DOES run; the test that asserts "this never runs"
    // just calls the same helper.
    #[cfg(any(feature = "tls-rustls", feature = "tls-native"))]
    async fn marker_connector_failure() -> Result<tonic::transport::Channel, crate::BoxError> {
        Err("MARKER".into())
    }

    #[cfg(any(feature = "tls-rustls", feature = "tls-native"))]
    #[tokio::test]
    async fn tls_config_then_channel_connector_last_wins() {
        // channel_connector is set LAST, so its path runs on get_ts. The
        // marker error surfaces as `ClientError::Connector`, proving the
        // builder did not silently keep the prior tls_config.
        let builder = ClientBuilder::endpoints(vec!["a:1".into()])
            .tls_config(tonic::transport::ClientTlsConfig::new())
            .channel_connector(|_endpoint: &str| marker_connector_failure());
        let client = builder.build().await.expect("build must not fail");
        match client.get_ts().await {
            Err(ClientError::Connector(inner)) => {
                assert!(inner.to_string().contains("MARKER"));
            }
            other => panic!("expected Connector(MARKER), got {other:?}"),
        }
    }

    #[cfg(any(feature = "tls-rustls", feature = "tls-native"))]
    #[tokio::test]
    async fn channel_connector_then_tls_config_last_wins() {
        // tls_config is set LAST, so the connector path is replaced and its
        // marker error must NOT surface. The tls_config path produces a
        // transport-level failure (or NoReachableEndpoints / Rpc) instead.
        let builder = ClientBuilder::endpoints(vec!["a:1".into()])
            .channel_connector(|_endpoint: &str| marker_connector_failure())
            .tls_config(tonic::transport::ClientTlsConfig::new());
        let client = builder.build().await.expect("build must not fail");
        let result = client.get_ts().await;
        if let Err(ClientError::Connector(inner)) = &result
            && inner.to_string().contains("MARKER")
        {
            panic!("tls_config set last must overwrite the prior channel_connector");
        }
    }

    #[tokio::test]
    async fn batch_flush_interval_overrides_default() {
        // The builder's `batch_flush_interval` knob feeds the driver's
        // coalescing window; without a test it could silently revert to the
        // default and no-one would notice from black-box behavior. We
        // confirm the override path by reaching into the builder fields.
        let custom = Duration::from_millis(25);
        let builder = ClientBuilder::endpoints(vec!["http://127.0.0.1:1".into()])
            .batch_flush_interval(custom);
        assert_eq!(builder.flush_interval, custom);
    }

    #[tokio::test]
    async fn retry_policy_override_propagates_to_builder() {
        // The builder field is what `build` hands to the pool and retry
        // loop. If `retry_policy()` ever silently stops storing the
        // override, the loop reverts to defaults; this test pins the
        // override path against that.
        let policy = RetryPolicy {
            max_attempts: 7,
            per_attempt_deadline: Duration::from_millis(11),
            overall_deadline: Duration::from_millis(13),
            base_backoff: Duration::from_millis(17),
            leader_ttl: Duration::from_millis(19),
        };
        let builder = ClientBuilder::endpoints(vec!["http://127.0.0.1:1".into()])
            .retry_policy(policy.clone());
        assert_eq!(builder.retry_policy.max_attempts, policy.max_attempts);
        assert_eq!(
            builder.retry_policy.per_attempt_deadline,
            policy.per_attempt_deadline
        );
        assert_eq!(
            builder.retry_policy.overall_deadline,
            policy.overall_deadline
        );
        assert_eq!(builder.retry_policy.base_backoff, policy.base_backoff);
        assert_eq!(builder.retry_policy.leader_ttl, policy.leader_ttl);
    }

    /// Acceptance criterion for the `get_current_max_safe` deadline fix
    /// (security finding 8c3ea943): a single-shot call against a
    /// black-holed endpoint must surface `DeadlineExceeded` within the
    /// configured `per_attempt_deadline`, not park indefinitely on a
    /// connector whose future never resolves. Exercised through the
    /// public `channel_connector` surface — `std::future::pending()`
    /// guarantees the connect future never completes, so any code path
    /// without an outer `tokio::time::timeout` hangs until an external
    /// test timeout kills the runtime. The wall-clock bound is generous
    /// enough to absorb CI scheduler jitter but well below the OS-level
    /// TCP timeout, mirroring the structure of the `get_ts`
    /// equivalent below.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn get_current_max_safe_returns_within_per_attempt_deadline_when_connector_hangs() {
        let policy = RetryPolicy {
            max_attempts: 1,
            per_attempt_deadline: Duration::from_millis(100),
            overall_deadline: Duration::from_millis(300),
            base_backoff: Duration::ZERO,
            leader_ttl: Duration::from_secs(30),
        };
        let client = ClientBuilder::endpoints(vec!["hang:1".into()])
            .channel_connector(|_endpoint: &str| async {
                // The future never resolves; only an outer timeout can end
                // the await. This is the exact attack shape the finding
                // describes: a user-supplied connector (or a black-holed
                // peer behind one) for which the caller relied on
                // `RetryPolicy` to bound the wait.
                std::future::pending::<Result<tonic::transport::Channel, crate::BoxError>>().await
            })
            .retry_policy(policy)
            .build()
            .await
            .expect("builder must accept the policy");
        // Outer safety timeout: when the fix is missing, `get_current_max_safe`
        // awaits a never-resolving connector indefinitely, which would hang the
        // whole test runner rather than fail this case. The 5s ceiling converts
        // that hang into a clean panic with a diagnostic message. Under a
        // correctly-deadlined call this outer guard never fires — the inner
        // future returns in ~100ms, well below the elapsed-time assertion.
        let outer_safety = Duration::from_secs(5);
        let start = std::time::Instant::now();
        let result = match tokio::time::timeout(outer_safety, client.get_current_max_safe()).await {
            Ok(r) => r,
            Err(_) => panic!(
                "get_current_max_safe failed to honor its own per_attempt_deadline; \
                 the {outer_safety:?} outer safety net had to fire — this is the \
                 security finding's exact symptom (channel acquisition or RPC was \
                 never bounded)",
            ),
        };
        let elapsed = start.elapsed();
        assert!(
            result.is_err(),
            "a hanging connector must surface as Err, got {result:?}",
        );
        assert!(
            elapsed < Duration::from_secs(2),
            "deadline must short-circuit; took {elapsed:?} (per_attempt_deadline was 100ms)",
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn get_ts_returns_within_overall_deadline_when_all_endpoints_unreachable() {
        // End-to-end test of the issue's acceptance criterion: with no
        // listener bound at the configured endpoints, a `get_ts` call
        // must return well before the OS-default TCP timeout (~75 s on
        // Linux). The bound here is generous enough to absorb CI
        // scheduler jitter — the assertion is "fast", not "exactly the
        // configured deadline".
        let policy = RetryPolicy {
            max_attempts: 3,
            per_attempt_deadline: Duration::from_millis(100),
            overall_deadline: Duration::from_millis(300),
            base_backoff: Duration::ZERO,
            leader_ttl: Duration::from_secs(30),
        };
        let client = ClientBuilder::endpoints(vec![
            "http://127.0.0.1:1".into(),
            "http://127.0.0.1:2".into(),
            "http://127.0.0.1:3".into(),
        ])
        .retry_policy(policy)
        .build()
        .await
        .expect("builder must accept the policy");
        let start = std::time::Instant::now();
        let result = client.get_ts().await;
        let elapsed = start.elapsed();
        assert!(result.is_err(), "no listener can reply: {result:?}");
        assert!(
            elapsed < Duration::from_secs(2),
            "deadline must short-circuit; took {elapsed:?}"
        );
    }

    /// Integration test: `get_seq` against a real loopback gRPC server that
    /// simulates a file-driver leader. The server uses a fake `TsoService`
    /// with a real `get_seq` implementation (incrementing an atomic counter).
    /// Asserts: consecutive blocks are gapless, the epoch is propagated, and
    /// invalid inputs are rejected up-front.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn get_seq_returns_gapless_blocks_and_rejects_invalid_inputs() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU64, Ordering};
        use tsoracle_proto::v1::tso_service_server::{TsoService, TsoServiceServer};

        const EPOCH: u128 = 42;

        struct LeaderServer {
            counter: Arc<AtomicU64>,
        }

        #[tonic::async_trait]
        impl TsoService for LeaderServer {
            async fn get_ts(
                &self,
                _request: tonic::Request<tsoracle_proto::v1::GetTsRequest>,
            ) -> Result<tonic::Response<tsoracle_proto::v1::GetTsResponse>, tonic::Status>
            {
                Err(tonic::Status::failed_precondition("not leader"))
            }

            async fn get_current_max_safe(
                &self,
                _request: tonic::Request<tsoracle_proto::v1::GetCurrentMaxSafeRequest>,
            ) -> Result<tonic::Response<tsoracle_proto::v1::GetCurrentMaxSafeResponse>, tonic::Status>
            {
                Ok(tonic::Response::new(
                    tsoracle_proto::v1::GetCurrentMaxSafeResponse::default(),
                ))
            }

            async fn get_seq(
                &self,
                request: tonic::Request<tsoracle_proto::v1::GetSeqRequest>,
            ) -> Result<tonic::Response<tsoracle_proto::v1::GetSeqResponse>, tonic::Status>
            {
                let req = request.into_inner();
                if req.key.is_empty() {
                    return Err(tonic::Status::invalid_argument("invalid sequence key"));
                }
                if req.count == 0 {
                    return Err(tonic::Status::invalid_argument(
                        "count must be between 1 and the maximum",
                    ));
                }
                // Atomic fetch-add: gapless per the single server
                let start = self
                    .counter
                    .fetch_add(u64::from(req.count), Ordering::SeqCst);
                let (hi, lo) = tsoracle_core::Epoch(EPOCH).to_wire();
                Ok(tonic::Response::new(tsoracle_proto::v1::GetSeqResponse {
                    key: req.key,
                    start,
                    count: req.count,
                    epoch: Some(tsoracle_proto::v1::EpochWire { hi, lo }),
                }))
            }
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback listener");
        let addr = listener.local_addr().expect("local_addr");
        let incoming = tonic::transport::server::TcpIncoming::from(listener);
        let counter = Arc::new(AtomicU64::new(0));
        let server_counter = counter.clone();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(TsoServiceServer::new(LeaderServer {
                    counter: server_counter,
                }))
                .serve_with_incoming(incoming)
                .await
                .ok();
        });

        let endpoint = format!("http://{addr}");
        let client = ClientBuilder::endpoints(vec![endpoint])
            .retry_policy(RetryPolicy {
                max_attempts: 3,
                per_attempt_deadline: Duration::from_secs(2),
                overall_deadline: Duration::from_secs(5),
                base_backoff: Duration::ZERO,
                leader_ttl: Duration::from_secs(30),
            })
            .build()
            .await
            .expect("client must build");

        // First call: block [0, 5).
        let block1 = client
            .get_seq("orders", 5)
            .await
            .expect("first get_seq must succeed");
        assert_eq!(block1.start, 0, "first block must start at 0");
        assert_eq!(block1.count, 5, "first block must have count 5");
        assert_eq!(block1.epoch, EPOCH, "epoch must match the server's epoch");

        // Second call: block [5, 8) — gapless continuation.
        let block2 = client
            .get_seq("orders", 3)
            .await
            .expect("second get_seq must succeed");
        assert_eq!(block2.start, 5, "second block must start at 5 (gapless)");
        assert_eq!(block2.count, 3);
        assert_eq!(block2.epoch, EPOCH);

        // Empty key is rejected up-front by the client, not the server.
        match client.get_seq("", 1).await {
            Err(ClientError::InvalidSeqKey) => {}
            other => panic!("empty key must return InvalidSeqKey, got {other:?}"),
        }

        // Zero count is rejected up-front by the client.
        match client.get_seq("orders", 0).await {
            Err(ClientError::InvalidCount(0)) => {}
            other => panic!("count=0 must return InvalidCount, got {other:?}"),
        }
    }

    /// Regression guard for the dense classification bug: a bare
    /// `FailedPrecondition` from the server (NO leader-hint trailer) — e.g.
    /// `SeqOverflow` — must surface immediately as that definitive error,
    /// preserving the server's message, NOT be misread as a NOT_LEADER
    /// "no leader yet" and ridden out across passes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn get_seq_bare_failed_precondition_surfaces_definitive_error() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU64, Ordering};
        use tsoracle_proto::v1::tso_service_server::{TsoService, TsoServiceServer};

        struct OverflowServer {
            calls: Arc<AtomicU64>,
        }

        #[tonic::async_trait]
        impl TsoService for OverflowServer {
            async fn get_ts(
                &self,
                _request: tonic::Request<tsoracle_proto::v1::GetTsRequest>,
            ) -> Result<tonic::Response<tsoracle_proto::v1::GetTsResponse>, tonic::Status>
            {
                Err(tonic::Status::failed_precondition("not leader"))
            }
            async fn get_current_max_safe(
                &self,
                _request: tonic::Request<tsoracle_proto::v1::GetCurrentMaxSafeRequest>,
            ) -> Result<tonic::Response<tsoracle_proto::v1::GetCurrentMaxSafeResponse>, tonic::Status>
            {
                Ok(tonic::Response::new(
                    tsoracle_proto::v1::GetCurrentMaxSafeResponse::default(),
                ))
            }
            async fn get_seq(
                &self,
                _request: tonic::Request<tsoracle_proto::v1::GetSeqRequest>,
            ) -> Result<tonic::Response<tsoracle_proto::v1::GetSeqResponse>, tonic::Status>
            {
                self.calls.fetch_add(1, Ordering::SeqCst);
                // Bare FailedPrecondition, NO leader-hint trailer — mirrors the
                // server's SeqOverflow mapping.
                Err(tonic::Status::failed_precondition("dense counter overflow"))
            }
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let incoming = tonic::transport::server::TcpIncoming::from(listener);
        let calls = Arc::new(AtomicU64::new(0));
        let server_calls = calls.clone();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(TsoServiceServer::new(OverflowServer {
                    calls: server_calls,
                }))
                .serve_with_incoming(incoming)
                .await
                .ok();
        });

        let client = ClientBuilder::endpoints(vec![format!("http://{addr}")])
            .retry_policy(RetryPolicy {
                max_attempts: 3,
                per_attempt_deadline: Duration::from_secs(2),
                overall_deadline: Duration::from_secs(5),
                base_backoff: Duration::ZERO,
                leader_ttl: Duration::from_secs(30),
            })
            .build()
            .await
            .unwrap();

        match client.get_seq("orders", 1).await {
            Err(ClientError::Rpc(status)) => {
                assert_eq!(status.code(), tonic::Code::FailedPrecondition);
                assert!(
                    status.message().contains("overflow"),
                    "must surface the server's overflow message, not a synthetic \
                     'no leader yet'; got {:?}",
                    status.message()
                );
            }
            other => panic!("expected the overflow FailedPrecondition, got {other:?}"),
        }
        // Definitive-error path: bounded attempts, never an unbounded ride-out.
        let n = calls.load(Ordering::SeqCst);
        assert!((1..=3).contains(&n), "expected bounded attempts, got {n}");
    }

    /// The non-idempotency contract: a post-send transport failure
    /// (`Unavailable` after the request reached the server) is ambiguous — the
    /// advance may have committed — so `get_seq` returns `SeqUncertain` and must
    /// NOT transparently retry (a retry could silently spend a second block).
    /// Asserts the server is invoked exactly once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn get_seq_ambiguous_post_send_failure_is_uncertain_without_retry() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU64, Ordering};
        use tsoracle_proto::v1::tso_service_server::{TsoService, TsoServiceServer};

        struct FlakyServer {
            calls: Arc<AtomicU64>,
        }

        #[tonic::async_trait]
        impl TsoService for FlakyServer {
            async fn get_ts(
                &self,
                _request: tonic::Request<tsoracle_proto::v1::GetTsRequest>,
            ) -> Result<tonic::Response<tsoracle_proto::v1::GetTsResponse>, tonic::Status>
            {
                Err(tonic::Status::failed_precondition("not leader"))
            }
            async fn get_current_max_safe(
                &self,
                _request: tonic::Request<tsoracle_proto::v1::GetCurrentMaxSafeRequest>,
            ) -> Result<tonic::Response<tsoracle_proto::v1::GetCurrentMaxSafeResponse>, tonic::Status>
            {
                Ok(tonic::Response::new(
                    tsoracle_proto::v1::GetCurrentMaxSafeResponse::default(),
                ))
            }
            async fn get_seq(
                &self,
                _request: tonic::Request<tsoracle_proto::v1::GetSeqRequest>,
            ) -> Result<tonic::Response<tsoracle_proto::v1::GetSeqResponse>, tonic::Status>
            {
                self.calls.fetch_add(1, Ordering::SeqCst);
                Err(tonic::Status::unavailable("connection reset mid-call"))
            }
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let incoming = tonic::transport::server::TcpIncoming::from(listener);
        let calls = Arc::new(AtomicU64::new(0));
        let server_calls = calls.clone();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(TsoServiceServer::new(FlakyServer {
                    calls: server_calls,
                }))
                .serve_with_incoming(incoming)
                .await
                .ok();
        });

        let client = ClientBuilder::endpoints(vec![format!("http://{addr}")])
            .retry_policy(RetryPolicy {
                max_attempts: 3,
                per_attempt_deadline: Duration::from_secs(2),
                overall_deadline: Duration::from_secs(5),
                base_backoff: Duration::ZERO,
                leader_ttl: Duration::from_secs(30),
            })
            .build()
            .await
            .unwrap();

        match client.get_seq("orders", 1).await {
            Err(ClientError::SeqUncertain) => {}
            other => panic!("post-send Unavailable must be SeqUncertain, got {other:?}"),
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "non-idempotent get_seq must NOT retry an ambiguous post-send failure"
        );
    }

    /// The non-idempotency contract extended to application faults: a post-send
    /// `INTERNAL` (the server's mapping for a permanent dense driver fault — the
    /// file driver emits this for a directory-fsync failure that happens *after*
    /// the durable rename) is ambiguous, not pre-commit-certain. `get_seq` must
    /// return `SeqUncertain` and invoke the server exactly once, never surfacing
    /// it as a definitive (caller-retry-safe) error.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn get_seq_post_send_internal_is_uncertain_without_retry() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU64, Ordering};
        use tsoracle_proto::v1::tso_service_server::{TsoService, TsoServiceServer};

        struct InternalServer {
            calls: Arc<AtomicU64>,
        }

        #[tonic::async_trait]
        impl TsoService for InternalServer {
            async fn get_ts(
                &self,
                _request: tonic::Request<tsoracle_proto::v1::GetTsRequest>,
            ) -> Result<tonic::Response<tsoracle_proto::v1::GetTsResponse>, tonic::Status>
            {
                Err(tonic::Status::failed_precondition("not leader"))
            }
            async fn get_current_max_safe(
                &self,
                _request: tonic::Request<tsoracle_proto::v1::GetCurrentMaxSafeRequest>,
            ) -> Result<tonic::Response<tsoracle_proto::v1::GetCurrentMaxSafeResponse>, tonic::Status>
            {
                Ok(tonic::Response::new(
                    tsoracle_proto::v1::GetCurrentMaxSafeResponse::default(),
                ))
            }
            async fn get_seq(
                &self,
                _request: tonic::Request<tsoracle_proto::v1::GetSeqRequest>,
            ) -> Result<tonic::Response<tsoracle_proto::v1::GetSeqResponse>, tonic::Status>
            {
                self.calls.fetch_add(1, Ordering::SeqCst);
                Err(tonic::Status::internal("permanent dense driver fault"))
            }
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let incoming = tonic::transport::server::TcpIncoming::from(listener);
        let calls = Arc::new(AtomicU64::new(0));
        let server_calls = calls.clone();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(TsoServiceServer::new(InternalServer {
                    calls: server_calls,
                }))
                .serve_with_incoming(incoming)
                .await
                .ok();
        });

        let client = ClientBuilder::endpoints(vec![format!("http://{addr}")])
            .retry_policy(RetryPolicy {
                max_attempts: 3,
                per_attempt_deadline: Duration::from_secs(2),
                overall_deadline: Duration::from_secs(5),
                base_backoff: Duration::ZERO,
                leader_ttl: Duration::from_secs(30),
            })
            .build()
            .await
            .unwrap();

        match client.get_seq("orders", 1).await {
            Err(ClientError::SeqUncertain) => {}
            other => panic!("post-send INTERNAL must be SeqUncertain, got {other:?}"),
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "post-send INTERNAL must NOT be retried (possible committed advance)"
        );
    }

    /// A successful `GetSeqResponse` whose `count` does not match the request is
    /// a protocol violation: the server committed an advance, but the block it
    /// describes is not the one the caller asked for. The client must not hand
    /// back a malformed block — it surfaces `SeqUncertain` (a commit occurred,
    /// reconcile) and invokes the server exactly once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn get_seq_response_count_mismatch_is_uncertain() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU64, Ordering};
        use tsoracle_proto::v1::tso_service_server::{TsoService, TsoServiceServer};

        struct MismatchedCountServer {
            calls: Arc<AtomicU64>,
        }

        #[tonic::async_trait]
        impl TsoService for MismatchedCountServer {
            async fn get_ts(
                &self,
                _request: tonic::Request<tsoracle_proto::v1::GetTsRequest>,
            ) -> Result<tonic::Response<tsoracle_proto::v1::GetTsResponse>, tonic::Status>
            {
                Err(tonic::Status::failed_precondition("not leader"))
            }
            async fn get_current_max_safe(
                &self,
                _request: tonic::Request<tsoracle_proto::v1::GetCurrentMaxSafeRequest>,
            ) -> Result<tonic::Response<tsoracle_proto::v1::GetCurrentMaxSafeResponse>, tonic::Status>
            {
                Ok(tonic::Response::new(
                    tsoracle_proto::v1::GetCurrentMaxSafeResponse::default(),
                ))
            }
            async fn get_seq(
                &self,
                request: tonic::Request<tsoracle_proto::v1::GetSeqRequest>,
            ) -> Result<tonic::Response<tsoracle_proto::v1::GetSeqResponse>, tonic::Status>
            {
                self.calls.fetch_add(1, Ordering::SeqCst);
                let req = request.into_inner();
                let (hi, lo) = tsoracle_core::Epoch(7).to_wire();
                // Honour the key, but return a count the caller did NOT request.
                Ok(tonic::Response::new(tsoracle_proto::v1::GetSeqResponse {
                    key: req.key,
                    start: 0,
                    count: req.count + 1,
                    epoch: Some(tsoracle_proto::v1::EpochWire { hi, lo }),
                }))
            }
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let incoming = tonic::transport::server::TcpIncoming::from(listener);
        let calls = Arc::new(AtomicU64::new(0));
        let server_calls = calls.clone();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(TsoServiceServer::new(MismatchedCountServer {
                    calls: server_calls,
                }))
                .serve_with_incoming(incoming)
                .await
                .ok();
        });

        let client = ClientBuilder::endpoints(vec![format!("http://{addr}")])
            .retry_policy(RetryPolicy {
                max_attempts: 3,
                per_attempt_deadline: Duration::from_secs(2),
                overall_deadline: Duration::from_secs(5),
                base_backoff: Duration::ZERO,
                leader_ttl: Duration::from_secs(30),
            })
            .build()
            .await
            .unwrap();

        match client.get_seq("orders", 5).await {
            Err(ClientError::SeqUncertain) => {}
            other => panic!("count-mismatch success must be SeqUncertain, got {other:?}"),
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "a malformed success must not trigger a (double-spending) retry"
        );
    }

    /// A successful `GetSeqResponse` echoing a different `key` than requested is
    /// likewise a protocol violation → `SeqUncertain`, server invoked once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn get_seq_response_key_mismatch_is_uncertain() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU64, Ordering};
        use tsoracle_proto::v1::tso_service_server::{TsoService, TsoServiceServer};

        struct MismatchedKeyServer {
            calls: Arc<AtomicU64>,
        }

        #[tonic::async_trait]
        impl TsoService for MismatchedKeyServer {
            async fn get_ts(
                &self,
                _request: tonic::Request<tsoracle_proto::v1::GetTsRequest>,
            ) -> Result<tonic::Response<tsoracle_proto::v1::GetTsResponse>, tonic::Status>
            {
                Err(tonic::Status::failed_precondition("not leader"))
            }
            async fn get_current_max_safe(
                &self,
                _request: tonic::Request<tsoracle_proto::v1::GetCurrentMaxSafeRequest>,
            ) -> Result<tonic::Response<tsoracle_proto::v1::GetCurrentMaxSafeResponse>, tonic::Status>
            {
                Ok(tonic::Response::new(
                    tsoracle_proto::v1::GetCurrentMaxSafeResponse::default(),
                ))
            }
            async fn get_seq(
                &self,
                request: tonic::Request<tsoracle_proto::v1::GetSeqRequest>,
            ) -> Result<tonic::Response<tsoracle_proto::v1::GetSeqResponse>, tonic::Status>
            {
                self.calls.fetch_add(1, Ordering::SeqCst);
                let req = request.into_inner();
                let (hi, lo) = tsoracle_core::Epoch(7).to_wire();
                // Echo a DIFFERENT key than the caller requested.
                Ok(tonic::Response::new(tsoracle_proto::v1::GetSeqResponse {
                    key: format!("{}-tampered", req.key),
                    start: 0,
                    count: req.count,
                    epoch: Some(tsoracle_proto::v1::EpochWire { hi, lo }),
                }))
            }
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let incoming = tonic::transport::server::TcpIncoming::from(listener);
        let calls = Arc::new(AtomicU64::new(0));
        let server_calls = calls.clone();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(TsoServiceServer::new(MismatchedKeyServer {
                    calls: server_calls,
                }))
                .serve_with_incoming(incoming)
                .await
                .ok();
        });

        let client = ClientBuilder::endpoints(vec![format!("http://{addr}")])
            .retry_policy(RetryPolicy {
                max_attempts: 3,
                per_attempt_deadline: Duration::from_secs(2),
                overall_deadline: Duration::from_secs(5),
                base_backoff: Duration::ZERO,
                leader_ttl: Duration::from_secs(30),
            })
            .build()
            .await
            .unwrap();

        match client.get_seq("orders", 5).await {
            Err(ClientError::SeqUncertain) => {}
            other => panic!("key-mismatch success must be SeqUncertain, got {other:?}"),
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "malformed success: one call"
        );
    }
}
