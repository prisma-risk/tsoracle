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
mod transport;
mod worklist;

#[cfg(test)]
mod test_support;

pub use error::ClientError;
pub use retry_policy::RetryPolicy;
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
}
