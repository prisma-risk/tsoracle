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

//! A single `GetTs` attempt against one endpoint: the `(connect, get_ts)` pair
//! under one `PairBudget`, transport-class channel eviction (issue #239), and
//! the per-attempt outcome the retry loop dispatches on.

use std::time::Duration;

use tsoracle_core::Epoch;

use crate::budget::PairBudget;
use crate::channel_pool::ChannelPool;
use crate::error::ClientError;
use crate::leader_hint::classify_not_leader_hint;
use crate::response::{TimestampRange, decode_get_ts_response};
use crate::retry_policy::is_transport_failure;

/// Per-attempt outcome the retry loop dispatches on.
#[cfg_attr(test, derive(Debug))]
pub(crate) enum AttemptOutcome {
    Ok {
        /// Compact validated range decoded from `GetTsResponse`; expanded to
        /// per-waiter `Vec<Timestamp>`s only in `driver::deliver`.
        range: TimestampRange,
        /// Leader epoch carried in `GetTsResponse` (reassembled from the
        /// `epoch_hi`/`epoch_lo` halves). Plumbed to
        /// `ChannelPool::record_success` so the cache can compare it
        /// against future `LeaderHint` epochs.
        epoch: u128,
    },
    LeaderHint {
        endpoint: String,
        /// `None` only when the server omitted the leader epoch from the
        /// `LeaderHint` payload (a paxos backend, or an older openraft
        /// server). Once populated, the cache uses it as the upper bound
        /// future hints must meet to be honored.
        epoch: Option<u128>,
    },
    /// A reachable peer returned `FAILED_PRECONDITION` with **no** leader-hint
    /// trailer: it cannot redirect us because no leader is known yet. Distinct
    /// from `HintUnusable` (which carries an actionable-but-unfollowable hint)
    /// so the retry loop can ride out an in-progress election. Carries the
    /// originating status as `last_err`.
    NoLeaderYet(tonic::Status),
    /// A NOT_LEADER carrying a hint the client could not follow. `reason`
    /// distinguishes transient cluster flux the loop rides out (`StaleEpoch`)
    /// from a deterministic rejection it must keep failing fast on
    /// (`Rejected`). Carries the originating `FAILED_PRECONDITION` so a
    /// worklist that empties after only unusable hints surfaces the real
    /// NOT_LEADER rather than the misleading `NoReachableEndpoints` fallback.
    HintUnusable {
        status: tonic::Status,
        reason: HintUnusableReason,
    },
    Err(ClientError),
}

/// Why a decoded leader hint could not be followed — selects the retry loop's
/// reaction (ride out the election vs. fail fast).
#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
pub(crate) enum HintUnusableReason {
    /// A well-formed hint that could not outrank the cached leader (a strictly
    /// lower epoch, or an epoch-less hint to an off-list endpoint). Transient
    /// cluster flux — an election signal the loop rides out (issue #340).
    StaleEpoch,
    /// A NOT_LEADER the client could not act on for a deterministic reason: a
    /// malformed trailer, or a plaintext hint dropped under `tls_config`. Not
    /// an election — the loop must keep failing fast.
    Rejected,
}

pub(crate) async fn attempt(
    pool: &ChannelPool,
    endpoint: &str,
    count: u32,
    budget: Duration,
) -> AttemptOutcome {
    // `budget` bounds the whole `(connect, get_ts)` pair, not each phase
    // independently. `PairBudget` anchors one deadline up front so a slow
    // connect eats into the time left for `get_ts` instead of each phase
    // getting a fresh full budget — which would let one attempt run for up to
    // `2 * budget` and overrun `overall_deadline` before `max_attempts` is
    // reached.
    let pair = PairBudget::start(budget);
    // Keep the channel's cell handle for the whole RPC: a transport-class
    // failure below hands this exact cell to `evict_if_current` so the dead
    // channel is dropped without racing a concurrent re-dial (issue #239).
    let (mut client, cell) =
        match tokio::time::timeout(budget, pool.client_with_cell(endpoint)).await {
            Ok(Ok(leased)) => leased,
            Ok(Err(err)) => {
                #[cfg(feature = "metrics")]
                metrics::counter!(
                    "tsoracle.client.retries.total",
                    "reason" => "connect_failure",
                )
                .increment(1);
                #[cfg(feature = "tracing")]
                tracing::debug!(
                    endpoint = %endpoint,
                    error = %err,
                    "tsoracle-client: connect failed; advancing worklist",
                );
                // No channel was leased, so there is nothing to evict.
                return AttemptOutcome::Err(err);
            }
            Err(_) => {
                #[cfg(feature = "metrics")]
                metrics::counter!(
                    "tsoracle.client.retries.total",
                    "reason" => "deadline_exceeded",
                )
                .increment(1);
                #[cfg(feature = "tracing")]
                tracing::debug!(
                    endpoint = %endpoint,
                    budget_ms = budget.as_millis() as u64,
                    "tsoracle-client: connect exceeded per_attempt_deadline",
                );
                // No channel was leased, so there is nothing to evict.
                return AttemptOutcome::Err(ClientError::Rpc(tonic::Status::deadline_exceeded(
                    format!("connect exceeded per_attempt_deadline of {budget:?}"),
                )));
            }
        };
    // Give `get_ts` only what the connect phase left of the pair's budget.
    // `PairBudget::remaining` floors at zero, so a connect that consumed the
    // whole budget yields an immediate timeout rather than a fresh one.
    let rpc_budget = pair.remaining();
    let rpc = client.get_ts(tsoracle_proto::v1::GetTsRequest { count });
    // Each post-connect failure path produces the error here, then falls
    // through to the single eviction decision below — so the
    // "transport-class ⇒ evict the cached channel" invariant (issue #239)
    // lives in exactly one auditable place instead of being duplicated at
    // each failure site. The success and NOT_LEADER paths return early: a
    // healthy channel is never evicted.
    let err = match tokio::time::timeout(rpc_budget, rpc).await {
        Ok(Ok(response)) => {
            let inner = response.into_inner();
            // Capture before `decode_get_ts_response` consumes the message —
            // it returns only the timestamp vector, but the cache needs the
            // epoch from the same response to gate future `LeaderHint`
            // arrivals.
            let epoch = Epoch::from_wire(inner.epoch_hi, inner.epoch_lo).0;
            return match decode_get_ts_response(inner, count) {
                Ok(range) => AttemptOutcome::Ok { range, epoch },
                Err(err) => {
                    #[cfg(feature = "metrics")]
                    metrics::counter!(
                        "tsoracle.client.retries.total",
                        "reason" => "decode_error",
                    )
                    .increment(1);
                    // A decode error means the server answered over a healthy
                    // connection with a malformed payload — not a transport
                    // failure — so the channel is kept (this early return
                    // skips the eviction tail).
                    AttemptOutcome::Err(err)
                }
            };
        }
        Ok(Err(status)) if status.code() == tonic::Code::FailedPrecondition => {
            #[cfg(feature = "metrics")]
            metrics::counter!("tsoracle.client.not_leader.total").increment(1);
            #[cfg(feature = "metrics")]
            metrics::counter!(
                "tsoracle.client.retries.total",
                "reason" => "not_leader",
            )
            .increment(1);
            // NOT_LEADER is an application redirect over a healthy channel,
            // not a transport failure; classify it and return without
            // touching the cached channel.
            return classify_not_leader_hint(pool, endpoint, status);
        }
        Ok(Err(status)) => {
            #[cfg(feature = "metrics")]
            metrics::counter!(
                "tsoracle.client.retries.total",
                "reason" => "transport",
            )
            .increment(1);
            #[cfg(feature = "tracing")]
            tracing::debug!(
                endpoint = %endpoint,
                code = ?status.code(),
                "tsoracle-client: RPC failed; advancing worklist",
            );
            ClientError::Rpc(status)
        }
        Err(_) => {
            #[cfg(feature = "metrics")]
            metrics::counter!(
                "tsoracle.client.retries.total",
                "reason" => "deadline_exceeded",
            )
            .increment(1);
            #[cfg(feature = "tracing")]
            tracing::debug!(
                endpoint = %endpoint,
                budget_ms = rpc_budget.as_millis() as u64,
                "tsoracle-client: RPC exceeded its share of per_attempt_deadline",
            );
            // A timed-out RPC surfaces as `DeadlineExceeded`, which
            // `is_transport_failure` classifies as transport-class — so the
            // shared eviction tail below drops the (possibly half-open,
            // black-holing) channel just as the dedicated arm used to.
            ClientError::Rpc(tonic::Status::deadline_exceeded(format!(
                "rpc exceeded its share of per_attempt_deadline \
                 ({rpc_budget:?} of {budget:?})"
            )))
        }
    };
    // Single eviction point for every post-connect RPC failure: drop the
    // cached channel only on a transport-class failure (the connection looks
    // dead — issue #239). A non-transport status (`Internal`, etc.) means the
    // channel is healthy and the server merely returned an error, so it is
    // kept.
    if is_transport_failure(&err) {
        pool.evict_if_current(endpoint, &cell);
    }
    AttemptOutcome::Err(err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RetryPolicy;
    use crate::channel_pool::ChannelPool;
    use crate::test_support::{enable_tracing, short_policy};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    /// The per-attempt deadline bounds the whole `(connect, get_ts)` pair, not
    /// each phase independently. A slow connect that consumes most of the
    /// budget must leave only the remainder for `get_ts`, so a single
    /// `attempt` never runs longer than ~`per_attempt_deadline`. Wrapping each
    /// phase in the full budget would let a slow-connect/slow-RPC pair burn up
    /// to 2x the deadline and overrun `overall_deadline` before `max_attempts`.
    ///
    /// Uses an injected connector that sleeps for most of the budget before
    /// returning a live channel, then points it at a server whose `get_ts`
    /// hangs — so the RPC phase would block for its full timeout if it were
    /// given one. The assertion is the wall-clock bound on the whole pair.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn connect_and_rpc_share_one_per_attempt_deadline() {
        enable_tracing();
        use tsoracle_proto::v1::tso_service_server::{TsoService, TsoServiceServer};

        struct HangingServer;

        #[tonic::async_trait]
        impl TsoService for HangingServer {
            async fn get_ts(
                &self,
                _request: tonic::Request<tsoracle_proto::v1::GetTsRequest>,
            ) -> Result<tonic::Response<tsoracle_proto::v1::GetTsResponse>, tonic::Status>
            {
                // Hang well past any per-attempt budget so the client's RPC
                // timeout — not a server reply — decides when the phase ends.
                tokio::time::sleep(Duration::from_secs(30)).await;
                Err(tonic::Status::internal(
                    "unreachable: server should be timed out",
                ))
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
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback listener");
        let addr = listener.local_addr().expect("local_addr");
        let incoming = tonic::transport::server::TcpIncoming::from(listener);
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(TsoServiceServer::new(HangingServer))
                .serve_with_incoming(incoming)
                .await
                .ok();
        });

        let budget = Duration::from_millis(300);
        // Burn most of the budget in the connect phase; the RPC must get only
        // the ~50ms remainder, not a fresh full budget.
        let connect_delay = Duration::from_millis(250);

        let connector: std::sync::Arc<crate::transport::ChannelConnector> =
            std::sync::Arc::new(move |_endpoint: &str| {
                Box::pin(async move {
                    tokio::time::sleep(connect_delay).await;
                    let endpoint =
                        tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
                            .map_err(ClientError::from)?;
                    endpoint.connect().await.map_err(ClientError::from)
                })
            });

        let policy = RetryPolicy {
            max_attempts: 1,
            per_attempt_deadline: budget,
            overall_deadline: Duration::from_secs(30),
            base_backoff: Duration::ZERO,
            leader_ttl: Duration::from_secs(30),
        };
        let pool = ChannelPool::new(vec!["ignored:1".into()], Some(connector), false, policy);

        let start = std::time::Instant::now();
        let outcome = attempt(&pool, "ignored:1", 1, budget).await;
        let elapsed = start.elapsed();

        assert!(
            matches!(outcome, AttemptOutcome::Err(_)),
            "a hanging RPC must surface as Err, got {outcome:?}",
        );
        assert!(
            elapsed < Duration::from_millis(450),
            "connect + get_ts must share one per_attempt_deadline (~{budget:?}); \
             took {elapsed:?} — ~2x budget means each phase got the full deadline",
        );
    }

    /// A connector that counts its invocations and yields the channel built by
    /// `build`. The count is the observable proxy for "did `attempt` re-dial":
    /// an evicted channel forces a fresh `client_with_cell` cache miss (and
    /// thus a new connector call), while a retained channel is reused without
    /// one. Keeps the eviction tests from reaching into the pool's private
    /// channel map.
    fn counting_connector<F>(
        count: Arc<AtomicUsize>,
        build: F,
    ) -> Arc<crate::transport::ChannelConnector>
    where
        F: Fn() -> tonic::transport::Channel + Send + Sync + 'static,
    {
        Arc::new(move |_endpoint: &str| {
            count.fetch_add(1, Ordering::SeqCst);
            let channel = build();
            Box::pin(async move { Ok(channel) })
        })
    }

    /// Issue #239: a transport-class RPC failure must evict the cached channel
    /// so the next attempt re-dials (and re-resolves the endpoint) rather than
    /// reusing a channel pinned to a dead address. Here a lazily-connected
    /// channel to a closed port surfaces `Unavailable` on the first RPC; with
    /// eviction the connector runs once per attempt (count == 2), without it
    /// the second attempt would reuse the cached channel (count == 1).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn transport_failure_evicts_cached_channel() {
        let connect_count = Arc::new(AtomicUsize::new(0));
        let connector = counting_connector(connect_count.clone(), || {
            // connect_lazy returns immediately; the first RPC over it attempts
            // the real connect to the closed port and fails with Unavailable.
            tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy()
        });
        let pool = ChannelPool::new(
            vec!["dead:1".into()],
            Some(connector),
            false,
            short_policy(),
        );

        let budget = Duration::from_millis(200);
        let first = attempt(&pool, "dead:1", 1, budget).await;
        assert!(
            matches!(first, AttemptOutcome::Err(_)),
            "a closed port must fail the RPC, got {first:?}",
        );
        let _second = attempt(&pool, "dead:1", 1, budget).await;

        assert_eq!(
            connect_count.load(Ordering::SeqCst),
            2,
            "a transport failure must evict the channel so the next attempt re-dials",
        );
    }

    /// A non-transport application error (`Internal`) must NOT evict the
    /// cached channel — the connection is healthy, the server merely returned
    /// an error. The connector connects to a real server that always answers
    /// `Internal`; the second attempt must reuse the cached channel, so the
    /// connector runs exactly once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn application_error_preserves_cached_channel() {
        enable_tracing();
        use tsoracle_proto::v1::tso_service_server::{TsoService, TsoServiceServer};

        struct InternalServer;

        #[tonic::async_trait]
        impl TsoService for InternalServer {
            async fn get_ts(
                &self,
                _request: tonic::Request<tsoracle_proto::v1::GetTsRequest>,
            ) -> Result<tonic::Response<tsoracle_proto::v1::GetTsResponse>, tonic::Status>
            {
                Err(tonic::Status::internal("boom"))
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
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback listener");
        let addr = listener.local_addr().expect("local_addr");
        let incoming = tonic::transport::server::TcpIncoming::from(listener);
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(TsoServiceServer::new(InternalServer))
                .serve_with_incoming(incoming)
                .await
                .ok();
        });

        let connect_count = Arc::new(AtomicUsize::new(0));
        let connector = counting_connector(connect_count.clone(), move || {
            tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
                .expect("valid endpoint")
                .connect_lazy()
        });
        let pool = ChannelPool::new(
            vec!["server:1".into()],
            Some(connector),
            false,
            short_policy(),
        );

        let budget = Duration::from_secs(2);
        let first = attempt(&pool, "server:1", 1, budget).await;
        assert!(
            matches!(first, AttemptOutcome::Err(_)),
            "Internal must surface as Err, got {first:?}",
        );
        let _second = attempt(&pool, "server:1", 1, budget).await;

        assert_eq!(
            connect_count.load(Ordering::SeqCst),
            1,
            "an application error must not evict the channel; it must be reused",
        );
    }

    /// The user-selected policy evicts on timeout too: a hung RPC that exceeds
    /// its share of the per-attempt budget surfaces `DeadlineExceeded`
    /// (transport-class), so the channel is evicted and the next attempt
    /// re-dials. A half-open connection to a replaced pod that black-holes
    /// until timeout — rather than failing fast with `Unavailable` — is the
    /// real-world case this covers.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rpc_timeout_evicts_cached_channel() {
        use tsoracle_proto::v1::tso_service_server::{TsoService, TsoServiceServer};

        struct HangingServer;

        #[tonic::async_trait]
        impl TsoService for HangingServer {
            async fn get_ts(
                &self,
                _request: tonic::Request<tsoracle_proto::v1::GetTsRequest>,
            ) -> Result<tonic::Response<tsoracle_proto::v1::GetTsResponse>, tonic::Status>
            {
                tokio::time::sleep(Duration::from_secs(30)).await;
                Err(tonic::Status::internal("unreachable: should be timed out"))
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
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback listener");
        let addr = listener.local_addr().expect("local_addr");
        let incoming = tonic::transport::server::TcpIncoming::from(listener);
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(TsoServiceServer::new(HangingServer))
                .serve_with_incoming(incoming)
                .await
                .ok();
        });

        let connect_count = Arc::new(AtomicUsize::new(0));
        let connector = counting_connector(connect_count.clone(), move || {
            tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
                .expect("valid endpoint")
                .connect_lazy()
        });
        let pool = ChannelPool::new(
            vec!["hang:1".into()],
            Some(connector),
            false,
            short_policy(),
        );

        let budget = Duration::from_millis(200);
        let first = attempt(&pool, "hang:1", 1, budget).await;
        assert!(
            matches!(first, AttemptOutcome::Err(_)),
            "a hung RPC must surface as Err, got {first:?}",
        );
        let _second = attempt(&pool, "hang:1", 1, budget).await;

        assert_eq!(
            connect_count.load(Ordering::SeqCst),
            2,
            "an RPC timeout must evict the channel so the next attempt re-dials",
        );
    }

    /// A server that answers with a structurally valid gRPC message whose
    /// payload fails `decode_get_ts_response` (here: a `count` that disagrees
    /// with the requested count) surfaces as a non-transport `Err` and must NOT
    /// evict the channel — the connection is healthy, only the payload was
    /// wrong. Covers the decode-error arm in `attempt`, which early-returns
    /// before the transport-eviction tail.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn malformed_response_payload_surfaces_error_without_evicting() {
        enable_tracing();
        use tsoracle_proto::v1::tso_service_server::{TsoService, TsoServiceServer};

        struct WrongCountServer;

        #[tonic::async_trait]
        impl TsoService for WrongCountServer {
            async fn get_ts(
                &self,
                _request: tonic::Request<tsoracle_proto::v1::GetTsRequest>,
            ) -> Result<tonic::Response<tsoracle_proto::v1::GetTsResponse>, tonic::Status>
            {
                // The test requests count = 1, but the server claims 9: the
                // client's decoder rejects the mismatch over a healthy channel.
                Ok(tonic::Response::new(tsoracle_proto::v1::GetTsResponse {
                    physical_ms: 1,
                    logical_start: 0,
                    count: 9,
                    epoch_hi: 0,
                    epoch_lo: 0,
                }))
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
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback listener");
        let addr = listener.local_addr().expect("local_addr");
        let incoming = tonic::transport::server::TcpIncoming::from(listener);
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(TsoServiceServer::new(WrongCountServer))
                .serve_with_incoming(incoming)
                .await
                .ok();
        });

        let connect_count = Arc::new(AtomicUsize::new(0));
        let connector = counting_connector(connect_count.clone(), move || {
            tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
                .expect("valid endpoint")
                .connect_lazy()
        });
        let pool = ChannelPool::new(
            vec!["server:1".into()],
            Some(connector),
            false,
            short_policy(),
        );

        let budget = Duration::from_secs(2);
        let first = attempt(&pool, "server:1", 1, budget).await;
        assert!(
            matches!(first, AttemptOutcome::Err(_)),
            "a malformed payload must surface as Err, got {first:?}",
        );
        let _second = attempt(&pool, "server:1", 1, budget).await;
        assert_eq!(
            connect_count.load(Ordering::SeqCst),
            1,
            "a decode error leaves the channel cached (the connection is healthy); \
             the second attempt must reuse it rather than re-dial",
        );
    }
}
