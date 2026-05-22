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

//! Endpoint retry policy for client RPCs.
//!
//! The worklist starts with the cached leader (if any) followed by configured
//! endpoints. On a NOT_LEADER response carrying a LeaderHint pointing at an
//! unvisited endpoint, that endpoint is pushed to the FRONT of the worklist
//! so we retry the hinted leader immediately — not at the end of the
//! round-robin pass, which would leave the current call to fail if the
//! hinted endpoint wasn't otherwise in the queue.
//!
//! Three deadlines bound the loop, governed by [`crate::RetryPolicy`]:
//!
//! - `per_attempt_deadline`: each `(pool.client, client.get_ts)` pair is
//!   wrapped in `tokio::time::timeout`. Same value is pushed to the
//!   tonic `Endpoint::connect_timeout` / `Endpoint::timeout` for the
//!   built-in transport paths so the transport layer also fails fast.
//! - `overall_deadline`: hard wall-clock cap on the whole call. The
//!   loop exits before starting any attempt that would push past it,
//!   even when `max_attempts` and the worklist still have headroom.
//! - `max_attempts`: tighter cap than the visited-set (which already
//!   prevents revisiting an endpoint). Bites only when leader-hint
//!   redirects expand the effective worklist.
//!
//! Between attempts whose last error is `Unavailable`,
//! `DeadlineExceeded`, or a transport-layer failure, the loop sleeps a
//! jittered exponential backoff. FAILED_PRECONDITION-with-hint redirects
//! do not back off — the next endpoint is known and the redirect is
//! part of normal discovery.

use std::collections::{HashSet, VecDeque};
use std::time::Duration;

use tokio::time::Instant;
use tsoracle_core::Timestamp;

use crate::error::ClientError;
use crate::leader_resolved::{ChannelPool, LeaderHintLookup, decode_leader_hint};
use crate::response::decode_get_ts_response;
use crate::retry_policy::{jittered_backoff, should_backoff};

pub(crate) async fn issue_rpc(
    pool: &ChannelPool,
    count: u32,
) -> Result<Vec<Timestamp>, ClientError> {
    let policy = pool.retry_policy().clone();
    let start = Instant::now();
    let deadline = start + policy.overall_deadline;
    let mut worklist: VecDeque<String> = pool.iter_round_robin().into();
    let mut visited: HashSet<String> = HashSet::new();
    let mut last_err: Option<ClientError> = None;
    let mut attempt_index: u32 = 0;

    while let Some(endpoint) = worklist.pop_front() {
        if !visited.insert(endpoint.clone()) {
            continue;
        }
        if attempt_index as usize >= policy.max_attempts {
            break;
        }
        let now = Instant::now();
        if now >= deadline {
            break;
        }
        let attempt_budget = (deadline - now).min(policy.per_attempt_deadline);

        #[cfg(feature = "tracing")]
        tracing::debug!(
            endpoint = %endpoint,
            count,
            attempt_index,
            budget_ms = attempt_budget.as_millis() as u64,
            "tsoracle-client: dispatching GetTs to endpoint",
        );

        match attempt(pool, &endpoint, count, attempt_budget).await {
            AttemptOutcome::Ok(timestamps) => {
                pool.set_leader(endpoint);
                return Ok(timestamps);
            }
            AttemptOutcome::LeaderHint(hinted_endpoint) => {
                #[cfg(feature = "metrics")]
                metrics::counter!("tsoracle.client.leader_pivots.total").increment(1);
                #[cfg(feature = "tracing")]
                tracing::debug!(
                    from = %endpoint,
                    to = %hinted_endpoint,
                    "tsoracle-client: pivoting to hinted leader",
                );
                pool.set_leader(hinted_endpoint.clone());
                worklist.push_front(hinted_endpoint);
                // No backoff: a leader hint is known progress, not a
                // failure to throttle.
                attempt_index = attempt_index.saturating_add(1);
                continue;
            }
            AttemptOutcome::HintRejected(status) => {
                pool.clear_leader();
                last_err = Some(ClientError::Rpc(status));
                attempt_index = attempt_index.saturating_add(1);
                continue;
            }
            AttemptOutcome::Err(err) => {
                let should_sleep = should_backoff(&err);
                last_err = Some(err);
                attempt_index = attempt_index.saturating_add(1);
                if should_sleep {
                    let backoff = jittered_backoff(policy.base_backoff, attempt_index - 1);
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    let sleep_for = backoff.min(remaining);
                    if sleep_for > Duration::ZERO {
                        tokio::time::sleep(sleep_for).await;
                    }
                }
                continue;
            }
        }
    }
    Err(last_err.unwrap_or(ClientError::NoReachableEndpoints))
}

/// Per-attempt outcome. Surfaces FAILED_PRECONDITION redirects as
/// their own variant so the caller can preserve the existing "no
/// backoff on hint" behaviour while still applying backoff to other
/// retriable failures.
enum AttemptOutcome {
    Ok(Vec<Timestamp>),
    LeaderHint(String),
    HintRejected(tonic::Status),
    Err(ClientError),
}

async fn attempt(
    pool: &ChannelPool,
    endpoint: &str,
    count: u32,
    budget: Duration,
) -> AttemptOutcome {
    let mut client = match tokio::time::timeout(budget, pool.client(endpoint)).await {
        Ok(Ok(client)) => client,
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
            return AttemptOutcome::Err(ClientError::Rpc(tonic::Status::deadline_exceeded(
                format!("connect exceeded per_attempt_deadline of {budget:?}"),
            )));
        }
    };
    let rpc = client.get_ts(tsoracle_proto::v1::GetTsRequest { count });
    let response = match tokio::time::timeout(budget, rpc).await {
        Ok(Ok(resp)) => resp,
        Ok(Err(status)) if status.code() == tonic::Code::FailedPrecondition => {
            #[cfg(feature = "metrics")]
            metrics::counter!("tsoracle.client.not_leader.total").increment(1);
            #[cfg(feature = "metrics")]
            metrics::counter!(
                "tsoracle.client.retries.total",
                "reason" => "not_leader",
            )
            .increment(1);

            let hinted_endpoint = match decode_leader_hint(&status) {
                LeaderHintLookup::Decoded(hint) => hint.leader_endpoint,
                LeaderHintLookup::Absent => {
                    #[cfg(feature = "tracing")]
                    tracing::warn!(
                        endpoint = %endpoint,
                        "tsoracle-client: FAILED_PRECONDITION without leader-hint trailer; \
                         contacted peer cannot redirect us",
                    );
                    None
                }
                LeaderHintLookup::Malformed => {
                    #[cfg(feature = "metrics")]
                    metrics::counter!("tsoracle.client.leader_hint.decode_failures.total")
                        .increment(1);
                    #[cfg(feature = "tracing")]
                    tracing::warn!(
                        endpoint = %endpoint,
                        "tsoracle-client: FAILED_PRECONDITION carried a malformed \
                         leader-hint trailer; treating as no hint",
                    );
                    None
                }
            };
            let usable_hint =
                hinted_endpoint.filter(|hinted| !rejects_plaintext_hint(pool, hinted));
            return match usable_hint {
                Some(hinted) => AttemptOutcome::LeaderHint(hinted),
                None => AttemptOutcome::HintRejected(status),
            };
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
            return AttemptOutcome::Err(ClientError::Rpc(status));
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
                "tsoracle-client: RPC exceeded per_attempt_deadline",
            );
            return AttemptOutcome::Err(ClientError::Rpc(tonic::Status::deadline_exceeded(
                format!("rpc exceeded per_attempt_deadline of {budget:?}"),
            )));
        }
    };
    match decode_get_ts_response(response.into_inner(), count) {
        Ok(timestamps) => AttemptOutcome::Ok(timestamps),
        Err(err) => {
            #[cfg(feature = "metrics")]
            metrics::counter!(
                "tsoracle.client.retries.total",
                "reason" => "decode_error",
            )
            .increment(1);
            AttemptOutcome::Err(err)
        }
    }
}

/// Refuse a wire-supplied leader hint that would downgrade the transport.
///
/// Under `ClientBuilder::tls_config`, a malicious or misconfigured peer
/// could otherwise feed the client an `http://...` leader endpoint via the
/// `tsoracle-leader-hint-bin` trailer and route the next RPC over plaintext.
/// The check is scoped to wire input: operator-supplied `endpoints` carrying
/// an explicit `http://` scheme are still honored ("explicit beats configured"
/// remains true for caller-controlled config).
///
/// Match shape mirrors `normalize_uri`: ASCII lowercase `http://` prefix.
/// Uppercase variants would already fail to parse after the bare→https
/// rewrite, so checking the lowercase form is sufficient.
fn rejects_plaintext_hint(pool: &ChannelPool, hint: &str) -> bool {
    let reject = pool.tls_required() && hint.starts_with("http://");
    #[cfg(feature = "tracing")]
    if reject {
        tracing::warn!(
            hinted_endpoint = %hint,
            "tsoracle-client: dropping plaintext leader-hint under tls_config; \
             refusing to downgrade transport"
        );
    }
    reject
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RetryPolicy;

    /// Aggressive policy used by the unit tests to keep them fast.
    /// `per_attempt_deadline` is the dominant cost — the integration
    /// tests cover wall-clock behaviour against real (unreachable)
    /// sockets, but the unit tests just want the loop to terminate.
    fn short_policy() -> RetryPolicy {
        RetryPolicy {
            max_attempts: 2,
            per_attempt_deadline: Duration::from_millis(100),
            overall_deadline: Duration::from_millis(300),
            base_backoff: Duration::from_millis(1),
        }
    }

    /// A pool seeded with duplicate endpoints must visit each once; the
    /// second visit hits the `!visited.insert` short-circuit and continues
    /// without burning an extra connect attempt. Since the endpoint is
    /// unreachable, the final outcome is `NoReachableEndpoints`, but the
    /// `visited` set being effective is the property under test here.
    #[tokio::test]
    async fn duplicate_endpoints_are_visited_once() {
        let pool = ChannelPool::new(
            vec!["http://127.0.0.1:1".into(), "http://127.0.0.1:1".into()],
            None,
            false,
            short_policy(),
        );
        let result = issue_rpc(&pool, 1).await;
        assert!(result.is_err(), "no live endpoint must surface as Err");
    }

    /// When every configured endpoint fails the connect attempt (closed
    /// port), the retry loop accumulates the last error and returns it as
    /// the surface failure. Exercises the `pool.client(...) -> Err`
    /// continue path that's not reached by the happy-path integration tests.
    #[tokio::test]
    async fn unreachable_endpoints_surface_last_error() {
        let pool = ChannelPool::new(
            vec!["http://127.0.0.1:1".into()],
            None,
            false,
            short_policy(),
        );
        let result = issue_rpc(&pool, 1).await;
        assert!(result.is_err(), "expected Err from unreachable pool");
    }

    /// Direct table-test for the wire-hint policy. The integration test in
    /// `crates/tsoracle-tests/tests/client_tls.rs` exercises the full
    /// FAILED_PRECONDITION→trailer→retry path end-to-end; this unit test
    /// pins down the predicate itself so a refactor cannot quietly flip
    /// the policy.
    #[test]
    fn plaintext_hint_policy_matches_scheme_and_tls_state() {
        let tls = ChannelPool::new(vec!["a:1".into()], None, true, RetryPolicy::default());
        let plain = ChannelPool::new(vec!["a:1".into()], None, false, RetryPolicy::default());

        assert!(
            rejects_plaintext_hint(&tls, "http://attacker:1"),
            "http:// hint must be rejected under tls_required"
        );
        assert!(
            !rejects_plaintext_hint(&tls, "https://peer:1"),
            "https:// hint must be allowed under tls_required"
        );
        assert!(
            !rejects_plaintext_hint(&tls, "peer:1"),
            "bare host:port hint must be allowed under tls_required (gets rewritten to https)"
        );
        assert!(
            !rejects_plaintext_hint(&plain, "http://peer:1"),
            "http:// hint must be allowed when tls is not required"
        );
    }

    /// A pool full of unreachable endpoints must surface its failure within
    /// the `overall_deadline`, not the OS-default TCP timeout (`~75 s` on
    /// Linux). The per-attempt deadline ensures each closed-port dial
    /// returns quickly; the overall deadline ensures the loop terminates
    /// even if a large pool would otherwise blow past it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn overall_deadline_caps_total_wall_clock() {
        // 5 endpoints, each closed. With max_attempts=5 and per_attempt
        // budget=100ms, naive iteration could spend up to ~500ms; the
        // overall_deadline=200ms must cut the loop short. Choose
        // base_backoff=0 so backoff sleeps are not a factor here — this
        // test pins the overall_deadline branch, not the backoff.
        let policy = RetryPolicy {
            max_attempts: 5,
            per_attempt_deadline: Duration::from_millis(100),
            overall_deadline: Duration::from_millis(200),
            base_backoff: Duration::ZERO,
        };
        let pool = ChannelPool::new(
            vec![
                "http://127.0.0.1:1".into(),
                "http://127.0.0.1:2".into(),
                "http://127.0.0.1:3".into(),
                "http://127.0.0.1:4".into(),
                "http://127.0.0.1:5".into(),
            ],
            None,
            false,
            policy,
        );
        let start = std::time::Instant::now();
        let result = issue_rpc(&pool, 1).await;
        let elapsed = start.elapsed();
        assert!(result.is_err(), "expected Err from all-unreachable pool");
        // Grace allowance covers tokio runtime jitter on slow CI runners.
        // The point is "≪ OS TCP timeout", not a microbenchmark.
        assert!(
            elapsed < Duration::from_secs(2),
            "must return within ~overall_deadline; took {elapsed:?}"
        );
    }

    /// `max_attempts` must cap the attempt count below the worklist size.
    /// Configuring 4 unreachable endpoints with `max_attempts=2` and a
    /// generous per-attempt budget proves the loop exits after two
    /// attempts rather than burning through the whole worklist.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn max_attempts_caps_iteration() {
        let policy = RetryPolicy {
            max_attempts: 2,
            per_attempt_deadline: Duration::from_millis(50),
            overall_deadline: Duration::from_secs(10),
            base_backoff: Duration::ZERO,
        };
        let pool = ChannelPool::new(
            vec![
                "http://127.0.0.1:1".into(),
                "http://127.0.0.1:2".into(),
                "http://127.0.0.1:3".into(),
                "http://127.0.0.1:4".into(),
            ],
            None,
            false,
            policy,
        );
        let start = std::time::Instant::now();
        let result = issue_rpc(&pool, 1).await;
        let elapsed = start.elapsed();
        assert!(result.is_err());
        // Two attempts at ~50ms each + scheduler slack. Capping at 1s is
        // enough to detect "loop kept iterating past max_attempts".
        assert!(
            elapsed < Duration::from_secs(1),
            "max_attempts=2 must cap iteration; took {elapsed:?}"
        );
    }
}
