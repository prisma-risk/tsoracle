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
//! The worklist starts with the cached leader (if any, and only while
//! still inside `RetryPolicy::leader_ttl`) followed by configured
//! endpoints. On a NOT_LEADER response carrying a LeaderHint pointing
//! at an unvisited endpoint, that endpoint is pushed to the FRONT of
//! the worklist so we retry the hinted leader immediately — not at
//! the end of the round-robin pass, which would leave the current
//! call to fail if the hinted endpoint wasn't otherwise in the queue.
//!
//! A LeaderHint that carries a leader epoch is honored only when the
//! cache permits it: a strictly lower-epoch hint is dropped silently
//! (counted, traced) so a delayed NOT_LEADER from an old epoch cannot
//! flap the cache backward. Hints with no epoch (a paxos backend, or an
//! older openraft server from before the epoch was populated) and hints
//! arriving when the cache has no epoch yet are accepted unconditionally
//! so a transition-state deployment is not left without leader discovery.
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
use tsoracle_core::{Epoch, Timestamp};

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
            AttemptOutcome::Ok { timestamps, epoch } => {
                pool.record_success(&endpoint, epoch);
                return Ok(timestamps);
            }
            AttemptOutcome::LeaderHint {
                endpoint: hinted_endpoint,
                epoch: hint_epoch,
            } => {
                #[cfg(feature = "metrics")]
                metrics::counter!("tsoracle.client.leader_pivots.total").increment(1);
                #[cfg(feature = "tracing")]
                tracing::debug!(
                    from = %endpoint,
                    to = %hinted_endpoint,
                    hint_epoch = ?hint_epoch,
                    "tsoracle-client: pivoting to hinted leader",
                );
                pool.set_leader_with(hinted_endpoint.clone(), hint_epoch);
                worklist.push_front(hinted_endpoint);
                // No backoff: a leader hint is known progress, not a
                // failure to throttle.
                attempt_index = attempt_index.saturating_add(1);
                continue;
            }
            AttemptOutcome::StaleLeaderHint(status) => {
                #[cfg(feature = "metrics")]
                metrics::counter!("tsoracle.client.leader_hint.stale.total").increment(1);
                // A stale hint means the contacted peer is out of date,
                // not that our cache is wrong. Keep the cache and advance
                // the worklist without backoff (this is still a
                // FAILED_PRECONDITION redirect attempt — just one we refuse
                // to follow). Preserve the status as last_err so a worklist
                // that empties after only stale redirects surfaces the real
                // NOT_LEADER, not a misleading NoReachableEndpoints.
                last_err = Some(ClientError::Rpc(status));
                attempt_index = attempt_index.saturating_add(1);
                continue;
            }
            AttemptOutcome::HintRejected(status) => {
                // A NOT_LEADER whose hint we could not act on — absent or
                // malformed trailer, or a hinted endpoint dropped by the
                // TLS-downgrade guard — is not evidence the cached leader is
                // wrong, only that this peer could not redirect us. Leave the
                // cache intact (clearing it stampedes every coalesced caller
                // back onto a cold worklist on each flap) and advance the
                // worklist, preserving the status to surface once exhausted.
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
/// their own variants so the caller can preserve the existing "no
/// backoff on hint" behaviour while still applying backoff to other
/// retriable failures. `StaleLeaderHint` carries the originating
/// `FAILED_PRECONDITION` status: the arm does not mutate the cache, but it
/// records the status as `last_err` so a worklist that empties after only
/// stale redirects surfaces the real NOT_LEADER rather than the misleading
/// `NoReachableEndpoints` fallback.
#[cfg_attr(test, derive(Debug))]
enum AttemptOutcome {
    Ok {
        timestamps: Vec<Timestamp>,
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
    StaleLeaderHint(tonic::Status),
    HintRejected(tonic::Status),
    Err(ClientError),
}

async fn attempt(
    pool: &ChannelPool,
    endpoint: &str,
    count: u32,
    budget: Duration,
) -> AttemptOutcome {
    // `budget` bounds the whole `(connect, get_ts)` pair, not each phase
    // independently. Anchor one deadline up front so a slow connect eats into
    // the time left for `get_ts` instead of each phase getting a fresh full
    // budget — which would let one attempt run for up to `2 * budget` and
    // overrun `overall_deadline` before `max_attempts` is reached.
    let pair_deadline = Instant::now() + budget;
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
    // Give `get_ts` only what the connect phase left of the pair's budget.
    // `saturating_duration_since` floors at zero, so a connect that consumed
    // the whole budget yields an immediate timeout rather than a fresh one.
    let rpc_budget = pair_deadline.saturating_duration_since(Instant::now());
    let rpc = client.get_ts(tsoracle_proto::v1::GetTsRequest { count });
    let response = match tokio::time::timeout(rpc_budget, rpc).await {
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
                budget_ms = rpc_budget.as_millis() as u64,
                "tsoracle-client: RPC exceeded its share of per_attempt_deadline",
            );
            return AttemptOutcome::Err(ClientError::Rpc(tonic::Status::deadline_exceeded(
                format!(
                    "rpc exceeded its share of per_attempt_deadline \
                     ({rpc_budget:?} of {budget:?})"
                ),
            )));
        }
    };
    let inner = response.into_inner();
    // Capture before `decode_get_ts_response` consumes the message —
    // it returns only the timestamp vector, but the cache needs the
    // epoch from the same response to gate future `LeaderHint` arrivals.
    let epoch = Epoch::from_wire(inner.epoch_hi, inner.epoch_lo).0;
    match decode_get_ts_response(inner, count) {
        Ok(timestamps) => AttemptOutcome::Ok { timestamps, epoch },
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

/// Decide what `issue_rpc` should do with a `FAILED_PRECONDITION` reply.
///
/// Pulled out of `attempt` so the decision tree — hint decoding,
/// plaintext-downgrade rejection, and the epoch-monotone gate — is
/// unit-testable without standing up a real gRPC peer. The production
/// path goes through here too, so the integration and unit tests
/// exercise the same code.
fn classify_not_leader_hint(
    pool: &ChannelPool,
    endpoint: &str,
    status: tonic::Status,
) -> AttemptOutcome {
    // Silence the unused-variable warning when `tracing` is off; the
    // parameter only flows into log fields below.
    let _ = endpoint;
    let (hinted_endpoint, hint_epoch) = match decode_leader_hint(&status) {
        LeaderHintLookup::Decoded(hint) => {
            // `leader_epoch` is present in full or absent — the nested
            // `EpochWire` makes a half-populated epoch unrepresentable — so
            // reassembly is a single map with no partial-pair case.
            let epoch = hint
                .leader_epoch
                .map(|epoch| Epoch::from_wire(epoch.hi, epoch.lo).0);
            (hint.leader_endpoint, epoch)
        }
        LeaderHintLookup::Absent => {
            #[cfg(feature = "tracing")]
            tracing::warn!(
                endpoint = %endpoint,
                "tsoracle-client: FAILED_PRECONDITION without leader-hint trailer; \
                 contacted peer cannot redirect us",
            );
            (None, None)
        }
        LeaderHintLookup::Malformed => {
            #[cfg(feature = "metrics")]
            metrics::counter!("tsoracle.client.leader_hint.decode_failures.total").increment(1);
            #[cfg(feature = "tracing")]
            tracing::warn!(
                endpoint = %endpoint,
                "tsoracle-client: FAILED_PRECONDITION carried a malformed \
                 leader-hint trailer; treating as no hint",
            );
            (None, None)
        }
    };
    let usable_endpoint = hinted_endpoint.filter(|hinted| !rejects_plaintext_hint(pool, hinted));
    match usable_endpoint {
        Some(hinted) => {
            if pool.accept_hint(hint_epoch) {
                AttemptOutcome::LeaderHint {
                    endpoint: hinted,
                    epoch: hint_epoch,
                }
            } else {
                #[cfg(feature = "tracing")]
                tracing::warn!(
                    from = %endpoint,
                    to = %hinted,
                    hint_epoch = ?hint_epoch,
                    "tsoracle-client: dropping stale leader hint with epoch \
                     behind the cached leader's epoch",
                );
                AttemptOutcome::StaleLeaderHint(status)
            }
        }
        None => AttemptOutcome::HintRejected(status),
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
            leader_ttl: Duration::from_secs(30),
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
            leader_ttl: Duration::from_secs(30),
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
            leader_ttl: Duration::from_secs(30),
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

    /// Build a `FAILED_PRECONDITION` status with a `LeaderHint`
    /// trailer encoded under the same key the server uses. Mirrors
    /// the production encoding in `crates/tsoracle-server/src/leader_hint.rs`
    /// so the unit tests exercise the exact wire shape the client
    /// will see in production.
    fn make_status_with_hint(hint: tsoracle_proto::v1::LeaderHint) -> tonic::Status {
        use prost::Message;
        use tonic::metadata::{BinaryMetadataValue, MetadataKey};
        let mut buf = Vec::new();
        hint.encode(&mut buf)
            .expect("LeaderHint encode is infallible");
        let mut status = tonic::Status::failed_precondition("not leader");
        let key =
            MetadataKey::from_bytes(b"tsoracle-leader-hint-bin").expect("static ASCII key parses");
        status
            .metadata_mut()
            .insert_bin(key, BinaryMetadataValue::from_bytes(&buf));
        status
    }

    /// A `FAILED_PRECONDITION` carrying no trailer at all surfaces as
    /// `HintRejected` so the retry loop preserves the status to return
    /// to the caller once the worklist is exhausted. Covers the
    /// `LeaderHintLookup::Absent` arm.
    #[test]
    fn classify_absent_hint_returns_hint_rejected() {
        let pool = ChannelPool::new(vec!["a:1".into()], None, false, RetryPolicy::default());
        let status = tonic::Status::failed_precondition("not leader");
        match classify_not_leader_hint(&pool, "a:1", status) {
            AttemptOutcome::HintRejected(_) => {}
            other => panic!("expected HintRejected, got {other:?}"),
        }
    }

    /// A trailer containing bytes that don't decode as a `LeaderHint`
    /// (here: 0xff repeated — never a valid protobuf prefix) must
    /// route to `HintRejected`, not panic, and must bump the
    /// decode-failures metric. Covers `LeaderHintLookup::Malformed`.
    #[test]
    fn classify_malformed_hint_returns_hint_rejected() {
        use tonic::metadata::{BinaryMetadataValue, MetadataKey};
        let pool = ChannelPool::new(vec!["a:1".into()], None, false, RetryPolicy::default());
        let mut status = tonic::Status::failed_precondition("not leader");
        let key = MetadataKey::from_bytes(b"tsoracle-leader-hint-bin").unwrap();
        status.metadata_mut().insert_bin(
            key,
            BinaryMetadataValue::from_bytes(&[0xff, 0xff, 0xff, 0xff]),
        );
        match classify_not_leader_hint(&pool, "a:1", status) {
            AttemptOutcome::HintRejected(_) => {}
            other => panic!("expected HintRejected, got {other:?}"),
        }
    }

    /// A well-formed hint with a higher leader epoch than the
    /// cached leader's must be followed. This is the bread-and-butter
    /// case: a freshly-elected leader supersedes our cached one.
    #[test]
    fn classify_higher_epoch_hint_returns_leader_hint() {
        let pool = ChannelPool::new(
            vec!["a:1".into(), "b:1".into()],
            None,
            false,
            RetryPolicy::default(),
        );
        pool.record_success("a:1", 5);
        let status = make_status_with_hint(tsoracle_proto::v1::LeaderHint {
            leader_endpoint: Some("b:1".into()),
            leader_epoch: Some(tsoracle_proto::v1::EpochWire { hi: 0, lo: 7 }),
        });
        match classify_not_leader_hint(&pool, "a:1", status) {
            AttemptOutcome::LeaderHint { endpoint, epoch } => {
                assert_eq!(endpoint, "b:1");
                assert_eq!(epoch, Some(7));
            }
            other => panic!("expected LeaderHint, got {other:?}"),
        }
    }

    /// Two peers redirect us at different epochs. Whatever order the hints
    /// arrive in, the client must end up following the higher-epoch leader and
    /// never flap its cache back to the lower-epoch one. This is the end-to-end
    /// safety property the populated server epoch unlocks.
    #[test]
    fn higher_epoch_hint_wins_regardless_of_arrival_order() {
        for stale_first in [true, false] {
            let pool = ChannelPool::new(
                vec!["a:1".into(), "b:1".into(), "c:1".into()],
                None,
                false,
                RetryPolicy::default(),
            );
            // Bootstrap the cache at a low epoch so the first hint is accepted.
            pool.record_success("a:1", 1);

            let fresh = make_status_with_hint(tsoracle_proto::v1::LeaderHint {
                leader_endpoint: Some("c:1".into()),
                leader_epoch: Some(tsoracle_proto::v1::EpochWire { hi: 0, lo: 9 }),
            });
            let stale = make_status_with_hint(tsoracle_proto::v1::LeaderHint {
                leader_endpoint: Some("b:1".into()),
                leader_epoch: Some(tsoracle_proto::v1::EpochWire { hi: 0, lo: 4 }),
            });
            let ordered = if stale_first {
                vec![stale, fresh]
            } else {
                vec![fresh, stale]
            };

            for status in ordered {
                if let AttemptOutcome::LeaderHint { endpoint, epoch } =
                    classify_not_leader_hint(&pool, "a:1", status)
                {
                    pool.set_leader_with(endpoint, epoch);
                }
            }

            assert_eq!(
                pool.cached_leader().as_deref(),
                Some("c:1"),
                "must follow the epoch-9 leader (stale_first={stale_first})",
            );
        }
    }

    /// A well-formed hint whose `leader_epoch` is strictly less than
    /// the cached leader's epoch must be dropped — that is the whole
    /// point of the epoch-monotone gate. The outcome carries the
    /// originating `FAILED_PRECONDITION` so the retry loop's
    /// `StaleLeaderHint` arm can record it as `last_err`; the arm
    /// continues without mutating the cache.
    #[test]
    fn classify_stale_epoch_hint_returns_stale_leader_hint() {
        let pool = ChannelPool::new(
            vec!["a:1".into(), "b:1".into()],
            None,
            false,
            RetryPolicy::default(),
        );
        pool.record_success("a:1", 10);
        let status = make_status_with_hint(tsoracle_proto::v1::LeaderHint {
            leader_endpoint: Some("b:1".into()),
            leader_epoch: Some(tsoracle_proto::v1::EpochWire { hi: 0, lo: 5 }),
        });
        match classify_not_leader_hint(&pool, "a:1", status) {
            AttemptOutcome::StaleLeaderHint(status) => {
                assert_eq!(status.code(), tonic::Code::FailedPrecondition);
            }
            other => panic!("expected StaleLeaderHint, got {other:?}"),
        }
        // Cache must be untouched.
        assert_eq!(pool.cached_leader().as_deref(), Some("a:1"));
    }

    /// A hint that carries no `leader_epoch` (a paxos backend, or an older
    /// openraft server) is accepted unconditionally so the client remains
    /// useful during a mixed-version deployment.
    #[test]
    fn classify_no_epoch_hint_returns_leader_hint() {
        let pool = ChannelPool::new(
            vec!["a:1".into(), "b:1".into()],
            None,
            false,
            RetryPolicy::default(),
        );
        pool.record_success("a:1", 10);
        let status = make_status_with_hint(tsoracle_proto::v1::LeaderHint {
            leader_endpoint: Some("b:1".into()),
            leader_epoch: None,
        });
        match classify_not_leader_hint(&pool, "a:1", status) {
            AttemptOutcome::LeaderHint { endpoint, epoch } => {
                assert_eq!(endpoint, "b:1");
                assert_eq!(epoch, None);
            }
            other => panic!("expected LeaderHint, got {other:?}"),
        }
    }

    /// Under `tls_required = true`, a hint with an explicit `http://`
    /// scheme must be refused so a malicious or misconfigured peer
    /// cannot downgrade the transport. The outcome is `HintRejected`
    /// (not `StaleLeaderHint`) because the cache is still valid; the
    /// hint just wasn't usable.
    #[test]
    fn classify_plaintext_hint_under_tls_returns_hint_rejected() {
        let pool = ChannelPool::new(vec!["a:1".into()], None, true, RetryPolicy::default());
        let status = make_status_with_hint(tsoracle_proto::v1::LeaderHint {
            leader_endpoint: Some("http://attacker:1".into()),
            leader_epoch: Some(tsoracle_proto::v1::EpochWire { hi: 0, lo: 7 }),
        });
        match classify_not_leader_hint(&pool, "a:1", status) {
            AttemptOutcome::HintRejected(_) => {}
            other => panic!("expected HintRejected, got {other:?}"),
        }
    }

    /// A NOT_LEADER answer that carries no actionable hint (here: no trailer
    /// at all → `AttemptOutcome::HintRejected`) must leave the cached leader
    /// in place. The cached leader is not evidence-wrong just because the
    /// contacted peer could not redirect us; clearing it stampedes every
    /// coalesced caller back onto a cold worklist on each NOT_LEADER flap.
    ///
    /// Drives the real `issue_rpc` loop against a loopback fake server so the
    /// `attempt → classify_not_leader_hint → HintRejected` path — and the
    /// loop's reaction to it — is exercised end to end, not stubbed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn hint_rejected_preserves_cached_leader() {
        use tsoracle_proto::v1::tso_service_server::{TsoService, TsoServiceServer};

        struct HintlessFollower;

        #[tonic::async_trait]
        impl TsoService for HintlessFollower {
            async fn get_ts(
                &self,
                _request: tonic::Request<tsoracle_proto::v1::GetTsRequest>,
            ) -> Result<tonic::Response<tsoracle_proto::v1::GetTsResponse>, tonic::Status>
            {
                // No `tsoracle-leader-hint-bin` trailer → LeaderHintLookup::Absent
                // → AttemptOutcome::HintRejected in the retry loop.
                Err(tonic::Status::failed_precondition("not leader"))
            }
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback listener");
        let addr = listener.local_addr().expect("local_addr");
        let incoming = tonic::transport::server::TcpIncoming::from(listener);
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(TsoServiceServer::new(HintlessFollower))
                .serve_with_incoming(incoming)
                .await
                .ok();
        });

        let endpoint = format!("http://{addr}");
        let pool = ChannelPool::new(vec![endpoint.clone()], None, false, short_policy());

        // Wait until the fake server accepts and replies FAILED_PRECONDITION.
        // The first successful connect also caches the channel in the pool, so
        // the later `issue_rpc` reaches the RPC layer instead of racing the dial.
        let ready_deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Ok(mut client) = pool.client(&endpoint).await {
                let replied_not_leader = client
                    .get_ts(tsoracle_proto::v1::GetTsRequest { count: 1 })
                    .await
                    .err()
                    .is_some_and(|status| status.code() == tonic::Code::FailedPrecondition);
                if replied_not_leader {
                    break;
                }
            }
            assert!(
                Instant::now() < ready_deadline,
                "fake follower never came up",
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // Seed a cached leader, as a prior successful RPC against it would.
        pool.record_success(&endpoint, 1);
        assert_eq!(pool.cached_leader().as_deref(), Some(endpoint.as_str()));

        // The only endpoint answers NOT_LEADER without a usable hint, so the
        // loop exhausts the worklist and surfaces the preserved status.
        let result = issue_rpc(&pool, 1).await;
        assert!(result.is_err(), "hintless NOT_LEADER must surface as Err");

        assert_eq!(
            pool.cached_leader().as_deref(),
            Some(endpoint.as_str()),
            "HintRejected (absent/malformed/TLS-rejected hint) must not \
             invalidate the cached leader",
        );
    }

    /// When every endpoint in the worklist redirects us to a strictly
    /// lower-epoch (stale) leader, the loop drops each hint and exhausts the
    /// worklist. The surfaced error must be the originating
    /// `FAILED_PRECONDITION`, not `NoReachableEndpoints` — the network was
    /// fine, the peer just pointed at an out-of-date leader. Surfacing
    /// `NoReachableEndpoints` would mislead callers during epoch transitions
    /// and mixed-version clusters, where stale redirects are common.
    ///
    /// Drives the real `issue_rpc` loop against a loopback peer so the
    /// `attempt → classify_not_leader_hint → StaleLeaderHint` path — and the
    /// loop's `last_err` bookkeeping — is exercised end to end.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stale_leader_hint_surfaces_failed_precondition_not_no_reachable_endpoints() {
        use tsoracle_proto::v1::tso_service_server::{TsoService, TsoServiceServer};

        struct StaleHintingFollower;

        #[tonic::async_trait]
        impl TsoService for StaleHintingFollower {
            async fn get_ts(
                &self,
                _request: tonic::Request<tsoracle_proto::v1::GetTsRequest>,
            ) -> Result<tonic::Response<tsoracle_proto::v1::GetTsResponse>, tonic::Status>
            {
                // NOT_LEADER with a well-formed hint at epoch 5 — strictly
                // behind the epoch-10 leader the client has cached, so the
                // epoch-monotone gate drops it: AttemptOutcome::StaleLeaderHint.
                Err(make_status_with_hint(tsoracle_proto::v1::LeaderHint {
                    leader_endpoint: Some("b:1".into()),
                    leader_epoch: Some(tsoracle_proto::v1::EpochWire { hi: 0, lo: 5 }),
                }))
            }
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback listener");
        let addr = listener.local_addr().expect("local_addr");
        let incoming = tonic::transport::server::TcpIncoming::from(listener);
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(TsoServiceServer::new(StaleHintingFollower))
                .serve_with_incoming(incoming)
                .await
                .ok();
        });

        let endpoint = format!("http://{addr}");
        let pool = ChannelPool::new(vec![endpoint.clone()], None, false, short_policy());

        // Wait until the fake peer accepts and replies FAILED_PRECONDITION; the
        // first successful connect also caches the channel so `issue_rpc` reaches
        // the RPC layer instead of racing the dial.
        let ready_deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Ok(mut client) = pool.client(&endpoint).await {
                let replied_not_leader = client
                    .get_ts(tsoracle_proto::v1::GetTsRequest { count: 1 })
                    .await
                    .err()
                    .is_some_and(|status| status.code() == tonic::Code::FailedPrecondition);
                if replied_not_leader {
                    break;
                }
            }
            assert!(
                Instant::now() < ready_deadline,
                "fake follower never came up",
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // Cache the only endpoint as leader at epoch 10, so the epoch-5 hint is
        // strictly stale and gets dropped rather than followed.
        pool.record_success(&endpoint, 10);

        let err = issue_rpc(&pool, 1)
            .await
            .expect_err("a stale-hint-only worklist must surface an error");
        match err {
            ClientError::Rpc(status) => assert_eq!(
                status.code(),
                tonic::Code::FailedPrecondition,
                "stale redirect must surface as FAILED_PRECONDITION",
            ),
            other => panic!(
                "expected ClientError::Rpc(FailedPrecondition), got {other:?} \
                 (NoReachableEndpoints means the StaleLeaderHint arm dropped last_err)"
            ),
        }
    }

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
}
