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
//! Queue bookkeeping (the worklist, the visited-set dedup, and
//! push-front-on-hint steering) lives in [`crate::worklist::Worklist`];
//! the deadline arithmetic lives in [`crate::budget`]. This module owns
//! only the policy decisions.
//!
//! Three deadlines bound the loop, governed by [`crate::RetryPolicy`] and
//! enforced by [`Budget`] / [`PairBudget`]:
//!
//! - `per_attempt_deadline`: each `(pool.client, client.get_ts)` pair is
//!   wrapped in `tokio::time::timeout`. Same value is pushed to the
//!   tonic `Endpoint::connect_timeout` / `Endpoint::timeout` for the
//!   built-in transport paths so the transport layer also fails fast.
//! - `overall_deadline`: hard wall-clock cap on the whole call. The
//!   loop exits before starting any attempt that would push past it,
//!   even when `max_attempts` and the worklist still have headroom.
//! - `max_attempts`: caps the number of *failed* attempts (dialed
//!   endpoints that returned an error), but never below the size of the
//!   initial worklist — a single cold-cache sweep always dials every
//!   endpoint it knows about at least once. Because `iter_round_robin`
//!   starts the configured tail at a randomly seeded rotation offset, a
//!   pool with more configured endpoints than `max_attempts` would
//!   otherwise be able to exhaust the budget on the peers ahead of the
//!   offset and never reach the only reachable endpoint behind it; the
//!   floor closes that gap. Leader-hint redirects are not charged against
//!   `max_attempts` either — they are bounded instead by the worklist
//!   visited-set, the per-pass [`MAX_LEADER_REDIRECTS`] cap, and the
//!   `overall_deadline` — so a legitimate failover redirect chain can still
//!   reach the live leader (issue #340) while a pathological one is bounded by
//!   the deadline: the client rides out the churn, then surfaces the redirect
//!   status (see "Riding out a leader election" on [`issue_rpc`]).
//!
//! Between attempts whose last error is `Unavailable`,
//! `DeadlineExceeded`, or a transport-layer failure, the loop sleeps a
//! jittered exponential backoff. FAILED_PRECONDITION-with-hint redirects
//! do not back off — the next endpoint is known and the redirect is
//! part of normal discovery.
//!
//! A transport-class RPC failure also evicts the endpoint's cached channel
//! ([`ChannelPool::evict_if_current`]) so the next attempt re-dials and
//! re-resolves rather than reusing a channel pinned to a now-dead address
//! (issue #239: a static tonic `Endpoint` resolves once and never
//! re-resolves, so a pod-replaced endpoint would otherwise keep the dead
//! channel and its background reconnect task forever). Application errors
//! such as `Internal` leave the channel cached — the connection is healthy.

use std::time::Duration;

use tsoracle_core::Epoch;

use crate::budget::{Budget, PairBudget};
use crate::channel_pool::{ChannelPool, LeaderHintLookup, decode_leader_hint};
use crate::error::ClientError;
use crate::response::{TimestampRange, decode_get_ts_response};
use crate::retry_policy::{is_transport_failure, jittered_backoff, should_backoff};
use crate::worklist::Worklist;

/// Ceiling on actionable leader-hint pivots within a single re-poll *pass* of
/// `issue_rpc`.
///
/// Issue #340 deliberately stopped charging leader-hint redirects against
/// [`RetryPolicy::max_attempts`](crate::RetryPolicy::max_attempts) so a
/// legitimate failover chain can outlast the failure budget. This constant caps
/// the per-pass redirect chain so a malicious or persistently flapping peer that
/// returns a fresh, never-visited hint on every dial cannot churn connections
/// unboundedly within one pass. Hitting the cap is treated as an in-progress
/// leadership transfer: the pass ends and `issue_rpc` rides out the churn across
/// further passes, bounded overall by `overall_deadline` — so the deadline, not
/// this cap, is the whole-call ceiling on churn. The cap is far above any real
/// failover, which dedups via the worklist visited-set and settles in a few hops.
const MAX_LEADER_REDIRECTS: u32 = 16;

/// Issue one `GetTs`, retrying across endpoints and following leader hints.
///
/// # Riding out a leader election
///
/// When a pass over the worklist ends without a timestamp, `issue_rpc` re-polls
/// (backing off, bounded by `overall_deadline`) **only** if that pass saw a
/// reachable server report an in-progress election: an absent-hint NOT_LEADER
/// (`AttemptOutcome::NoLeaderYet`), a `StaleLeaderHint`, or the
/// `MAX_LEADER_REDIRECTS` cap being hit (a churning leadership transfer). A pass
/// that only hit transport failures or deterministic hint rejections
/// (`HintRejected`) does not re-poll — a genuinely-unreachable pool still fails
/// fast. `failed_attempts` and the last error persist across passes (so
/// `max_attempts` keeps its whole-call meaning and the surfaced error is the
/// real NOT_LEADER / transport status, never `NoReachableEndpoints`); the
/// worklist and the per-pass redirect budget reset each pass.
pub(crate) async fn issue_rpc(
    pool: &ChannelPool,
    count: u32,
) -> Result<TimestampRange, ClientError> {
    let policy = pool.retry_policy().clone();
    let budget = Budget::start(&policy);
    // Persist across passes: the overall-deadline budget, the last error
    // surfaced, and the failed-attempt budget (so `RetryPolicy::max_attempts`
    // keeps its documented whole-call meaning — see the field's rustdoc).
    let mut last_err: Option<ClientError> = None;
    let mut failed_attempts: u32 = 0;
    // The failed-attempt cap is floored at the initial worklist size so one
    // cold sweep always dials every configured endpoint at least once even when
    // `max_attempts` is smaller (issue #404). Computed once from the first
    // pass's endpoint set.
    let mut attempt_cap: usize = 0;
    let mut pass: u32 = 0;

    loop {
        // Reset per pass: a fresh worklist (fresh visited-set), the redirect
        // budget (so a settled cluster can be reached after an earlier pass hit
        // the cap — see the design spec), and the election signal.
        let initial_endpoints = pool.iter_round_robin();
        if pass == 0 {
            attempt_cap = policy.max_attempts.max(initial_endpoints.len());
        }
        let mut worklist = Worklist::new(initial_endpoints);
        let mut redirects: u32 = 0;
        let mut saw_election_signal = false;

        while let Some(endpoint) = worklist.next() {
            if failed_attempts as usize >= attempt_cap {
                break;
            }
            let Some(attempt_budget) = budget.next_attempt() else {
                // Overall deadline reached; do not start another attempt.
                break;
            };

            #[cfg(feature = "tracing")]
            tracing::debug!(
                endpoint = %endpoint,
                count,
                failed_attempts,
                pass,
                budget_ms = attempt_budget.as_millis() as u64,
                "tsoracle-client: dispatching GetTs to endpoint",
            );

            match attempt(pool, &endpoint, count, attempt_budget).await {
                AttemptOutcome::Ok { range, epoch } => {
                    pool.record_success(&endpoint, epoch);
                    return Ok(range);
                }
                AttemptOutcome::LeaderHint {
                    endpoint: hinted_endpoint,
                    epoch: hint_epoch,
                } => {
                    let _ = hint_epoch;
                    if redirects >= MAX_LEADER_REDIRECTS {
                        #[cfg(feature = "metrics")]
                        metrics::counter!("tsoracle.client.leader_redirect_cap.total").increment(1);
                        #[cfg(feature = "tracing")]
                        tracing::warn!(
                            from = %endpoint,
                            to = %hinted_endpoint,
                            max_redirects = MAX_LEADER_REDIRECTS,
                            "tsoracle-client: leader-hint redirect cap reached this pass",
                        );
                        last_err = Some(ClientError::Rpc(tonic::Status::failed_precondition(
                            format!(
                                "leader-hint redirect cap ({MAX_LEADER_REDIRECTS}) reached \
                                 before finding the live leader"
                            ),
                        )));
                        // A cluster that keeps hinting a not-yet-ready leader is
                        // churning (CockroachDB-style transfer). Signal an
                        // election so the worklist-empty handler backs off and
                        // re-polls; `redirects` resets next pass, so once the
                        // cluster settles a later pass reaches the leader.
                        saw_election_signal = true;
                        break;
                    }
                    redirects += 1;
                    #[cfg(feature = "metrics")]
                    metrics::counter!("tsoracle.client.leader_pivots.total").increment(1);
                    #[cfg(feature = "tracing")]
                    tracing::debug!(
                        from = %endpoint,
                        to = %hinted_endpoint,
                        hint_epoch = ?hint_epoch,
                        "tsoracle-client: pivoting to hinted leader",
                    );
                    worklist.redirect_to(hinted_endpoint);
                    continue;
                }
                AttemptOutcome::NoLeaderYet(status) => {
                    // A reachable peer has no leader to redirect us to: the
                    // cluster is (re-)electing. Signal it so the worklist-empty
                    // handler rides out the election. Known progress, not a
                    // throttled failure — no budget charge, no in-pass backoff.
                    saw_election_signal = true;
                    last_err = Some(ClientError::Rpc(status));
                    continue;
                }
                AttemptOutcome::StaleLeaderHint(status) => {
                    #[cfg(feature = "metrics")]
                    metrics::counter!("tsoracle.client.leader_hint.stale.total").increment(1);
                    // A lagging peer pointed at an older-epoch leader — transient
                    // cluster flux. Treat as an election signal; do not charge
                    // the budget (issue #340).
                    saw_election_signal = true;
                    last_err = Some(ClientError::Rpc(status));
                    continue;
                }
                AttemptOutcome::HintRejected(status) => {
                    // NOT_LEADER we could not act on (malformed trailer or a
                    // guard-dropped hint — absent-hint is NoLeaderYet above).
                    // Deterministic, not an election: record and advance
                    // without a signal.
                    last_err = Some(ClientError::Rpc(status));
                    continue;
                }
                AttemptOutcome::Err(err) => {
                    let should_sleep = should_backoff(&err);
                    last_err = Some(err);
                    failed_attempts = failed_attempts.saturating_add(1);
                    if should_sleep {
                        let backoff = jittered_backoff(policy.base_backoff, failed_attempts - 1);
                        let sleep_for = budget.clamp_backoff(backoff);
                        if sleep_for > Duration::ZERO {
                            tokio::time::sleep(sleep_for).await;
                        }
                    }
                    continue;
                }
            }
        }

        // The pass ended. Ride out only if a reachable server signalled an
        // in-progress election this pass and the overall deadline still has
        // room; otherwise fail fast (dead pool / deterministic rejection),
        // surfacing the persisted last error.
        if saw_election_signal && budget.next_attempt().is_some() {
            let backoff = jittered_backoff(policy.base_backoff, pass);
            let sleep_for = budget.clamp_backoff(backoff);
            if sleep_for > Duration::ZERO {
                tokio::time::sleep(sleep_for).await;
            }
            pass = pass.saturating_add(1);
            continue;
        }
        break;
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
    StaleLeaderHint(tonic::Status),
    HintRejected(tonic::Status),
    /// A reachable peer returned `FAILED_PRECONDITION` with **no** leader-hint
    /// trailer: it cannot redirect us because no leader is known yet. Distinct
    /// from `HintRejected` (malformed trailer / TLS-downgrade-guard drop, which
    /// are deterministic and must keep failing fast) so the retry loop can ride
    /// out an in-progress election. Carries the originating status as `last_err`.
    NoLeaderYet(tonic::Status),
    Err(ClientError),
}

async fn attempt(
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
            // No leader-hint trailer: the peer is up but no leader is known
            // yet (the election signature). Return early as its own outcome so
            // the retry loop can ride out the election — distinct from the
            // malformed / guard-dropped cases below, which stay `HintRejected`
            // and keep failing fast.
            #[cfg(feature = "tracing")]
            tracing::warn!(
                endpoint = %endpoint,
                "tsoracle-client: FAILED_PRECONDITION without leader-hint trailer; \
                 contacted peer cannot redirect us (no leader yet)",
            );
            return AttemptOutcome::NoLeaderYet(status);
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
            // Seat the hint under the same lock that checks the
            // monotone-forward rule. Doing the check and the write as one
            // atomic step (rather than gating here and writing later in the
            // dispatch loop) is what prevents a concurrent higher-epoch
            // promotion from being clobbered by this lower-or-equal hint.
            if pool.compare_and_set_leader(hinted.clone(), hint_epoch) {
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
                    "tsoracle-client: dropping leader hint that cannot outrank \
                     the cached leader — either a stale epoch behind the cached \
                     one, or an epoch-less hint to an off-list endpoint",
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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::Instant;

    /// Install a process-global `TRACE`-level subscriber so the retry loop's
    /// `tracing::{debug,warn}!` sites evaluate and format their fields under
    /// test. With no subscriber installed those macros short-circuit before the
    /// field expressions run, so the formatting code never executes (and a typo
    /// in a `%endpoint` Display field or a renamed variable would go unnoticed).
    /// Idempotent across tests: `try_init` installs the global default once and
    /// returns `Err` (ignored) for every later caller, so any test may call it.
    fn enable_tracing() {
        use tracing_subscriber::filter::LevelFilter;
        let _ = tracing_subscriber::fmt()
            .with_max_level(LevelFilter::TRACE)
            .with_test_writer()
            .try_init();
    }

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
        enable_tracing();
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
        enable_tracing();
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

    /// The failed-attempt budget is floored at the initial worklist size, so a
    /// single cold-cache sweep dials *every* configured endpoint at least once
    /// even when `max_attempts` is smaller than the endpoint count. This is the
    /// contract that keeps the randomly seeded rotation offset in
    /// `iter_round_robin` from stranding the only reachable endpoint behind
    /// more failing peers than `max_attempts` allows.
    ///
    /// Four unreachable endpoints with `max_attempts = 2`: the loop must dial
    /// all four (not stop at two), proven by the connector's invocation count.
    /// Before the floor, the loop broke after two dials and left two endpoints
    /// — possibly the only live one — untried.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failed_attempt_budget_is_floored_to_a_full_sweep() {
        let policy = RetryPolicy {
            max_attempts: 2,
            per_attempt_deadline: Duration::from_millis(50),
            overall_deadline: Duration::from_secs(10),
            base_backoff: Duration::ZERO,
            leader_ttl: Duration::from_secs(30),
        };
        // Every dial fails fast with a transport-class error and bumps the
        // counter, so the count is exactly the number of endpoints the loop
        // visited — the observable proxy for "did the sweep cover all four".
        let dials = Arc::new(AtomicUsize::new(0));
        let dials_for_connector = dials.clone();
        let connector: Arc<crate::transport::ChannelConnector> =
            Arc::new(move |_endpoint: &str| {
                dials_for_connector.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move {
                    Err(ClientError::Rpc(tonic::Status::unavailable(
                        "simulated dead endpoint",
                    )))
                })
            });
        let pool = ChannelPool::new(
            vec![
                "dead-1:1".into(),
                "dead-2:1".into(),
                "dead-3:1".into(),
                "dead-4:1".into(),
            ],
            Some(connector),
            false,
            policy,
        );
        let result = issue_rpc(&pool, 1).await;
        assert!(result.is_err(), "expected Err from all-unreachable pool");
        assert_eq!(
            dials.load(Ordering::SeqCst),
            4,
            "max_attempts=2 must not cut the cold sweep short: every configured \
             endpoint must be dialed at least once (the floor)",
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
        let key = MetadataKey::from_bytes(tsoracle_proto::v1::LEADER_HINT_TRAILER_KEY.as_bytes())
            .expect("static ASCII key parses");
        status
            .metadata_mut()
            .insert_bin(key, BinaryMetadataValue::from_bytes(&buf));
        status
    }

    /// A FAILED_PRECONDITION with NO leader-hint trailer (a follower that does
    /// not yet know a leader — the election signature) classifies as
    /// `NoLeaderYet`, distinct from a malformed/guard-dropped hint
    /// (`HintRejected`). Pins the split that lets the retry loop ride out an
    /// election without also riding out deterministic bad-hint rejections.
    #[test]
    fn absent_hint_classifies_as_no_leader_yet() {
        let pool = ChannelPool::new(vec!["peer:1".into()], None, false, RetryPolicy::default());
        let status = tonic::Status::failed_precondition("electing, no leader yet");
        match classify_not_leader_hint(&pool, "peer:1", status) {
            AttemptOutcome::NoLeaderYet(s) => {
                assert_eq!(s.code(), tonic::Code::FailedPrecondition);
            }
            other => panic!("absent hint must be NoLeaderYet, got {other:?}"),
        }
    }

    /// A `FAILED_PRECONDITION` carrying no trailer at all surfaces as
    /// `NoLeaderYet` (the election signature: the peer is up but no leader is
    /// known yet). Covers the `LeaderHintLookup::Absent` arm.
    #[test]
    fn classify_absent_hint_returns_no_leader_yet() {
        enable_tracing();
        let pool = ChannelPool::new(vec!["a:1".into()], None, false, RetryPolicy::default());
        let status = tonic::Status::failed_precondition("not leader");
        match classify_not_leader_hint(&pool, "a:1", status) {
            AttemptOutcome::NoLeaderYet(_) => {}
            other => panic!("expected NoLeaderYet, got {other:?}"),
        }
    }

    /// A trailer containing bytes that don't decode as a `LeaderHint`
    /// (here: 0xff repeated — never a valid protobuf prefix) must
    /// route to `HintRejected`, not panic, and must bump the
    /// decode-failures metric. Covers `LeaderHintLookup::Malformed`.
    #[test]
    fn classify_malformed_hint_returns_hint_rejected() {
        enable_tracing();
        use tonic::metadata::{BinaryMetadataValue, MetadataKey};
        let pool = ChannelPool::new(vec!["a:1".into()], None, false, RetryPolicy::default());
        let mut status = tonic::Status::failed_precondition("not leader");
        let key = MetadataKey::from_bytes(tsoracle_proto::v1::LEADER_HINT_TRAILER_KEY.as_bytes())
            .unwrap();
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

            // `classify_not_leader_hint` seats the cache atomically as part
            // of the monotone-forward check, so the loop need only drive
            // each redirect through it — no separate write step.
            for status in ordered {
                let _ = classify_not_leader_hint(&pool, "a:1", status);
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
        enable_tracing();
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
        enable_tracing();
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
    /// at all → `AttemptOutcome::NoLeaderYet`) must leave the cached leader
    /// in place. The cached leader is not evidence-wrong just because the
    /// contacted peer could not redirect us; clearing it stampedes every
    /// coalesced caller back onto a cold worklist on each NOT_LEADER flap.
    ///
    /// Drives the real `issue_rpc` loop against a loopback fake server so the
    /// `attempt → classify_not_leader_hint → NoLeaderYet` path — and the
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
                // → AttemptOutcome::NoLeaderYet in the retry loop.
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
            "NoLeaderYet (absent hint) must not invalidate the cached leader",
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

    /// Issue #340: a legitimate leader-hint redirect chain longer than
    /// `max_attempts` must still reach the live leader. Redirects are
    /// "known progress, not a failure to throttle" — only failed attempts
    /// (`AttemptOutcome::Err`) consume the `max_attempts` budget. Here the
    /// peer redirects three times (more than `max_attempts = 2`) before a
    /// fourth dial answers with a timestamp; the loop must follow the whole
    /// chain and return the timestamp rather than surfacing a hint status.
    ///
    /// A single backend stands in for the chain: a connector maps every
    /// hinted endpoint string to one loopback server whose per-call counter
    /// decides whether to redirect (to a fresh, unvisited endpoint, so the
    /// worklist keeps advancing) or to succeed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn redirect_chain_longer_than_max_attempts_reaches_leader() {
        use std::future::Future;
        use std::pin::Pin;
        use tsoracle_proto::v1::tso_service_server::{TsoService, TsoServiceServer};

        // Number of NOT_LEADER redirects before the server answers. Strictly
        // greater than `max_attempts` below, so the old "every outcome bumps
        // the attempt budget" behaviour would exhaust the budget mid-chain.
        const REDIRECTS: usize = 3;

        struct RedirectingLeaderChain {
            calls: Arc<AtomicUsize>,
        }

        #[tonic::async_trait]
        impl TsoService for RedirectingLeaderChain {
            async fn get_ts(
                &self,
                _request: tonic::Request<tsoracle_proto::v1::GetTsRequest>,
            ) -> Result<tonic::Response<tsoracle_proto::v1::GetTsResponse>, tonic::Status>
            {
                let n = self.calls.fetch_add(1, Ordering::SeqCst);
                if n < REDIRECTS {
                    // Hint at a fresh endpoint string (unvisited, so the
                    // worklist advances) with no epoch (followed
                    // unconditionally, so this is always an actionable hint).
                    Err(make_status_with_hint(tsoracle_proto::v1::LeaderHint {
                        leader_endpoint: Some(format!("redirect-{}:1", n + 1)),
                        leader_epoch: None,
                    }))
                } else {
                    Ok(tonic::Response::new(tsoracle_proto::v1::GetTsResponse {
                        physical_ms: 1,
                        logical_start: 0,
                        count: 1,
                        epoch_hi: 0,
                        epoch_lo: 0,
                    }))
                }
            }
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback listener");
        let addr = listener.local_addr().expect("local_addr");
        let incoming = tonic::transport::server::TcpIncoming::from(listener);
        let calls = Arc::new(AtomicUsize::new(0));
        let server_calls = calls.clone();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(TsoServiceServer::new(RedirectingLeaderChain {
                    calls: server_calls,
                }))
                .serve_with_incoming(incoming)
                .await
                .ok();
        });

        // Every hinted endpoint resolves to the one backend; the chain lives
        // in the server's call counter, not in distinct listeners.
        let connector: Arc<crate::transport::ChannelConnector> =
            Arc::new(move |_endpoint: &str| {
                let target = format!("http://{addr}");
                Box::pin(async move {
                    tonic::transport::Endpoint::from_shared(target)
                        .map_err(ClientError::from)?
                        .connect()
                        .await
                        .map_err(ClientError::from)
                }) as Pin<Box<dyn Future<Output = Result<_, _>> + Send>>
            });

        let policy = RetryPolicy {
            max_attempts: 2,
            per_attempt_deadline: Duration::from_secs(2),
            overall_deadline: Duration::from_secs(10),
            base_backoff: Duration::ZERO,
            leader_ttl: Duration::from_secs(30),
        };
        let pool = ChannelPool::new(vec!["redirect-0:1".into()], Some(connector), false, policy);

        let range = issue_rpc(&pool, 1)
            .await
            .expect("a redirect chain that ends at a live leader must yield a timestamp");
        assert_eq!(
            range.count(),
            1,
            "the leader returned exactly one timestamp"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            REDIRECTS + 1,
            "the loop must dial through all {REDIRECTS} redirects to the leader",
        );
    }

    /// CockroachDB-style leadership transfer: the cluster hints a churning chain
    /// that exceeds MAX_LEADER_REDIRECTS, then settles and serves. With the cap
    /// treated as an election signal (and `redirects` reset per pass), a later
    /// pass must follow the chain to the settled leader rather than giving up at
    /// the cap.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn redirect_cap_then_settles_reaches_leader() {
        use tsoracle_proto::v1::tso_service_server::{TsoService, TsoServiceServer};

        const CHURN: usize = MAX_LEADER_REDIRECTS as usize + 3;

        struct ChurnsThenServes {
            calls: Arc<AtomicUsize>,
        }

        #[tonic::async_trait]
        impl TsoService for ChurnsThenServes {
            async fn get_ts(
                &self,
                _request: tonic::Request<tsoracle_proto::v1::GetTsRequest>,
            ) -> Result<tonic::Response<tsoracle_proto::v1::GetTsResponse>, tonic::Status>
            {
                let n = self.calls.fetch_add(1, Ordering::SeqCst);
                if n < CHURN {
                    Err(make_status_with_hint(tsoracle_proto::v1::LeaderHint {
                        leader_endpoint: Some(format!("redirect-{}:1", n + 1)),
                        leader_epoch: None,
                    }))
                } else {
                    Ok(tonic::Response::new(tsoracle_proto::v1::GetTsResponse {
                        physical_ms: 1,
                        logical_start: 0,
                        count: 1,
                        epoch_hi: 0,
                        epoch_lo: 0,
                    }))
                }
            }
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback listener");
        let addr = listener.local_addr().expect("local_addr");
        let incoming = tonic::transport::server::TcpIncoming::from(listener);
        let calls = Arc::new(AtomicUsize::new(0));
        let server_calls = calls.clone();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(TsoServiceServer::new(ChurnsThenServes {
                    calls: server_calls,
                }))
                .serve_with_incoming(incoming)
                .await
                .ok();
        });

        let connector: Arc<crate::transport::ChannelConnector> =
            Arc::new(move |_endpoint: &str| {
                let target = format!("http://{addr}");
                Box::pin(async move {
                    tonic::transport::Endpoint::from_shared(target)
                        .map_err(ClientError::from)?
                        .connect()
                        .await
                        .map_err(ClientError::from)
                })
            });

        let policy = RetryPolicy {
            max_attempts: 2,
            per_attempt_deadline: Duration::from_secs(2),
            overall_deadline: Duration::from_secs(10),
            base_backoff: Duration::from_millis(5),
            leader_ttl: Duration::from_secs(30),
        };
        let pool = ChannelPool::new(vec!["redirect-0:1".into()], Some(connector), false, policy);

        let range = issue_rpc(&pool, 1)
            .await
            .expect("a cluster that settles after churn must be ridden out to the leader");
        assert_eq!(
            range.count(),
            1,
            "the settled leader returned one timestamp"
        );
    }

    /// Security/availability hardening: a peer that answers every dial with a
    /// fresh, never-visited leader hint never lets the client reach a leader.
    /// With the redirect cap treated as an election signal (so a genuine
    /// leadership *transfer* is ridden out), such a peer is bounded by the
    /// client's own `overall_deadline` — it rides out, then surfaces the
    /// redirect `FAILED_PRECONDITION`, never a misleading `NoReachableEndpoints`
    /// and never an unbounded loop. (Before the ride-out change this stopped at
    /// exactly `MAX_LEADER_REDIRECTS + 1` dials; the cap is now per-pass and the
    /// deadline is the whole-call ceiling — see
    /// docs/superpowers/specs/2026-05-25-client-ride-out-election-design.md.)
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn endless_redirect_chain_is_bounded_by_overall_deadline() {
        use tsoracle_proto::v1::tso_service_server::{TsoService, TsoServiceServer};

        struct AlwaysRedirecting {
            calls: Arc<AtomicUsize>,
        }

        #[tonic::async_trait]
        impl TsoService for AlwaysRedirecting {
            async fn get_ts(
                &self,
                _request: tonic::Request<tsoracle_proto::v1::GetTsRequest>,
            ) -> Result<tonic::Response<tsoracle_proto::v1::GetTsResponse>, tonic::Status>
            {
                let n = self.calls.fetch_add(1, Ordering::SeqCst);
                Err(make_status_with_hint(tsoracle_proto::v1::LeaderHint {
                    leader_endpoint: Some(format!("redirect-{}:1", n + 1)),
                    leader_epoch: None,
                }))
            }
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback listener");
        let addr = listener.local_addr().expect("local_addr");
        let incoming = tonic::transport::server::TcpIncoming::from(listener);
        let calls = Arc::new(AtomicUsize::new(0));
        let server_calls = calls.clone();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(TsoServiceServer::new(AlwaysRedirecting {
                    calls: server_calls,
                }))
                .serve_with_incoming(incoming)
                .await
                .ok();
        });

        let connector: Arc<crate::transport::ChannelConnector> =
            Arc::new(move |_endpoint: &str| {
                let target = format!("http://{addr}");
                Box::pin(async move {
                    tonic::transport::Endpoint::from_shared(target)
                        .map_err(ClientError::from)?
                        .connect()
                        .await
                        .map_err(ClientError::from)
                })
            });

        let policy = RetryPolicy {
            max_attempts: 2,
            per_attempt_deadline: Duration::from_millis(200),
            overall_deadline: Duration::from_millis(400),
            base_backoff: Duration::from_millis(2),
            leader_ttl: Duration::from_secs(30),
        };
        let pool = ChannelPool::new(vec!["redirect-0:1".into()], Some(connector), false, policy);

        let start = std::time::Instant::now();
        let err = issue_rpc(&pool, 1)
            .await
            .expect_err("an endless redirect chain must surface an error, not a timestamp");
        let elapsed = start.elapsed();
        // Whether the per-pass redirect cap or the overall deadline trips first
        // is a runner-speed race (each churn redirect is a fresh connect), so
        // the surfaced status is either the cap's synthesized FAILED_PRECONDITION
        // or the deadline edge's DEADLINE_EXCEEDED. Both are bounded, meaningful,
        // and reachable-but-churning — the security property is that it is NOT
        // the misleading `NoReachableEndpoints` fallback and NOT a timestamp.
        match err {
            ClientError::Rpc(status) => assert!(
                matches!(
                    status.code(),
                    tonic::Code::FailedPrecondition | tonic::Code::DeadlineExceeded
                ),
                "a churning chain must surface a bounded retryable status \
                 (cap -> FailedPrecondition or deadline -> DeadlineExceeded), got {:?}",
                status.code(),
            ),
            other => panic!(
                "expected a bounded ClientError::Rpc, not {other:?} \
                 (e.g. the misleading NoReachableEndpoints)"
            ),
        }
        assert!(
            elapsed < Duration::from_secs(3),
            "the loop must be bounded by overall_deadline (~400ms), not spin; took {elapsed:?}",
        );
    }

    /// The cap must not bite a legitimate failover: a redirect chain of
    /// exactly `MAX_LEADER_REDIRECTS` hops must still reach the leader. This
    /// pins the off-by-one — the cap permits CAP pivots and rejects only the
    /// (CAP + 1)th — so a real cluster that needs the full budget to settle is
    /// never cut short one hop early.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn redirect_chain_at_cap_still_reaches_leader() {
        use tsoracle_proto::v1::tso_service_server::{TsoService, TsoServiceServer};

        // Redirect for the first MAX_LEADER_REDIRECTS calls, then answer with a
        // timestamp — a chain that consumes the whole redirect budget and no
        // more.
        struct RedirectsExactlyToCap {
            calls: Arc<AtomicUsize>,
        }

        #[tonic::async_trait]
        impl TsoService for RedirectsExactlyToCap {
            async fn get_ts(
                &self,
                _request: tonic::Request<tsoracle_proto::v1::GetTsRequest>,
            ) -> Result<tonic::Response<tsoracle_proto::v1::GetTsResponse>, tonic::Status>
            {
                let n = self.calls.fetch_add(1, Ordering::SeqCst);
                if n < MAX_LEADER_REDIRECTS as usize {
                    Err(make_status_with_hint(tsoracle_proto::v1::LeaderHint {
                        leader_endpoint: Some(format!("redirect-{}:1", n + 1)),
                        leader_epoch: None,
                    }))
                } else {
                    Ok(tonic::Response::new(tsoracle_proto::v1::GetTsResponse {
                        physical_ms: 1,
                        logical_start: 0,
                        count: 1,
                        epoch_hi: 0,
                        epoch_lo: 0,
                    }))
                }
            }
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback listener");
        let addr = listener.local_addr().expect("local_addr");
        let incoming = tonic::transport::server::TcpIncoming::from(listener);
        let calls = Arc::new(AtomicUsize::new(0));
        let server_calls = calls.clone();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(TsoServiceServer::new(RedirectsExactlyToCap {
                    calls: server_calls,
                }))
                .serve_with_incoming(incoming)
                .await
                .ok();
        });

        let connector: Arc<crate::transport::ChannelConnector> =
            Arc::new(move |_endpoint: &str| {
                let target = format!("http://{addr}");
                Box::pin(async move {
                    tonic::transport::Endpoint::from_shared(target)
                        .map_err(ClientError::from)?
                        .connect()
                        .await
                        .map_err(ClientError::from)
                })
            });

        let policy = RetryPolicy {
            max_attempts: 2,
            per_attempt_deadline: Duration::from_secs(2),
            overall_deadline: Duration::from_secs(10),
            base_backoff: Duration::ZERO,
            leader_ttl: Duration::from_secs(30),
        };
        let pool = ChannelPool::new(vec!["redirect-0:1".into()], Some(connector), false, policy);

        let range = issue_rpc(&pool, 1)
            .await
            .expect("a chain of exactly MAX_LEADER_REDIRECTS hops must reach the leader");
        assert_eq!(
            range.count(),
            1,
            "the leader returned exactly one timestamp"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            MAX_LEADER_REDIRECTS as usize + 1,
            "the loop must dial through all MAX_LEADER_REDIRECTS redirects to the leader",
        );
    }

    /// A follower that answers FAILED_PRECONDITION with no hint (no leader yet)
    /// for the first few calls, then serves once the election settles, must be
    /// ridden out within `overall_deadline` — not surfaced as an error after a
    /// single pass. Regression for the leader-election ride-out.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rides_out_election_until_leader_appears() {
        use tsoracle_proto::v1::tso_service_server::{TsoService, TsoServiceServer};

        const NO_LEADER_REPLIES: usize = 4;

        struct ElectingThenServing {
            calls: Arc<AtomicUsize>,
        }

        #[tonic::async_trait]
        impl TsoService for ElectingThenServing {
            async fn get_ts(
                &self,
                _request: tonic::Request<tsoracle_proto::v1::GetTsRequest>,
            ) -> Result<tonic::Response<tsoracle_proto::v1::GetTsResponse>, tonic::Status>
            {
                let n = self.calls.fetch_add(1, Ordering::SeqCst);
                if n < NO_LEADER_REPLIES {
                    Err(tonic::Status::failed_precondition("no leader yet"))
                } else {
                    Ok(tonic::Response::new(tsoracle_proto::v1::GetTsResponse {
                        physical_ms: 1,
                        logical_start: 0,
                        count: 1,
                        epoch_hi: 0,
                        epoch_lo: 0,
                    }))
                }
            }
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback listener");
        let addr = listener.local_addr().expect("local_addr");
        let incoming = tonic::transport::server::TcpIncoming::from(listener);
        let calls = Arc::new(AtomicUsize::new(0));
        let server_calls = calls.clone();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(TsoServiceServer::new(ElectingThenServing {
                    calls: server_calls,
                }))
                .serve_with_incoming(incoming)
                .await
                .ok();
        });

        let connector: Arc<crate::transport::ChannelConnector> =
            Arc::new(move |_endpoint: &str| {
                let target = format!("http://{addr}");
                Box::pin(async move {
                    tonic::transport::Endpoint::from_shared(target)
                        .map_err(ClientError::from)?
                        .connect()
                        .await
                        .map_err(ClientError::from)
                })
            });

        let policy = RetryPolicy {
            max_attempts: 2,
            per_attempt_deadline: Duration::from_secs(2),
            overall_deadline: Duration::from_secs(10),
            base_backoff: Duration::from_millis(5),
            leader_ttl: Duration::from_secs(30),
        };
        let pool = ChannelPool::new(vec!["follower:1".into()], Some(connector), false, policy);

        let range = issue_rpc(&pool, 1)
            .await
            .expect("the client must ride out the election and reach the leader");
        assert_eq!(
            range.count(),
            1,
            "the leader returned exactly one timestamp"
        );
        assert!(
            calls.load(Ordering::SeqCst) > NO_LEADER_REPLIES,
            "the loop must re-poll through every no-leader reply to the serving call",
        );
    }

    /// A dead pool (all connections refused → transport Err, never a server
    /// NOT_LEADER) must NOT ride out: no election signal is set, so the loop
    /// fails after a single pass, well under `overall_deadline`. Pins
    /// "dead != electing".
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dead_pool_does_not_ride_out() {
        let policy = RetryPolicy {
            max_attempts: 2,
            per_attempt_deadline: Duration::from_millis(100),
            overall_deadline: Duration::from_secs(10),
            base_backoff: Duration::ZERO,
            leader_ttl: Duration::from_secs(30),
        };
        let pool = ChannelPool::new(
            vec!["http://127.0.0.1:1".into(), "http://127.0.0.1:2".into()],
            None,
            false,
            policy,
        );
        let start = std::time::Instant::now();
        let result = issue_rpc(&pool, 1).await;
        let elapsed = start.elapsed();
        assert!(result.is_err(), "all-dead pool must surface an error");
        assert!(
            elapsed < Duration::from_secs(2),
            "a dead pool must fail fast, not ride out the full overall_deadline; took {elapsed:?}",
        );
    }

    /// A NOT_LEADER carrying a *malformed* trailer is a deterministic peer bug,
    /// not an election: it classifies as `HintRejected`, sets no signal, and the
    /// single-endpoint loop fails fast rather than riding out to the deadline.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn malformed_hint_does_not_ride_out() {
        use tsoracle_proto::v1::tso_service_server::{TsoService, TsoServiceServer};

        struct AlwaysMalformed;
        #[tonic::async_trait]
        impl TsoService for AlwaysMalformed {
            async fn get_ts(
                &self,
                _request: tonic::Request<tsoracle_proto::v1::GetTsRequest>,
            ) -> Result<tonic::Response<tsoracle_proto::v1::GetTsResponse>, tonic::Status>
            {
                let mut status = tonic::Status::failed_precondition("not leader");
                let key = tonic::metadata::MetadataKey::from_bytes(
                    tsoracle_proto::v1::LEADER_HINT_TRAILER_KEY.as_bytes(),
                )
                .expect("trailer key is ascii");
                let garbage: &[u8] = &[0x0a, 0x05, b'h', b'i'];
                status.metadata_mut().insert_bin(
                    key,
                    tonic::metadata::BinaryMetadataValue::from_bytes(garbage),
                );
                Err(status)
            }
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback listener");
        let addr = listener.local_addr().expect("local_addr");
        let incoming = tonic::transport::server::TcpIncoming::from(listener);
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(TsoServiceServer::new(AlwaysMalformed))
                .serve_with_incoming(incoming)
                .await
                .ok();
        });

        let connector: Arc<crate::transport::ChannelConnector> =
            Arc::new(move |_endpoint: &str| {
                let target = format!("http://{addr}");
                Box::pin(async move {
                    tonic::transport::Endpoint::from_shared(target)
                        .map_err(ClientError::from)?
                        .connect()
                        .await
                        .map_err(ClientError::from)
                })
            });

        let policy = RetryPolicy {
            max_attempts: 2,
            per_attempt_deadline: Duration::from_secs(2),
            overall_deadline: Duration::from_secs(10),
            base_backoff: Duration::ZERO,
            leader_ttl: Duration::from_secs(30),
        };
        let pool = ChannelPool::new(vec!["peer:1".into()], Some(connector), false, policy);

        let start = std::time::Instant::now();
        let result = issue_rpc(&pool, 1).await;
        let elapsed = start.elapsed();
        assert!(
            result.is_err(),
            "malformed-hint NOT_LEADER must surface an error"
        );
        assert!(
            elapsed < Duration::from_secs(2),
            "a deterministic malformed-hint rejection must not ride out; took {elapsed:?}",
        );
    }

    /// A ride-out that exhausts the deadline must surface the real NOT_LEADER
    /// status, never `NoReachableEndpoints` — `last_err` persists across passes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn exhausted_ride_out_surfaces_not_leader() {
        use tsoracle_proto::v1::tso_service_server::{TsoService, TsoServiceServer};

        struct AlwaysElecting;
        #[tonic::async_trait]
        impl TsoService for AlwaysElecting {
            async fn get_ts(
                &self,
                _request: tonic::Request<tsoracle_proto::v1::GetTsRequest>,
            ) -> Result<tonic::Response<tsoracle_proto::v1::GetTsResponse>, tonic::Status>
            {
                Err(tonic::Status::failed_precondition("no leader yet"))
            }
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback listener");
        let addr = listener.local_addr().expect("local_addr");
        let incoming = tonic::transport::server::TcpIncoming::from(listener);
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(TsoServiceServer::new(AlwaysElecting))
                .serve_with_incoming(incoming)
                .await
                .ok();
        });

        let connector: Arc<crate::transport::ChannelConnector> =
            Arc::new(move |_endpoint: &str| {
                let target = format!("http://{addr}");
                Box::pin(async move {
                    tonic::transport::Endpoint::from_shared(target)
                        .map_err(ClientError::from)?
                        .connect()
                        .await
                        .map_err(ClientError::from)
                })
            });

        let policy = RetryPolicy {
            max_attempts: 2,
            per_attempt_deadline: Duration::from_millis(200),
            overall_deadline: Duration::from_millis(300),
            base_backoff: Duration::from_millis(5),
            leader_ttl: Duration::from_secs(30),
        };
        let pool = ChannelPool::new(vec!["follower:1".into()], Some(connector), false, policy);

        let err = issue_rpc(&pool, 1)
            .await
            .expect_err("a cluster that never elects must surface an error at the deadline");
        match err {
            ClientError::Rpc(status) => assert_eq!(
                status.code(),
                tonic::Code::FailedPrecondition,
                "must surface the NOT_LEADER status, not NoReachableEndpoints",
            ),
            other => panic!("expected ClientError::Rpc(FailedPrecondition), got {other:?}"),
        }
    }

    /// Regression test for the "randomized rotation can skip the only live
    /// peer" finding. Six configured endpoints, the only reachable one at
    /// index 0, five simulated-dead peers after it. The rotation cursor is
    /// pinned to offset 1 so the cold-cache worklist is
    /// `[dead-1, dead-2, dead-3, dead-4, dead-5, live]` — the live endpoint
    /// sits behind exactly `max_attempts = 5` failing peers.
    ///
    /// Before the fix, the loop burned its whole `max_attempts` budget on the
    /// five dead peers and broke before ever dialing `live`, so a reachable
    /// leader was reported unreachable purely because of where the random
    /// rotation offset landed. The fix floors the failed-attempt budget at the
    /// initial worklist length, so a cold-cache sweep always dials every known
    /// endpoint at least once: `live` must now be dialed and the request must
    /// succeed even though `max_attempts` is smaller than the endpoint count.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rotation_offset_cannot_strand_the_only_live_endpoint() {
        use tsoracle_proto::v1::tso_service_server::{TsoService, TsoServiceServer};

        struct LiveLeader;

        #[tonic::async_trait]
        impl TsoService for LiveLeader {
            async fn get_ts(
                &self,
                _request: tonic::Request<tsoracle_proto::v1::GetTsRequest>,
            ) -> Result<tonic::Response<tsoracle_proto::v1::GetTsResponse>, tonic::Status>
            {
                Ok(tonic::Response::new(tsoracle_proto::v1::GetTsResponse {
                    physical_ms: 1,
                    logical_start: 0,
                    count: 1,
                    epoch_hi: 0,
                    epoch_lo: 0,
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
                .add_service(TsoServiceServer::new(LiveLeader))
                .serve_with_incoming(incoming)
                .await
                .ok();
        });

        // A connector that only the single live endpoint can dial; every other
        // configured endpoint fails fast with a transport-class error, exactly
        // as an unreachable peer would. Records the dial order so the test can
        // prove the live endpoint was reached.
        let dialed: Arc<std::sync::Mutex<Vec<String>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let dialed_for_connector = dialed.clone();
        let connector: Arc<crate::transport::ChannelConnector> = Arc::new(move |endpoint: &str| {
            dialed_for_connector
                .lock()
                .unwrap()
                .push(endpoint.to_string());
            let is_live = endpoint.contains("live");
            Box::pin(async move {
                if is_live {
                    tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
                        .map_err(ClientError::from)?
                        .connect()
                        .await
                        .map_err(ClientError::from)
                } else {
                    Err(ClientError::Rpc(tonic::Status::unavailable(
                        "simulated dead endpoint",
                    )))
                }
            })
        });

        let endpoints = vec![
            "live:1".to_string(),
            "dead-1:1".to_string(),
            "dead-2:1".to_string(),
            "dead-3:1".to_string(),
            "dead-4:1".to_string(),
            "dead-5:1".to_string(),
        ];

        // max_attempts (5) is deliberately smaller than the endpoint count (6),
        // and the rotation offset parks `live` last — the exact shape that used
        // to strand it. base_backoff = 0 keeps the five dead dials instant; the
        // deadlines are generous because the connector fails synchronously.
        let policy = RetryPolicy {
            max_attempts: 5,
            per_attempt_deadline: Duration::from_secs(2),
            overall_deadline: Duration::from_secs(10),
            base_backoff: Duration::ZERO,
            leader_ttl: Duration::from_secs(30),
        };
        let pool = ChannelPool::new(endpoints, Some(connector), false, policy);
        pool.pin_rotation_for_test(1);

        let range = issue_rpc(&pool, 1).await.expect(
            "a reachable configured endpoint must be dialed even when it \
                     sits past max_attempts in the rotated worklist",
        );
        assert_eq!(range.count(), 1, "the live leader returned one timestamp");
        assert!(
            dialed
                .lock()
                .unwrap()
                .iter()
                .any(|endpoint| endpoint.contains("live")),
            "the live endpoint must be dialed; dialed = {:?}",
            dialed.lock().unwrap(),
        );
    }

    /// A connect that outlasts the per-attempt deadline is cut off by the outer
    /// `tokio::time::timeout`, surfacing as `DeadlineExceeded` from the
    /// connect-phase timeout arm — a path distinct from a connector that
    /// *returns* an error (which the unreachable-endpoint tests already cover).
    /// Virtual time (`start_paused`) fires the 100 ms budget deterministically
    /// with no real wall-clock wait.
    #[tokio::test(start_paused = true)]
    async fn connect_exceeding_per_attempt_deadline_surfaces_deadline_exceeded() {
        enable_tracing();
        // Parks far longer than the per-attempt deadline, so the outer timeout
        // wins and the connector future is cancelled before it ever resolves.
        let connector: Arc<crate::transport::ChannelConnector> = Arc::new(|_endpoint: &str| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_secs(3600)).await;
                unreachable!("the per-attempt timeout must cancel this connect")
            })
        });
        let policy = RetryPolicy {
            max_attempts: 1,
            per_attempt_deadline: Duration::from_millis(100),
            overall_deadline: Duration::from_secs(10),
            base_backoff: Duration::ZERO,
            leader_ttl: Duration::from_secs(30),
        };
        let pool = ChannelPool::new(vec!["slow:1".into()], Some(connector), false, policy);
        match issue_rpc(&pool, 1).await {
            Err(ClientError::Rpc(status)) => assert_eq!(
                status.code(),
                tonic::Code::DeadlineExceeded,
                "a connect that overran the per-attempt budget must surface DeadlineExceeded",
            ),
            other => panic!("expected an RPC DeadlineExceeded error, got {other:?}"),
        }
    }

    /// When the `overall_deadline` elapses *between* attempts — here consumed by
    /// a backoff sleep the clamp pins to the remaining budget — the loop must
    /// stop before starting the next attempt, even though endpoints are still
    /// queued and `max_attempts` has headroom. Virtual time trips the deadline
    /// deterministically. Covers the `budget.next_attempt() == None` break,
    /// which the fast-failing unreachable tests skip past.
    #[tokio::test(start_paused = true)]
    async fn overall_deadline_stops_loop_between_attempts() {
        enable_tracing();
        // Each dial fails transport-class (Unavailable) with no delay, which
        // triggers a backoff. `base_backoff` dwarfs the overall budget, so the
        // clamp pins the first sleep to the ~100 ms remaining; the next
        // `next_attempt()` then sees the deadline reached with `b`/`c` unvisited.
        let connector: Arc<crate::transport::ChannelConnector> = Arc::new(|_endpoint: &str| {
            Box::pin(async move { Err(ClientError::Rpc(tonic::Status::unavailable("dead"))) })
        });
        let policy = RetryPolicy {
            max_attempts: 10,
            per_attempt_deadline: Duration::from_millis(50),
            overall_deadline: Duration::from_millis(100),
            base_backoff: Duration::from_secs(60),
            leader_ttl: Duration::from_secs(30),
        };
        let pool = ChannelPool::new(
            vec!["a:1".into(), "b:1".into(), "c:1".into()],
            Some(connector),
            false,
            policy,
        );
        match issue_rpc(&pool, 1).await {
            Err(ClientError::Rpc(status)) => assert_eq!(
                status.code(),
                tonic::Code::Unavailable,
                "the loop must surface the last transport error once the overall \
                 deadline cuts it short",
            ),
            other => panic!("expected the last transport error, got {other:?}"),
        }
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
