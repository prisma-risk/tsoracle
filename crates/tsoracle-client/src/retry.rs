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

use crate::attempt::{AttemptOutcome, HintUnusableReason, attempt};
use crate::budget::Budget;
use crate::channel_pool::{ChannelPool, LeaderHintLookup, decode_leader_hint};
use crate::error::ClientError;
use crate::response::TimestampRange;
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
/// (`AttemptOutcome::NoLeaderYet`), a `HintUnusable { reason: StaleEpoch }`, or the
/// `MAX_LEADER_REDIRECTS` cap being hit (a churning leadership transfer). A pass
/// that only hit transport failures or deterministic hint rejections
/// (`HintUnusable { reason: Rejected }`) does not re-poll — a genuinely-unreachable pool still fails
/// fast. `failed_attempts` and the last error persist across passes (so
/// `max_attempts` keeps its whole-call meaning and the surfaced error is the
/// real NOT_LEADER / transport status, never `NoReachableEndpoints`); the
/// worklist and the per-pass redirect budget reset each pass.
///
/// The election signal is recorded separately and is *sticky*: if a reachable
/// server ever reported an in-progress election, that NOT_LEADER status is
/// surfaced in preference to a later transport-class straggler (e.g. a
/// `DeadlineExceeded` on the final attempt whose budget the overall deadline
/// squeezed to near zero). See [`surface_error`] for the full precedence.
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
    // The most recent in-progress-election signal — an absent-hint NOT_LEADER,
    // a stale leader hint, or the redirect cap being hit. Tracked separately
    // from `last_err` because it is *sticky*: a later transport-class straggler
    // (typically a `DeadlineExceeded` on the final attempt, whose budget the
    // overall deadline has squeezed to near zero) must not bury the cluster's
    // own "no leader yet" diagnosis. See `surface_error` for the precedence.
    let mut election_signal: Option<tonic::Status> = None;
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
                        let status = tonic::Status::failed_precondition(format!(
                            "leader-hint redirect cap ({MAX_LEADER_REDIRECTS}) reached \
                             before finding the live leader"
                        ));
                        election_signal = Some(status.clone());
                        last_err = Some(ClientError::Rpc(status));
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
                    election_signal = Some(status.clone());
                    last_err = Some(ClientError::Rpc(status));
                    continue;
                }
                AttemptOutcome::HintUnusable { status, reason } => {
                    if matches!(reason, HintUnusableReason::StaleEpoch) {
                        #[cfg(feature = "metrics")]
                        metrics::counter!("tsoracle.client.leader_hint.stale.total").increment(1);
                        // A lagging peer pointed at an older-epoch leader —
                        // transient cluster flux. Treat as an election signal so
                        // the worklist-empty handler rides it out, and record it
                        // as a sticky `election_signal` so a later budget-squeezed
                        // transport straggler can't bury it (see `surface_error`);
                        // do not charge the budget (issue #340). A deterministic
                        // `Rejected` (malformed trailer / TLS-downgrade drop) sets
                        // no signal, so a genuinely bad peer still fails fast.
                        saw_election_signal = true;
                        election_signal = Some(status.clone());
                    }
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
    Err(surface_error(election_signal, last_err))
}

/// Choose the error `issue_rpc` surfaces when a call ends without a timestamp.
///
/// Precedence, highest first:
/// 1. A *non-transport* `last_err` — a deterministic server rejection
///    (a malformed-hint `HintUnusable { reason: Rejected }`, a genuine
///    `Internal`, …). The server
///    spoke definitively about this request, so report it verbatim.
/// 2. The sticky `election_signal`, if one was ever recorded. A reachable
///    server told us "no leader yet" / pointed at a stale leader; that is the
///    most actionable diagnosis a caller can get, and it outranks a
///    transport-class straggler. The motivating case: earlier passes record
///    the election, then the overall deadline squeezes the final attempt's
///    budget to near zero so it times out with `DeadlineExceeded` — surfacing
///    that timeout would bury the real, actionable cluster state under an
///    artifact of our own budget.
/// 3. The transport-class `last_err` (every attempt failed at the wire and no
///    server ever reported an election).
/// 4. `NoReachableEndpoints` — the worklist emptied without a single attempt.
fn surface_error(
    election_signal: Option<tonic::Status>,
    last_err: Option<ClientError>,
) -> ClientError {
    match last_err {
        // A definitive, non-transport server status wins outright.
        Some(err) if !is_transport_failure(&err) => err,
        // Otherwise prefer the cluster's own election diagnosis, falling back
        // to the transport error, then to "nothing was reachable".
        last_err => election_signal
            .map(ClientError::Rpc)
            .or(last_err)
            .unwrap_or(ClientError::NoReachableEndpoints),
    }
}

/// Decide what `issue_rpc` should do with a `FAILED_PRECONDITION` reply.
///
/// Pulled out of `attempt` so the decision tree — hint decoding,
/// plaintext-downgrade rejection, and the epoch-monotone gate — is
/// unit-testable without standing up a real gRPC peer. The production
/// path goes through here too, so the integration and unit tests
/// exercise the same code.
pub(crate) fn classify_not_leader_hint(
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
            // malformed / guard-dropped cases below, which stay `HintUnusable { reason: Rejected }`
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
                AttemptOutcome::HintUnusable {
                    status,
                    reason: HintUnusableReason::StaleEpoch,
                }
            }
        }
        None => AttemptOutcome::HintUnusable {
            status,
            reason: HintUnusableReason::Rejected,
        },
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
    use crate::test_support::{enable_tracing, make_status_with_hint, short_policy};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::Instant;

    /// The bug behind the flaky `exhausted_ride_out_surfaces_not_leader`
    /// integration test, pinned deterministically at the decision point with
    /// no sockets or timing involved. A ride-out records the cluster's
    /// "no leader yet" `FAILED_PRECONDITION`, then the final attempt — its
    /// budget squeezed to near zero by the overall deadline — times out with
    /// `DeadlineExceeded`. The sticky election signal must win: surfacing the
    /// transport timeout would bury the actionable NOT_LEADER state under an
    /// artifact of our own budget.
    #[test]
    fn sticky_election_signal_outranks_a_transport_straggler() {
        let election = tonic::Status::failed_precondition("no leader yet");
        let timeout = ClientError::Rpc(tonic::Status::deadline_exceeded("rpc budget exhausted"));

        let surfaced = surface_error(Some(election), Some(timeout));
        match surfaced {
            ClientError::Rpc(status) => assert_eq!(
                status.code(),
                tonic::Code::FailedPrecondition,
                "election signal must outrank the transport timeout"
            ),
            other => panic!("expected the election FAILED_PRECONDITION, got {other:?}"),
        }
    }

    /// Symmetric guard: a *deterministic* (non-transport) server rejection is
    /// definitive about this request and must be surfaced verbatim, even when
    /// an election was seen earlier. Stickiness is scoped to transport-class
    /// stragglers, not to a `HintUnusable { reason: Rejected }` / `Internal`
    /// the server returned.
    #[test]
    fn deterministic_rejection_outranks_a_stale_election_signal() {
        let election = tonic::Status::failed_precondition("no leader yet");
        let rejection = ClientError::Rpc(tonic::Status::internal("malformed leader hint"));

        let surfaced = surface_error(Some(election), Some(rejection));
        match surfaced {
            ClientError::Rpc(status) => assert_eq!(
                status.code(),
                tonic::Code::Internal,
                "a non-transport rejection must win over the election signal"
            ),
            other => panic!("expected the Internal rejection, got {other:?}"),
        }
    }

    /// With no election ever recorded, a transport-class `last_err` is the
    /// surface (the all-unreachable path), and an empty worklist with nothing
    /// recorded falls back to `NoReachableEndpoints`.
    #[test]
    fn no_election_signal_falls_back_to_last_err_then_no_reachable_endpoints() {
        let timeout = ClientError::Rpc(tonic::Status::deadline_exceeded("budget exhausted"));
        match surface_error(None, Some(timeout)) {
            ClientError::Rpc(status) => {
                assert_eq!(status.code(), tonic::Code::DeadlineExceeded)
            }
            other => panic!("expected the transport timeout, got {other:?}"),
        }

        assert!(
            matches!(surface_error(None, None), ClientError::NoReachableEndpoints),
            "no signal and no attempt must fall back to NoReachableEndpoints"
        );
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

    /// A FAILED_PRECONDITION with NO leader-hint trailer (a follower that does
    /// not yet know a leader — the election signature) classifies as
    /// `NoLeaderYet`, distinct from a malformed/guard-dropped hint
    /// (`HintUnusable { reason: Rejected }`). Pins the split that lets the retry loop ride out an
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
    /// route to `HintUnusable { reason: Rejected }`, not panic, and must bump the
    /// decode-failures metric. Covers `LeaderHintLookup::Malformed`.
    #[test]
    fn classify_malformed_hint_returns_rejected() {
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
            AttemptOutcome::HintUnusable {
                reason: HintUnusableReason::Rejected,
                ..
            } => {}
            other => panic!("expected HintUnusable {{ reason: Rejected }}, got {other:?}"),
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
    /// `HintUnusable { reason: StaleEpoch }` arm can record it as `last_err`; the arm
    /// continues without mutating the cache.
    #[test]
    fn classify_stale_epoch_hint_returns_stale_epoch() {
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
            AttemptOutcome::HintUnusable {
                status,
                reason: HintUnusableReason::StaleEpoch,
            } => {
                assert_eq!(status.code(), tonic::Code::FailedPrecondition);
            }
            other => panic!("expected HintUnusable {{ reason: StaleEpoch }}, got {other:?}"),
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
    /// cannot downgrade the transport. The outcome is `HintUnusable { reason: Rejected }`
    /// (not `HintUnusable { reason: StaleEpoch }`) because the cache is still valid; the
    /// hint just wasn't usable.
    #[test]
    fn classify_plaintext_hint_under_tls_returns_rejected() {
        enable_tracing();
        let pool = ChannelPool::new(vec!["a:1".into()], None, true, RetryPolicy::default());
        let status = make_status_with_hint(tsoracle_proto::v1::LeaderHint {
            leader_endpoint: Some("http://attacker:1".into()),
            leader_epoch: Some(tsoracle_proto::v1::EpochWire { hi: 0, lo: 7 }),
        });
        match classify_not_leader_hint(&pool, "a:1", status) {
            AttemptOutcome::HintUnusable {
                reason: HintUnusableReason::Rejected,
                ..
            } => {}
            other => panic!("expected HintUnusable {{ reason: Rejected }}, got {other:?}"),
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
    /// `attempt → classify_not_leader_hint → HintUnusable { reason: StaleEpoch }` path — and the
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
                // epoch-monotone gate drops it: AttemptOutcome::HintUnusable { reason: StaleEpoch }.
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
                 (NoReachableEndpoints means the HintUnusable {{ reason: StaleEpoch }} arm dropped last_err)"
            ),
        }
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
    /// not an election: it classifies as `HintUnusable { reason: Rejected }`, sets no signal, and the
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
    /// status, never `NoReachableEndpoints` — and never a transport
    /// `DeadlineExceeded` from the final budget-squeezed attempt. The election
    /// signal recorded by earlier passes is sticky (see `surface_error`); the
    /// warm-up below guarantees at least one pass reaches the peer and records
    /// it, so this end-to-end assertion is deterministic rather than racing the
    /// overall deadline against a cold dial on a slow runner.
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

        // Warm the cached channel and confirm the peer is replying
        // FAILED_PRECONDITION before timing the ride-out, so `issue_rpc`'s first
        // attempt reaches the RPC layer (records the election signal) instead of
        // racing the dial on a slow runner.
        let ready_deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Ok(mut client) = pool.client("follower:1").await {
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
                "fake AlwaysElecting peer never became ready"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

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
}
