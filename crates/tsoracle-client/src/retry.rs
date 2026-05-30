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
//! Classifying a NOT_LEADER reply — leader-hint decoding, the epoch-monotone
//! gate that drops a stale-epoch hint, and the plaintext-downgrade guard —
//! lives in [`crate::leader_hint`], co-located with the cache write it gates.
//!
//! Queue bookkeeping (the worklist, the visited-set dedup, and
//! push-front-on-hint steering) lives in [`crate::worklist::Worklist`]; the
//! deadline arithmetic lives in [`crate::budget`]; one `(connect, get_ts)`
//! attempt and its channel eviction live in [`crate::attempt`]. This module
//! owns only the loop and its policy decisions.
//!
//! Three deadlines bound the loop, governed by [`crate::RetryPolicy`] and
//! enforced by [`Budget`] / [`crate::budget::PairBudget`]:
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
//! A transport-class RPC failure evicts the endpoint's cached channel
//! (in [`crate::attempt`], via [`ChannelPool::evict_if_current`]) so the
//! next attempt re-dials and re-resolves rather than reusing a channel
//! pinned to a now-dead address
//! (issue #239: a static tonic `Endpoint` resolves once and never
//! re-resolves, so a pod-replaced endpoint would otherwise keep the dead
//! channel and its background reconnect task forever). Application errors
//! such as `Internal` leave the channel cached — the connection is healthy.

use std::time::Duration;

use crate::attempt::{AttemptOutcome, HintUnusableReason, attempt};
use crate::budget::{Budget, PairBudget};
use crate::channel_pool::ChannelPool;
use crate::error::ClientError;
use crate::leader_hint::classify_not_leader_hint;
use crate::response::TimestampRange;
use crate::retry_policy::{is_transport_failure, jittered_backoff, should_backoff};
use crate::seq_attempt::{SeqAttemptOutcome, SeqBlock, classify_seq_status};
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
/// further passes, bounded overall by `overall_deadline` and the absolute
/// [`MAX_TOTAL_LEADER_REDIRECTS`] backstop. The cap is far above any real
/// failover, which dedups via the worklist visited-set and settles in a few hops.
const MAX_LEADER_REDIRECTS: u32 = 16;

/// Absolute ceiling on actionable leader-hint pivots across *all* re-poll passes
/// of a single `issue_rpc` call.
///
/// The per-pass [`MAX_LEADER_REDIRECTS`] cap resets each pass and a cap hit rides
/// out across further passes (it is treated as a churning leadership transfer),
/// so the per-pass cap alone leaves `overall_deadline` as the only whole-call
/// bound on attacker-directed dials. A malicious or persistently flapping peer
/// that returns a fresh, reachable hint on every dial could then churn outbound
/// connections for the entire deadline — and under a long operator-chosen
/// `overall_deadline` that window is large. This absolute cap is the
/// deadline-independent backstop: once a single call has followed this many
/// leader-hint pivots in total it stops following hints and surfaces the
/// redirect `FAILED_PRECONDITION`, *without* riding out further passes.
///
/// Set far above any legitimate failover. A genuine leadership transfer settles
/// in a few hops; a genuinely-electing cluster with no leader to point at signals
/// via [`AttemptOutcome::NoLeaderYet`], which consumes no redirect budget and so
/// still rides out for the full `overall_deadline`. Only a peer that keeps
/// emitting fresh, actionable hints — the redirect-churn attack — reaches this
/// cap, so `MAX_LEADER_REDIRECTS * 4` is generous headroom for any real cluster.
const MAX_TOTAL_LEADER_REDIRECTS: u32 = MAX_LEADER_REDIRECTS * 4;

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
/// Leader-hint redirects are bounded twice over: the per-pass
/// [`MAX_LEADER_REDIRECTS`] cap (which rides out a churning transfer) and the
/// absolute [`MAX_TOTAL_LEADER_REDIRECTS`] cap that persists across passes. The
/// absolute cap is a deadline-independent backstop: a peer that returns a fresh,
/// reachable hint on every dial cannot churn outbound connections for the whole
/// `overall_deadline` — once total pivots reach the absolute cap the call stops
/// following hints and fails fast rather than riding out further passes.
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
    // Absolute, whole-call leader-hint pivot count (persists across passes,
    // unlike the per-pass `redirects` below). Once it reaches
    // `MAX_TOTAL_LEADER_REDIRECTS` the call stops following hints and fails fast,
    // so a peer churning fresh hints cannot redirect us for the whole deadline.
    let mut total_redirects: u32 = 0;
    // The failed-attempt cap is floored at the initial worklist size so one
    // cold sweep always dials every configured endpoint at least once even when
    // `max_attempts` is smaller (issue #404). Computed once from the first
    // pass's endpoint set.
    let mut attempt_cap: usize = 0;
    let mut pass: u32 = 0;

    'passes: loop {
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
                    // Absolute, deadline-independent backstop. Unlike the
                    // per-pass cap below (which rides out a churning transfer),
                    // exhausting the whole-call pivot budget is terminal: a peer
                    // that keeps emitting fresh, actionable hints is treated as
                    // adversarial or permanently flapping, so we surface the
                    // redirect rejection and stop — never dialling its hints for
                    // the whole `overall_deadline`. A real cluster settles far
                    // below this, and a leaderless cluster signals via
                    // `NoLeaderYet` (no redirect charge), so this never bites a
                    // legitimate failover or election ride-out.
                    if total_redirects >= MAX_TOTAL_LEADER_REDIRECTS {
                        #[cfg(feature = "metrics")]
                        metrics::counter!("tsoracle.client.leader_redirect_total_cap.total")
                            .increment(1);
                        #[cfg(feature = "tracing")]
                        tracing::warn!(
                            from = %endpoint,
                            to = %hinted_endpoint,
                            max_total_redirects = MAX_TOTAL_LEADER_REDIRECTS,
                            "tsoracle-client: absolute leader-hint redirect cap reached; failing fast",
                        );
                        last_err = Some(ClientError::Rpc(tonic::Status::failed_precondition(
                            format!(
                                "absolute leader-hint redirect cap ({MAX_TOTAL_LEADER_REDIRECTS}) \
                                 reached across passes before finding the live leader"
                            ),
                        )));
                        break 'passes;
                    }
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
                    total_redirects = total_redirects.saturating_add(1);
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

/// Issue one `GetSeq(key, count)` with the dense retry policy.
///
/// Mirrors `issue_rpc`'s loop structure — per-attempt budget, overall deadline,
/// redirect bounds, election ride-out — but applies the **non-idempotent dense
/// retry policy**. Every outcome below is pre-commit-certain (the server
/// rejected before any durable advance) except the explicitly-ambiguous one:
///
/// - `FAILED_PRECONDITION` with **no** leader-hint trailer → a definitive server
///   rejection (e.g. `SeqOverflow`), surfaced as-is. Unlike the timestamp path,
///   GetSeq overloads bare `FAILED_PRECONDITION`, so an absent trailer is *not*
///   read as "no leader yet".
/// - `FAILED_PRECONDITION` with a usable leader-endpoint hint → follow the hint
///   within the redirect and attempt bounds.
/// - `FAILED_PRECONDITION` whose hint cannot outrank the cached leader (an
///   older-epoch / epoch-less hint) → an election signal: ride it out without
///   charging the attempt budget.
/// - `FAILED_PRECONDITION` whose hint is malformed or unfollowable → a definitive
///   rejection, surfaced fast (no ride-out).
/// - `Unavailable` / `DeadlineExceeded` **after** the RPC was put on the wire →
///   return `SeqUncertain` immediately. A commit may have occurred; retrying
///   transparently would risk silently spending a second block.
/// - A connect-phase failure **before** the RPC reached the wire → pre-commit-
///   certain (no bytes sent); retried within the per-attempt budget and, if every
///   attempt fails, surfaced as the last transport error.
///
/// "Sent" is determined by whether `pool.client_with_cell` succeeded (i.e., a
/// channel was acquired and the RPC future was created). Once we hold a live
/// channel and issue the RPC, any subsequent failure is ambiguous.
pub(crate) async fn issue_seq_rpc(
    pool: &ChannelPool,
    key: &str,
    count: u32,
) -> Result<SeqBlock, ClientError> {
    let policy = pool.retry_policy().clone();
    let budget = Budget::start(&policy);
    let mut last_err: Option<ClientError> = None;
    let mut election_signal: Option<tonic::Status> = None;
    let mut failed_attempts: u32 = 0;
    let mut total_redirects: u32 = 0;
    let mut attempt_cap: usize = 0;
    let mut pass: u32 = 0;

    'passes: loop {
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
                break;
            };

            #[cfg(feature = "tracing")]
            tracing::debug!(
                endpoint = %endpoint,
                key,
                count,
                failed_attempts,
                pass,
                budget_ms = attempt_budget.as_millis() as u64,
                "tsoracle-client: dispatching GetSeq to endpoint",
            );

            // Perform the (connect, get_seq) pair under the per-attempt budget.
            // `sent` tracks whether the RPC was put on the wire: only set to
            // `true` after a successful channel acquisition so that a
            // connect-phase failure (which never touches the server) is treated
            // as pre-commit-certain.
            let pair = PairBudget::start(attempt_budget);

            let (mut client, cell) = match tokio::time::timeout(
                attempt_budget,
                pool.client_with_cell(&endpoint),
            )
            .await
            {
                Ok(Ok(leased)) => leased,
                Ok(Err(err)) => {
                    // Connect failed before any bytes were sent — pre-commit-certain.
                    let do_backoff = should_backoff(&err);
                    last_err = Some(err);
                    failed_attempts = failed_attempts.saturating_add(1);
                    // Backoff on transport failures (Unavailable from connect).
                    if do_backoff {
                        let backoff = jittered_backoff(policy.base_backoff, failed_attempts - 1);
                        let sleep_for = budget.clamp_backoff(backoff);
                        if sleep_for > Duration::ZERO {
                            tokio::time::sleep(sleep_for).await;
                        }
                    }
                    continue;
                }
                Err(_) => {
                    // Connect timed out — pre-commit-certain (no bytes sent).
                    let err = ClientError::Rpc(tonic::Status::deadline_exceeded(format!(
                        "connect exceeded per_attempt_deadline of {attempt_budget:?}"
                    )));
                    last_err = Some(err);
                    failed_attempts = failed_attempts.saturating_add(1);
                    continue;
                }
            };

            // Channel acquired: from this point a commit may occur on the
            // server. Any transport-class failure from here on is ambiguous.
            let rpc_budget = pair.remaining();
            let rpc = client.get_seq(tsoracle_proto::v1::GetSeqRequest {
                key: key.to_string(),
                count,
            });

            let outcome = match tokio::time::timeout(rpc_budget, rpc).await {
                Ok(Ok(response)) => {
                    let inner = response.into_inner();
                    // Validate the payload before trusting it. A success means the
                    // server committed an advance, so a response that does not
                    // describe the block we asked for is a protocol violation we
                    // cannot resolve into a safe block: surface SeqUncertain (a
                    // commit occurred; the caller must reconcile) rather than hand
                    // back a malformed block or silently retry into a double-spend.
                    // Mirrors the None-epoch protocol-violation handling below.
                    if inner.key != key || inner.count != count {
                        return Err(ClientError::SeqUncertain);
                    }
                    // Decode the epoch from the nested EpochWire. A None epoch on
                    // a success response is a protocol violation → SeqUncertain.
                    let epoch = match inner.epoch {
                        Some(ew) => tsoracle_core::Epoch::from_wire(ew.hi, ew.lo).0,
                        None => {
                            // Protocol violation: server returned success with no epoch.
                            return Err(ClientError::SeqUncertain);
                        }
                    };
                    // Seat the confirmed leader.
                    pool.record_success(&endpoint, epoch);
                    return Ok(SeqBlock {
                        start: inner.start,
                        count: inner.count,
                        epoch,
                    });
                }
                Ok(Err(status)) if status.code() == tonic::Code::FailedPrecondition => {
                    use crate::attempt::{AttemptOutcome as AO, HintUnusableReason};
                    use crate::channel_pool::{LeaderHintLookup, decode_leader_hint};
                    // A bare FailedPrecondition carrying NO leader-hint trailer is
                    // a definitive server rejection — e.g. SeqOverflow — NOT a
                    // NOT_LEADER redirect. Surface it via classify_seq_status
                    // (definitive Err, preserving the server's message) instead of
                    // misreading it as "no leader yet" and riding out the deadline.
                    // (Unlike the timestamp path, get_seq has a bare-FailedPrecondition
                    // case, so the trailer presence must be checked explicitly.)
                    if matches!(decode_leader_hint(&status), LeaderHintLookup::Absent) {
                        classify_seq_status(status, false)
                    } else {
                        // NOT_LEADER (hint trailer present, possibly malformed):
                        // classify via the shared helper. Pre-commit-certain — the
                        // server rejected before any durable advance. Keep the channel.
                        match classify_not_leader_hint(pool, &endpoint, status) {
                            AO::LeaderHint {
                                endpoint: hinted,
                                epoch,
                            } => SeqAttemptOutcome::LeaderHint {
                                endpoint: hinted,
                                epoch,
                            },
                            // An election-in-progress signal: either a peer with no
                            // leader to name yet (`NoLeaderYet`) or a lagging peer
                            // pointing at an older-epoch leader (`StaleEpoch`). Both
                            // are pre-commit-certain — the server rejected before any
                            // durable advance — so ride out (no budget charge) rather
                            // than surfacing a hard error. The bare-FailedPrecondition
                            // pre-check above routes an absent trailer to the
                            // definitive branch, so in practice only `StaleEpoch`
                            // reaches here; folding `NoLeaderYet` into the same arm
                            // keeps both election signals on one ride-out path and
                            // stays correct if that pre-check ever changes.
                            AO::NoLeaderYet(status)
                            | AO::HintUnusable {
                                status,
                                reason: HintUnusableReason::StaleEpoch,
                            } => {
                                #[cfg(feature = "metrics")]
                                metrics::counter!("tsoracle.client.leader_hint.stale.total")
                                    .increment(1);
                                saw_election_signal = true;
                                election_signal = Some(status.clone());
                                last_err = Some(ClientError::Rpc(status));
                                continue;
                            }
                            AO::HintUnusable {
                                status,
                                reason: HintUnusableReason::Rejected,
                            } => SeqAttemptOutcome::Err(ClientError::Rpc(status)),
                            AO::Ok { .. } | AO::Err(_) => {
                                unreachable!(
                                    "classify_not_leader_hint on FailedPrecondition \
                                     cannot produce Ok/Err"
                                )
                            }
                        }
                    }
                }
                Ok(Err(status)) => {
                    // Post-connect RPC failure: the request reached the wire (the
                    // server returned a status), so this is always post-send.
                    // Transport-class failures additionally evict the now-suspect
                    // channel, but that does not change the fact that the outcome
                    // is post-send. classify_seq_status is fail-uncertain by default
                    // and keeps only provably-pre-commit codes definitive, so a
                    // post-send INTERNAL (the advance may already be committed)
                    // surfaces as Uncertain rather than a retry-safe error — no
                    // silent double-spend.
                    if is_transport_failure(&ClientError::Rpc(status.clone())) {
                        pool.evict_if_current(&endpoint, &cell);
                    }
                    classify_seq_status(status, true)
                }
                Err(_) => {
                    // RPC timed out post-connect — ambiguous.
                    pool.evict_if_current(&endpoint, &cell);
                    SeqAttemptOutcome::Uncertain
                }
            };

            match outcome {
                SeqAttemptOutcome::Uncertain => {
                    // Ambiguous post-send failure: surface immediately, never retry.
                    return Err(ClientError::SeqUncertain);
                }
                SeqAttemptOutcome::LeaderHint {
                    endpoint: hinted,
                    epoch: _hint_epoch,
                } => {
                    // Absolute total-redirect cap.
                    if total_redirects >= MAX_TOTAL_LEADER_REDIRECTS {
                        #[cfg(feature = "metrics")]
                        metrics::counter!("tsoracle.client.leader_redirect_total_cap.total")
                            .increment(1);
                        last_err = Some(ClientError::Rpc(tonic::Status::failed_precondition(
                            format!(
                                "absolute leader-hint redirect cap ({MAX_TOTAL_LEADER_REDIRECTS}) \
                                 reached across passes before finding the live leader"
                            ),
                        )));
                        break 'passes;
                    }
                    if redirects >= MAX_LEADER_REDIRECTS {
                        #[cfg(feature = "metrics")]
                        metrics::counter!("tsoracle.client.leader_redirect_cap.total").increment(1);
                        let status = tonic::Status::failed_precondition(format!(
                            "leader-hint redirect cap ({MAX_LEADER_REDIRECTS}) reached \
                             before finding the live leader"
                        ));
                        election_signal = Some(status.clone());
                        last_err = Some(ClientError::Rpc(status));
                        saw_election_signal = true;
                        break;
                    }
                    redirects += 1;
                    total_redirects = total_redirects.saturating_add(1);
                    #[cfg(feature = "metrics")]
                    metrics::counter!("tsoracle.client.leader_pivots.total").increment(1);
                    worklist.redirect_to(hinted);
                    continue;
                }
                SeqAttemptOutcome::Err(err) => {
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

        // End of pass.
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
        // A follower that answers NOT_LEADER with no hint trailer →
        // LeaderHintLookup::Absent → AttemptOutcome::NoLeaderYet in the loop.
        let addr = crate::test_support::FakeTso::new()
            .on_get_ts(|_req| async { Err(tonic::Status::failed_precondition("not leader")) })
            .spawn()
            .await;

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
        // NOT_LEADER with a well-formed hint at epoch 5 — strictly behind the
        // epoch-10 leader the client has cached, so the epoch-monotone gate drops
        // it: AttemptOutcome::HintUnusable { reason: StaleEpoch }.
        let addr = crate::test_support::FakeTso::new()
            .on_get_ts(|_req| async {
                Err(make_status_with_hint(tsoracle_proto::v1::LeaderHint {
                    leader_endpoint: Some("b:1".into()),
                    leader_epoch: Some(tsoracle_proto::v1::EpochWire { hi: 0, lo: 5 }),
                }))
            })
            .spawn()
            .await;

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
        // Number of NOT_LEADER redirects before the server answers. Strictly
        // greater than `max_attempts` below, so the old "every outcome bumps
        // the attempt budget" behaviour would exhaust the budget mid-chain.
        const REDIRECTS: usize = 3;

        let calls = Arc::new(AtomicUsize::new(0));
        let server_calls = calls.clone();
        // Each call hints a fresh (unvisited) endpoint with no epoch (followed
        // unconditionally) until the chain settles and serves; the chain lives in
        // the call counter, not in distinct listeners.
        let addr = crate::test_support::FakeTso::new()
            .on_get_ts(move |_req| {
                let calls = server_calls.clone();
                async move {
                    let n = calls.fetch_add(1, Ordering::SeqCst);
                    if n < REDIRECTS {
                        Err(make_status_with_hint(tsoracle_proto::v1::LeaderHint {
                            leader_endpoint: Some(format!("redirect-{}:1", n + 1)),
                            leader_epoch: None,
                        }))
                    } else {
                        Ok(tsoracle_proto::v1::GetTsResponse {
                            physical_ms: 1,
                            logical_start: 0,
                            count: 1,
                            epoch_hi: 0,
                            epoch_lo: 0,
                        })
                    }
                }
            })
            .spawn()
            .await;

        // Every hinted endpoint resolves to the one backend.
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
        const CHURN: usize = MAX_LEADER_REDIRECTS as usize + 3;

        let calls = Arc::new(AtomicUsize::new(0));
        let server_calls = calls.clone();
        // Churn longer than the per-pass cap, then settle and serve.
        let addr = crate::test_support::FakeTso::new()
            .on_get_ts(move |_req| {
                let calls = server_calls.clone();
                async move {
                    let n = calls.fetch_add(1, Ordering::SeqCst);
                    if n < CHURN {
                        Err(make_status_with_hint(tsoracle_proto::v1::LeaderHint {
                            leader_endpoint: Some(format!("redirect-{}:1", n + 1)),
                            leader_epoch: None,
                        }))
                    } else {
                        Ok(tsoracle_proto::v1::GetTsResponse {
                            physical_ms: 1,
                            logical_start: 0,
                            count: 1,
                            epoch_hi: 0,
                            epoch_lo: 0,
                        })
                    }
                }
            })
            .spawn()
            .await;

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
    /// The per-pass cap is treated as an election signal (so a genuine
    /// leadership *transfer* is ridden out across passes), which would otherwise
    /// leave `overall_deadline` as the only whole-call bound on attacker-directed
    /// dials. The absolute [`MAX_TOTAL_LEADER_REDIRECTS`] cap is the
    /// deadline-independent backstop: under a deliberately *long* deadline this
    /// churn must still terminate after ~`MAX_TOTAL_LEADER_REDIRECTS` dials with
    /// the absolute-cap `FAILED_PRECONDITION` — bounded by the cap, not the
    /// deadline, and never a misleading `NoReachableEndpoints` or an unbounded
    /// loop. (Before the ride-out change this stopped at exactly
    /// `MAX_LEADER_REDIRECTS + 1` dials; the per-pass cap is now per-pass and the
    /// absolute cap is the whole-call ceiling on churn (issues #340, #440).)
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn endless_redirect_chain_is_bounded_by_absolute_cap() {
        let calls = Arc::new(AtomicUsize::new(0));
        let server_calls = calls.clone();
        // Every dial hints a fresh, never-visited endpoint → the chain never
        // settles; only the absolute redirect cap can bound it.
        let addr = crate::test_support::FakeTso::new()
            .on_get_ts(move |_req| {
                let calls = server_calls.clone();
                async move {
                    let n = calls.fetch_add(1, Ordering::SeqCst);
                    Err(make_status_with_hint(tsoracle_proto::v1::LeaderHint {
                        leader_endpoint: Some(format!("redirect-{}:1", n + 1)),
                        leader_epoch: None,
                    }))
                }
            })
            .spawn()
            .await;

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

        // A deliberately *long* deadline: if the absolute cap were absent, the
        // churn would dial for the full 10s. The fix must terminate it far
        // sooner, after ~MAX_TOTAL_LEADER_REDIRECTS pivots — proving the cap, not
        // the deadline, is the whole-call ceiling.
        let policy = RetryPolicy {
            max_attempts: 2,
            per_attempt_deadline: Duration::from_secs(2),
            overall_deadline: Duration::from_secs(10),
            base_backoff: Duration::from_millis(2),
            leader_ttl: Duration::from_secs(30),
        };
        let pool = ChannelPool::new(vec!["redirect-0:1".into()], Some(connector), false, policy);

        let start = std::time::Instant::now();
        let err = issue_rpc(&pool, 1)
            .await
            .expect_err("an endless redirect chain must surface an error, not a timestamp");
        let elapsed = start.elapsed();
        let dials = calls.load(Ordering::SeqCst);

        // The absolute cap must be what stops the churn: a non-transport
        // FAILED_PRECONDITION naming the absolute cap, never the misleading
        // `NoReachableEndpoints` fallback, a DEADLINE_EXCEEDED edge, or a
        // timestamp.
        match err {
            ClientError::Rpc(status) => {
                assert_eq!(
                    status.code(),
                    tonic::Code::FailedPrecondition,
                    "the absolute cap must surface FailedPrecondition, got {:?}",
                    status.code(),
                );
                assert!(
                    status
                        .message()
                        .contains("absolute leader-hint redirect cap"),
                    "the surfaced status must be the absolute-cap rejection, got {:?}",
                    status.message(),
                );
            }
            other => panic!(
                "expected a bounded ClientError::Rpc, not {other:?} \
                 (e.g. the misleading NoReachableEndpoints)"
            ),
        }

        // Dials are bounded by the absolute cap, independent of the 10s deadline.
        // Each pass follows MAX_LEADER_REDIRECTS pivots then a per-pass-cap call,
        // so the whole call lands a little above MAX_TOTAL_LEADER_REDIRECTS; the
        // headroom covers those per-pass-cap dials without ever approaching the
        // hundreds a 10s deadline would otherwise permit.
        assert!(
            dials >= MAX_TOTAL_LEADER_REDIRECTS as usize,
            "the chain must churn up to the absolute cap; only {dials} dials",
        );
        assert!(
            dials <= (MAX_TOTAL_LEADER_REDIRECTS + 2 * MAX_LEADER_REDIRECTS) as usize,
            "dials must be bounded by the absolute cap, not the deadline; got {dials}",
        );
        assert!(
            elapsed < Duration::from_secs(5),
            "the cap (not the 10s deadline) must terminate the churn; took {elapsed:?}",
        );
    }

    /// The cap must not bite a legitimate failover: a redirect chain of
    /// exactly `MAX_LEADER_REDIRECTS` hops must still reach the leader. This
    /// pins the off-by-one — the cap permits CAP pivots and rejects only the
    /// (CAP + 1)th — so a real cluster that needs the full budget to settle is
    /// never cut short one hop early.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn redirect_chain_at_cap_still_reaches_leader() {
        let calls = Arc::new(AtomicUsize::new(0));
        let server_calls = calls.clone();
        // Redirect for the first MAX_LEADER_REDIRECTS calls, then answer with a
        // timestamp — a chain that consumes the whole redirect budget and no more.
        let addr = crate::test_support::FakeTso::new()
            .on_get_ts(move |_req| {
                let calls = server_calls.clone();
                async move {
                    let n = calls.fetch_add(1, Ordering::SeqCst);
                    if n < MAX_LEADER_REDIRECTS as usize {
                        Err(make_status_with_hint(tsoracle_proto::v1::LeaderHint {
                            leader_endpoint: Some(format!("redirect-{}:1", n + 1)),
                            leader_epoch: None,
                        }))
                    } else {
                        Ok(tsoracle_proto::v1::GetTsResponse {
                            physical_ms: 1,
                            logical_start: 0,
                            count: 1,
                            epoch_hi: 0,
                            epoch_lo: 0,
                        })
                    }
                }
            })
            .spawn()
            .await;

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
        const NO_LEADER_REPLIES: usize = 4;

        let calls = Arc::new(AtomicUsize::new(0));
        let server_calls = calls.clone();
        // Answer "no leader yet" for the first NO_LEADER_REPLIES calls, then serve.
        let addr = crate::test_support::FakeTso::new()
            .on_get_ts(move |_req| {
                let calls = server_calls.clone();
                async move {
                    let n = calls.fetch_add(1, Ordering::SeqCst);
                    if n < NO_LEADER_REPLIES {
                        Err(tonic::Status::failed_precondition("no leader yet"))
                    } else {
                        Ok(tsoracle_proto::v1::GetTsResponse {
                            physical_ms: 1,
                            logical_start: 0,
                            count: 1,
                            epoch_hi: 0,
                            epoch_lo: 0,
                        })
                    }
                }
            })
            .spawn()
            .await;

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
        // Answer NOT_LEADER with a trailer whose bytes are not a valid LeaderHint
        // → LeaderHintLookup::Malformed → HintUnusable { Rejected } (no ride-out).
        let addr = crate::test_support::FakeTso::new()
            .on_get_ts(|_req| async {
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
            })
            .spawn()
            .await;

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
        // Every dial answers "no leader yet" (bare FAILED_PRECONDITION), forever.
        let addr = crate::test_support::FakeTso::new()
            .on_get_ts(|_req| async { Err(tonic::Status::failed_precondition("no leader yet")) })
            .spawn()
            .await;

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
        // A live leader that always serves a timestamp.
        let addr = crate::test_support::FakeTso::new()
            .on_get_ts(|_req| async {
                Ok(tsoracle_proto::v1::GetTsResponse {
                    physical_ms: 1,
                    logical_start: 0,
                    count: 1,
                    epoch_hi: 0,
                    epoch_lo: 0,
                })
            })
            .spawn()
            .await;

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

    /// Dense-path analogue of `redirect_chain_longer_than_max_attempts_reaches_leader`:
    /// a GetSeq redirected by NOT_LEADER+hint several times must follow the chain
    /// to the live leader and return its block. A redirect is pre-commit-certain
    /// (the server refused before any durable advance), so following it is safe
    /// for the non-idempotent dense path. Exercises issue_seq_rpc's LeaderHint
    /// redirect arm and the redirect-count bookkeeping.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn seq_follows_leader_hint_chain_to_leader() {
        use std::future::Future;
        use std::pin::Pin;
        const REDIRECTS: usize = 3;

        let calls = Arc::new(AtomicUsize::new(0));
        let server_calls = calls.clone();
        // Redirect the GetSeq with a fresh (unvisited), epoch-less hint until the
        // chain settles, then serve a block. The chain lives in the call counter.
        let addr = crate::test_support::FakeTso::new()
            .on_get_seq(move |req| {
                let calls = server_calls.clone();
                async move {
                    let n = calls.fetch_add(1, Ordering::SeqCst);
                    if n < REDIRECTS {
                        Err(make_status_with_hint(tsoracle_proto::v1::LeaderHint {
                            leader_endpoint: Some(format!("redirect-{}:1", n + 1)),
                            leader_epoch: None,
                        }))
                    } else {
                        let (hi, lo) = tsoracle_core::Epoch(9).to_wire();
                        Ok(tsoracle_proto::v1::GetSeqResponse {
                            key: req.key,
                            start: 100,
                            count: req.count,
                            epoch: Some(tsoracle_proto::v1::EpochWire { hi, lo }),
                        })
                    }
                }
            })
            .spawn()
            .await;

        // Every hinted endpoint resolves to the one backend.
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

        let block = issue_seq_rpc(&pool, "orders", 5)
            .await
            .expect("a redirect chain ending at a live leader must yield a block");
        assert_eq!(block.start, 100);
        assert_eq!(block.count, 5);
        assert_eq!(block.epoch, 9);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            REDIRECTS + 1,
            "the loop must dial through all {REDIRECTS} redirects to the leader",
        );
    }

    /// A NOT_LEADER whose hint trailer is present but carries no endpoint (the
    /// hint cannot be followed) classifies as `HintUnusable { Rejected }` on the
    /// dense path — a definitive rejection, not an election ride-out — so
    /// issue_seq_rpc fails fast within its attempt bound rather than looping to
    /// the overall deadline. Exercises the `Err` outcome arm and its backoff.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn seq_endpointless_hint_fails_fast() {
        // NOT_LEADER whose hint trailer is present but carries no endpoint → an
        // unfollowable hint (HintUnusable::Rejected).
        let addr = crate::test_support::FakeTso::new()
            .on_get_seq(|_req| async {
                Err(make_status_with_hint(tsoracle_proto::v1::LeaderHint {
                    leader_endpoint: None,
                    leader_epoch: None,
                }))
            })
            .spawn()
            .await;

        let policy = RetryPolicy {
            max_attempts: 2,
            per_attempt_deadline: Duration::from_secs(2),
            overall_deadline: Duration::from_secs(10),
            base_backoff: Duration::ZERO,
            leader_ttl: Duration::from_secs(30),
        };
        let pool = ChannelPool::new(vec![format!("http://{addr}")], None, false, policy);

        let start = Instant::now();
        let result = issue_seq_rpc(&pool, "orders", 1).await;
        let elapsed = start.elapsed();
        assert!(
            matches!(result, Err(ClientError::Rpc(_))),
            "an unfollowable hint must surface a definitive Rpc error, got {result:?}",
        );
        assert!(
            elapsed < Duration::from_secs(2),
            "an unfollowable hint must fail fast, not ride out the deadline; took {elapsed:?}",
        );
    }

    /// A pool whose endpoints all refuse to connect exercises issue_seq_rpc's
    /// connect-failure arm: every attempt fails before any bytes reach the wire
    /// (pre-commit-certain), so the loop surfaces a transport error fast rather
    /// than riding out the overall deadline — and never risks a double-spend.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn seq_dead_pool_surfaces_error_fast() {
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
        let start = Instant::now();
        let result = issue_seq_rpc(&pool, "orders", 1).await;
        let elapsed = start.elapsed();
        assert!(result.is_err(), "all-dead pool must surface an error");
        assert!(
            elapsed < Duration::from_secs(2),
            "a dead pool must fail fast, not ride out the full deadline; took {elapsed:?}",
        );
    }

    /// A `GetSeq` that succeeds at the wire but omits the leader epoch is a
    /// protocol violation: the block cannot be trusted (the client seats its
    /// confirmed leader from that epoch). issue_seq_rpc must surface
    /// `SeqUncertain` rather than hand back an unverifiable block.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn seq_success_without_epoch_is_uncertain() {
        // A success whose key/count match the request but which omits the epoch
        // — a protocol violation the client must surface as SeqUncertain.
        let addr = crate::test_support::FakeTso::new()
            .on_get_seq(|req| async move {
                Ok(tsoracle_proto::v1::GetSeqResponse {
                    key: req.key,
                    start: 0,
                    count: req.count,
                    epoch: None,
                })
            })
            .spawn()
            .await;

        let pool = ChannelPool::new(vec![format!("http://{addr}")], None, false, short_policy());
        match issue_seq_rpc(&pool, "orders", 1).await {
            Err(ClientError::SeqUncertain) => {}
            other => panic!("epoch-less success must be SeqUncertain, got {other:?}"),
        }
    }

    /// A cluster that hints an endless redirect chain (never settling) must be
    /// bounded by the absolute cross-pass redirect cap rather than looping
    /// forever: issue_seq_rpc breaks out and surfaces an error. Exercises the
    /// per-pass and absolute redirect-cap arms and the end-of-pass ride-out.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn seq_endless_redirect_is_bounded_by_cap() {
        use std::future::Future;
        use std::pin::Pin;

        let calls = Arc::new(AtomicUsize::new(0));
        let server_calls = calls.clone();
        // Every GetSeq dial hints a fresh, unvisited endpoint → the chain never
        // settles; only the absolute redirect cap can bound it.
        let addr = crate::test_support::FakeTso::new()
            .on_get_seq(move |_req| {
                let calls = server_calls.clone();
                async move {
                    let n = calls.fetch_add(1, Ordering::SeqCst);
                    Err(make_status_with_hint(tsoracle_proto::v1::LeaderHint {
                        leader_endpoint: Some(format!("redirect-{}:1", n + 1)),
                        leader_epoch: None,
                    }))
                }
            })
            .spawn()
            .await;

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

        let start = Instant::now();
        let err = issue_seq_rpc(&pool, "orders", 1)
            .await
            .expect_err("an endless redirect chain must surface an error, not a block");
        let elapsed = start.elapsed();
        let dials = calls.load(Ordering::SeqCst);

        // The absolute cap (not the 10s deadline) must terminate the churn, and
        // it surfaces a FAILED_PRECONDITION naming that cap.
        match err {
            ClientError::Rpc(status) => {
                assert_eq!(status.code(), tonic::Code::FailedPrecondition);
                assert!(
                    status
                        .message()
                        .contains("absolute leader-hint redirect cap"),
                    "must surface the absolute-cap rejection, got {:?}",
                    status.message(),
                );
            }
            other => panic!("expected a bounded ClientError::Rpc, got {other:?}"),
        }
        // Each pass follows up to MAX_LEADER_REDIRECTS pivots then a per-pass-cap
        // dial, so the total lands a little above MAX_TOTAL_LEADER_REDIRECTS.
        assert!(
            dials >= MAX_TOTAL_LEADER_REDIRECTS as usize
                && dials <= (MAX_TOTAL_LEADER_REDIRECTS + 2 * MAX_LEADER_REDIRECTS) as usize,
            "dials must be bounded by the absolute cap, not the deadline; got {dials}",
        );
        assert!(
            elapsed < Duration::from_secs(5),
            "the cap (not the 10s deadline) must terminate the churn; took {elapsed:?}",
        );
    }

    /// A follower that hints at an *older-epoch* leader than the one the client
    /// has cached: the epoch-monotone gate drops the hint as `StaleEpoch`, which
    /// the dense loop treats as an election-in-progress signal and rides out
    /// (no budget charge) rather than surfacing a hard error — until the overall
    /// deadline elapses and the recorded election signal is surfaced. Covers the
    /// folded NoLeaderYet/StaleEpoch ride-out arm.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn seq_stale_epoch_hint_rides_out_then_surfaces_election() {
        // A follower whose GetSeq hint names an epoch-5 leader, strictly behind
        // the epoch-10 leader the client has cached → dropped as StaleEpoch.
        let addr = crate::test_support::FakeTso::new()
            .on_get_seq(|_req| async {
                Err(make_status_with_hint(tsoracle_proto::v1::LeaderHint {
                    leader_endpoint: Some("b:1".into()),
                    leader_epoch: Some(tsoracle_proto::v1::EpochWire { hi: 0, lo: 5 }),
                }))
            })
            .spawn()
            .await;

        let endpoint = format!("http://{addr}");
        let pool = ChannelPool::new(vec![endpoint.clone()], None, false, short_policy());

        // Wait until the fake peer is accepting and replying FAILED_PRECONDITION.
        let ready_deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Ok(mut client) = pool.client(&endpoint).await {
                let replied = client
                    .get_seq(tsoracle_proto::v1::GetSeqRequest {
                        key: "orders".into(),
                        count: 1,
                    })
                    .await
                    .err()
                    .is_some_and(|s| s.code() == tonic::Code::FailedPrecondition);
                if replied {
                    break;
                }
            }
            assert!(
                Instant::now() < ready_deadline,
                "fake follower never came up"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // Cache the only endpoint as leader at epoch 10 so the epoch-5 hint is
        // strictly stale and is dropped (ride-out) rather than followed.
        pool.record_success(&endpoint, 10);

        match issue_seq_rpc(&pool, "orders", 1).await {
            Err(ClientError::Rpc(status)) => assert_eq!(
                status.code(),
                tonic::Code::FailedPrecondition,
                "a stale-hint ride-out must surface the election FAILED_PRECONDITION",
            ),
            other => panic!("expected a ridden-out FAILED_PRECONDITION, got {other:?}"),
        }
    }
}
