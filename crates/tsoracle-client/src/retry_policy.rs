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

//! Retry and deadline policy for client RPCs.
//!
//! `per_attempt_deadline` bounds a single endpoint dial + RPC; the same
//! value is pushed down to `tonic::transport::Endpoint::connect_timeout`
//! and `Endpoint::timeout` so the transport layer also surfaces
//! `DeadlineExceeded` rather than parking on an OS-default TCP timeout.
//! `overall_deadline` bounds the whole `issue_rpc` call across all
//! candidate endpoints — the retry loop short-circuits once it elapses,
//! so a flapping leader cannot stretch a single caller's `request.await`
//! across many RTTs. `max_attempts` caps the number of *failed* attempts
//! (dialed endpoints that returned an error), floored at the initial worklist
//! size so a single cold-cache sweep always dials every known endpoint at
//! least once; it does not charge leader-hint redirects (issue #340).
//! Pathological leader-hint redirect chains are bounded separately — by the
//! worklist visited-set, the `overall_deadline`, the per-pass
//! `MAX_LEADER_REDIRECTS` cap, and the absolute, cross-pass
//! `MAX_TOTAL_LEADER_REDIRECTS` backstop in `crate::retry`.
//! `base_backoff` is
//! the unit for the jittered exponential backoff applied between
//! attempts when the previous attempt returned `Unavailable` or
//! `DeadlineExceeded` — see `crate::retry::issue_rpc`. `leader_ttl`
//! caps how long a cached leader endpoint may be retained without a
//! successful RPC against it; past the TTL the cache is treated as
//! empty and the worklist falls back to the configured endpoint order.

use std::time::Duration;

/// Retry and deadline knobs for client RPCs.
///
/// Every field has a default chosen for steady-state availability; see
/// [`RetryPolicy::default`] for the values. Pass a customised policy via
/// [`crate::ClientBuilder::retry_policy`] when the defaults don't match
/// your deployment (for example, longer `per_attempt_deadline` for
/// cross-region servers, or tighter `overall_deadline` for
/// latency-sensitive paths).
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum number of *failed* attempts `issue_rpc` will make before
    /// returning the last error — that is, endpoints dialed that returned
    /// an error, not total loop iterations.
    ///
    /// The budget is floored at the size of the initial worklist (the cached
    /// leader, if fresh, plus the configured endpoints), so a single
    /// cold-cache sweep always dials every endpoint at least once even when
    /// `max_attempts` is smaller than the endpoint count. Without this floor,
    /// the randomly seeded rotation offset in `ChannelPool::iter_round_robin`
    /// could leave the only reachable endpoint untried whenever a pool has
    /// more configured endpoints than `max_attempts`. The `overall_deadline`
    /// still bounds the worst case, so a pool full of dead peers cannot dial
    /// forever.
    ///
    /// Leader-hint redirects are not charged against this budget (they are
    /// known discovery progress, bounded instead by the per-endpoint
    /// visited-set, the `overall_deadline`, the per-pass `MAX_LEADER_REDIRECTS`
    /// cap, and the absolute, cross-pass `MAX_TOTAL_LEADER_REDIRECTS` backstop in
    /// `crate::retry`), so a legitimate failover redirect chain can be longer
    /// than `max_attempts` and still reach the live leader (issue #340).
    pub max_attempts: usize,
    /// Wall-clock deadline applied to each `(connect, get_ts)` pair.
    /// Pushed down to `Endpoint::connect_timeout` / `Endpoint::timeout`
    /// for the built-in transport paths, and enforced by an outer
    /// `tokio::time::timeout` for user-supplied
    /// [`crate::ClientBuilder::channel_connector`] closures.
    pub per_attempt_deadline: Duration,
    /// Wall-clock deadline for the entire `issue_rpc` call. Once
    /// elapsed, the retry loop returns the last observed error rather
    /// than starting another attempt — even if `max_attempts` and the
    /// worklist still have headroom.
    pub overall_deadline: Duration,
    /// Base unit for the jittered exponential backoff applied between
    /// attempts whose last error was `Unavailable` or `DeadlineExceeded`.
    /// The actual sleep is `rand_uniform(0, base_backoff * 2^attempt)`,
    /// capped internally at [`RetryPolicy::MAX_BACKOFF`] so a long
    /// `base_backoff` plus a high attempt count cannot consume the
    /// overall deadline in a single sleep.
    pub base_backoff: Duration,
    /// Freshness bound on the cached leader endpoint. The cache is
    /// touched on every successful RPC against the cached leader, so a
    /// busy steady-state leader is retained indefinitely. The TTL only
    /// bites when the leader falls quiet — past it, the next RPC
    /// re-evaluates the configured endpoint list rather than pinning to
    /// a possibly-dead endpoint that has not been re-validated within
    /// the window.
    pub leader_ttl: Duration,
}

impl RetryPolicy {
    /// Upper bound on a single backoff sleep, regardless of
    /// `base_backoff` and attempt count. Prevents the jittered
    /// exponential from sleeping past the overall deadline in one step.
    pub const MAX_BACKOFF: Duration = Duration::from_secs(5);
}

impl Default for RetryPolicy {
    /// `max_attempts = 5`, `per_attempt_deadline = 2s`,
    /// `overall_deadline = 10s`, `base_backoff = 50ms`,
    /// `leader_ttl = 30s`. Five attempts at the per-attempt cap plus
    /// modest backoff fit inside the overall deadline for the common
    /// case; the loop returns `NoReachableEndpoints` (or the last
    /// status) once exhausted. The 30-second TTL is roughly 3× the
    /// overall deadline — long enough that a steady-state leader is
    /// retained across normal request gaps, short enough that a leader
    /// that fell silent at startup is re-evaluated before too many
    /// callers fail-fast on the cached pin.
    fn default() -> Self {
        RetryPolicy {
            max_attempts: 5,
            per_attempt_deadline: Duration::from_secs(2),
            overall_deadline: Duration::from_secs(10),
            base_backoff: Duration::from_millis(50),
            leader_ttl: Duration::from_secs(30),
        }
    }
}

/// Classify an error as a transport-layer problem with the connection
/// itself: the peer was unreachable (`Unavailable`), the attempt timed out
/// (`DeadlineExceeded` — including a half-open connection that black-holes
/// until the per-attempt deadline rather than failing fast), or the
/// transport failed to establish (`Transport`). Deterministic failures
/// (`Internal`, `Unauthenticated`, a user-supplied connector's own error,
/// `DriverGone`, etc.) are not transport problems.
///
/// This single predicate drives two policies that happen to share the same
/// trigger today — backing off before the next attempt
/// ([`should_backoff`]) and evicting the cached channel after the RPC fails
/// (`crate::attempt::attempt`, issue #239). Keeping one definition means the
/// two cannot silently drift apart; if they ever need to diverge, that must
/// be a deliberate edit here.
pub(crate) fn is_transport_failure(error: &crate::error::ClientError) -> bool {
    use crate::error::ClientError;
    match error {
        ClientError::Rpc(status) => matches!(
            status.code(),
            tonic::Code::Unavailable | tonic::Code::DeadlineExceeded
        ),
        // `TransportFanout` is the fanned-out copy of a `Transport` failure
        // (a coalesced sibling waiter's view), so it carries the same
        // transport-failure semantics.
        ClientError::Transport(_) | ClientError::TransportFanout(_) => true,
        ClientError::NoReachableEndpoints
        | ClientError::InvalidEndpoint(_)
        | ClientError::InvalidCount(_)
        | ClientError::Connector(_)
        | ClientError::DriverGone => false,
    }
}

/// Decide whether to back off before the next attempt based on the last
/// error. Backoff applies to exactly the transport-failure class (see
/// [`is_transport_failure`]): those are the "service unavailable"-style
/// failures where pausing before retrying de-correlates a thundering herd.
/// Other failures are deterministic — the next endpoint is tried
/// immediately without sleep.
pub(crate) fn should_backoff(error: &crate::error::ClientError) -> bool {
    is_transport_failure(error)
}

/// Jittered exponential backoff. The returned duration is uniformly
/// distributed in `[0, base * 2^attempt]`, capped at
/// [`RetryPolicy::MAX_BACKOFF`]. Attempt 0 gives `[0, base]`; attempt 1
/// gives `[0, 2*base]`; and so on. Full-jitter (AWS-style) is used
/// because it minimises thundering-herd retries — every client picks
/// an independent point in the window rather than clustering at the
/// upper bound.
pub(crate) fn jittered_backoff(base: Duration, attempt: u32) -> Duration {
    if base.is_zero() {
        return Duration::ZERO;
    }
    let shift = attempt.min(20);
    let upper = base
        .saturating_mul(1u32.checked_shl(shift).unwrap_or(u32::MAX))
        .min(RetryPolicy::MAX_BACKOFF);
    let upper_micros = upper.as_micros().min(u64::MAX as u128) as u64;
    if upper_micros == 0 {
        return Duration::ZERO;
    }
    let picked = rand::random_range(0..=upper_micros);
    Duration::from_micros(picked)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_documented_values() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.max_attempts, 5);
        assert_eq!(policy.per_attempt_deadline, Duration::from_secs(2));
        assert_eq!(policy.overall_deadline, Duration::from_secs(10));
        assert_eq!(policy.base_backoff, Duration::from_millis(50));
        assert_eq!(policy.leader_ttl, Duration::from_secs(30));
    }

    #[test]
    fn jittered_backoff_zero_base_returns_zero() {
        for attempt in 0..5 {
            assert_eq!(jittered_backoff(Duration::ZERO, attempt), Duration::ZERO);
        }
    }

    #[test]
    fn jittered_backoff_respects_window_at_attempt_zero() {
        let base = Duration::from_millis(10);
        for _ in 0..200 {
            let picked = jittered_backoff(base, 0);
            assert!(
                picked <= base,
                "attempt 0 must be in [0, base]; got {picked:?} > {base:?}"
            );
        }
    }

    #[test]
    fn jittered_backoff_respects_window_at_higher_attempts() {
        let base = Duration::from_millis(10);
        for attempt in 1..6u32 {
            let upper = base
                .saturating_mul(1u32 << attempt)
                .min(RetryPolicy::MAX_BACKOFF);
            for _ in 0..200 {
                let picked = jittered_backoff(base, attempt);
                assert!(
                    picked <= upper,
                    "attempt {attempt} must be in [0, {upper:?}]; got {picked:?}"
                );
            }
        }
    }

    #[test]
    fn jittered_backoff_caps_at_max_backoff() {
        let base = Duration::from_secs(10);
        for _ in 0..200 {
            let picked = jittered_backoff(base, 5);
            assert!(
                picked <= RetryPolicy::MAX_BACKOFF,
                "saturating cap must hold; got {picked:?} > {:?}",
                RetryPolicy::MAX_BACKOFF
            );
        }
    }

    #[test]
    fn jittered_backoff_produces_a_distribution_not_a_constant() {
        // Full-jitter: across many draws at the same attempt count, the
        // picked duration must vary. If it were a constant (e.g. always
        // upper bound, never sampled), the property test above would
        // pass but the de-correlation goal would be defeated.
        let base = Duration::from_millis(100);
        let mut min = Duration::MAX;
        let mut max = Duration::ZERO;
        for _ in 0..200 {
            let picked = jittered_backoff(base, 3);
            if picked < min {
                min = picked;
            }
            if picked > max {
                max = picked;
            }
        }
        assert!(
            max > min,
            "jittered_backoff must produce a distribution; min={min:?}, max={max:?}"
        );
    }

    #[test]
    fn should_backoff_matches_documented_status_codes() {
        use crate::error::ClientError;
        assert!(should_backoff(&ClientError::Rpc(
            tonic::Status::unavailable("u")
        )));
        assert!(should_backoff(&ClientError::Rpc(
            tonic::Status::deadline_exceeded("d")
        )));
        assert!(!should_backoff(&ClientError::Rpc(tonic::Status::internal(
            "i"
        ))));
        assert!(!should_backoff(&ClientError::Rpc(
            tonic::Status::failed_precondition("fp")
        )));
        assert!(!should_backoff(&ClientError::NoReachableEndpoints));
        // `TransportFanout` is the coalesced-waiter copy of a `Transport`
        // failure and must stay in the transport-failure class — distinct
        // from the `NoReachableEndpoints` case directly above, which the
        // fanout previously collapsed into (issue #241).
        assert!(should_backoff(&ClientError::TransportFanout("t".into())));
        assert!(!should_backoff(&ClientError::InvalidEndpoint("e".into())));
        assert!(!should_backoff(&ClientError::InvalidCount(0)));
        // `DriverGone` is deterministic: the local driver task is dead,
        // so retrying without sleeping wouldn't gain anything (every
        // retry returns `DriverGone` immediately until the `Client` is
        // rebuilt). Sleeping would just delay the inevitable.
        assert!(!should_backoff(&ClientError::DriverGone));
    }
}
