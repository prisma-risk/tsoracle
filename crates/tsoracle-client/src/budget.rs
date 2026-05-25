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

//! Deadline arithmetic for the client retry loop.
//!
//! Three deadlines bound a call, governed by [`crate::RetryPolicy`]:
//!
//! - the overall wall-clock cap on the whole `issue_rpc` call,
//! - the per-attempt cap on a single `(connect, get_ts)` pair, and
//! - the pair-split that shares one per-attempt budget across the connect
//!   and `get_ts` phases so a slow connect eats into the time left for
//!   `get_ts` instead of each phase getting a fresh full budget.
//!
//! [`Budget`] owns the first two (loop level); [`PairBudget`] owns the
//! third (within a single attempt). Both anchor an [`Instant`] up front
//! and floor at zero via `saturating_duration_since`, so a deadline that
//! has already elapsed yields an immediate-timeout `Duration::ZERO`
//! rather than a fresh full budget.

use std::time::Duration;

use tokio::time::Instant;

use crate::retry_policy::RetryPolicy;

/// Loop-level deadlines: the overall wall-clock cap and the per-attempt
/// cap. Anchored once at the start of `issue_rpc`.
pub(crate) struct Budget {
    deadline: Instant,
    per_attempt: Duration,
}

impl Budget {
    /// Anchor the overall deadline at `now + overall_deadline` and carry
    /// the per-attempt cap.
    pub(crate) fn start(policy: &RetryPolicy) -> Self {
        Budget {
            deadline: Instant::now() + policy.overall_deadline,
            per_attempt: policy.per_attempt_deadline,
        }
    }

    /// Budget for the next attempt, or `None` if the overall deadline has
    /// been reached (the loop must stop before starting another attempt).
    /// When time remains, the attempt gets the smaller of the remaining
    /// overall budget and the per-attempt cap.
    pub(crate) fn next_attempt(&self) -> Option<Duration> {
        let now = Instant::now();
        if now >= self.deadline {
            return None;
        }
        Some((self.deadline - now).min(self.per_attempt))
    }

    /// Clamp a proposed backoff sleep so it never sleeps past the overall
    /// deadline. Floors at zero when the deadline has already elapsed.
    pub(crate) fn clamp_backoff(&self, backoff: Duration) -> Duration {
        backoff.min(self.deadline.saturating_duration_since(Instant::now()))
    }
}

/// Within-attempt deadline shared across the connect and `get_ts` phases.
/// Anchored at the start of an attempt so the second phase only gets what
/// the first left behind.
pub(crate) struct PairBudget {
    deadline: Instant,
}

impl PairBudget {
    /// Anchor the pair deadline at `now + budget`.
    pub(crate) fn start(budget: Duration) -> Self {
        PairBudget {
            deadline: Instant::now() + budget,
        }
    }

    /// Time left for the next phase of the pair. Floors at zero — a
    /// connect that consumed the whole budget yields an immediate timeout
    /// rather than a fresh one.
    pub(crate) fn remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(per_attempt: Duration, overall: Duration) -> RetryPolicy {
        RetryPolicy {
            max_attempts: 5,
            per_attempt_deadline: per_attempt,
            overall_deadline: overall,
            base_backoff: Duration::from_millis(1),
            leader_ttl: Duration::from_secs(30),
        }
    }

    /// With plenty of overall budget, an attempt is capped at the
    /// per-attempt deadline, not the (larger) remaining overall budget.
    #[tokio::test]
    async fn next_attempt_is_capped_by_per_attempt_deadline() {
        let budget = Budget::start(&policy(Duration::from_millis(100), Duration::from_secs(10)));
        let attempt = budget.next_attempt().expect("budget remains");
        assert!(
            attempt <= Duration::from_millis(100),
            "attempt must not exceed per_attempt_deadline, got {attempt:?}",
        );
        assert!(
            attempt > Duration::from_millis(90),
            "with 10s overall, the attempt should be ~per_attempt_deadline, got {attempt:?}",
        );
    }

    /// Once the overall deadline has elapsed, no further attempt may start.
    #[tokio::test(start_paused = true)]
    async fn next_attempt_returns_none_past_overall_deadline() {
        let budget = Budget::start(&policy(
            Duration::from_millis(100),
            Duration::from_millis(50),
        ));
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(
            budget.next_attempt(),
            None,
            "no attempt may start after the overall deadline",
        );
    }

    /// The remaining overall budget caps the attempt when it is smaller
    /// than the per-attempt deadline (near the end of the call).
    #[tokio::test(start_paused = true)]
    async fn next_attempt_is_capped_by_remaining_overall_budget() {
        let budget = Budget::start(&policy(Duration::from_secs(10), Duration::from_millis(100)));
        tokio::time::sleep(Duration::from_millis(70)).await;
        let attempt = budget.next_attempt().expect("budget remains");
        assert!(
            attempt <= Duration::from_millis(30),
            "attempt must be capped by the ~30ms remaining overall budget, got {attempt:?}",
        );
    }

    /// A backoff longer than the remaining overall budget is clamped down
    /// to that remainder so a single sleep cannot overrun the deadline.
    #[tokio::test(start_paused = true)]
    async fn clamp_backoff_caps_at_remaining_overall_budget() {
        let budget = Budget::start(&policy(Duration::from_secs(10), Duration::from_millis(100)));
        tokio::time::sleep(Duration::from_millis(80)).await;
        let sleep = budget.clamp_backoff(Duration::from_secs(5));
        assert!(
            sleep <= Duration::from_millis(20),
            "backoff must clamp to the ~20ms remaining, got {sleep:?}",
        );
    }

    /// Past the overall deadline, the clamp floors at zero.
    #[tokio::test(start_paused = true)]
    async fn clamp_backoff_floors_at_zero_past_deadline() {
        let budget = Budget::start(&policy(Duration::from_secs(10), Duration::from_millis(50)));
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(
            budget.clamp_backoff(Duration::from_secs(5)),
            Duration::ZERO,
            "an elapsed deadline must yield a zero-length backoff",
        );
    }

    /// A pair budget consumed in full by the first phase leaves nothing
    /// for the second — `remaining` floors at zero rather than wrapping.
    #[tokio::test(start_paused = true)]
    async fn pair_budget_remaining_floors_at_zero() {
        let pair = PairBudget::start(Duration::from_millis(50));
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(
            pair.remaining(),
            Duration::ZERO,
            "a fully-consumed pair budget must floor at zero",
        );
    }

    /// A pair budget only partly consumed leaves the remainder for the
    /// next phase.
    #[tokio::test(start_paused = true)]
    async fn pair_budget_remaining_returns_leftover() {
        let pair = PairBudget::start(Duration::from_millis(100));
        tokio::time::sleep(Duration::from_millis(40)).await;
        let left = pair.remaining();
        assert!(
            left <= Duration::from_millis(60) && left > Duration::from_millis(50),
            "≈60ms should remain after consuming 40ms of 100ms, got {left:?}",
        );
    }
}
