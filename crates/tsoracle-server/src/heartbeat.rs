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

//! Periodic heartbeat task: emits one structured `tracing::info!` line per
//! `interval` summarising activity since the prior tick. Lives next to the
//! leader-watch task in the `Server::into_router_parts()` spawn site so both
//! the embedder path (`into_router()`) and the daemon path (`serve*`) get it.

#![allow(dead_code)]

use crate::reporter::Reporter;

/// Subset of Reporter counters/timestamps the heartbeat line carries. Adding a
/// new Reporter field does NOT automatically appear here — `sample()` must be
/// updated explicitly. That's the gate: forgetting to add a metric to the
/// heartbeat is the safe default.
pub(crate) struct HeartbeatSnapshot {
    pub requests: u64,
    pub ts_issued: u64,
    pub not_leader: u64,
    pub transitions: u64,
    pub fence_retries: u64,
    pub last_transition_unix_ms: Option<u64>,
}

impl HeartbeatSnapshot {
    pub(crate) fn sample(r: &Reporter) -> Self {
        Self {
            requests: r.get_ts_requests.snapshot(),
            ts_issued: r.timestamps_issued.snapshot(),
            not_leader: r.not_leader.snapshot(),
            transitions: r.leader_transitions.snapshot(),
            fence_retries: r.fence_transient_retries.snapshot(),
            last_transition_unix_ms: r.last_leader_transition.snapshot(),
        }
    }
}

/// Saturating wall-clock age in seconds since `then_unix_ms`. Returns 0 if the
/// stored timestamp is ahead of now (wall-clock skew tolerance).
pub(crate) fn age_secs_from(then_unix_ms: u64) -> u64 {
    let now_ms = crate::reporter::now_unix_ms();
    now_ms.saturating_sub(then_unix_ms) / 1000
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_reflects_current_counter_values() {
        let r = Reporter::new();
        r.get_ts_requests.increment(3);
        r.timestamps_issued.increment(8);
        r.leader_transitions.increment(1);
        r.last_leader_transition.touch_now();

        let s = HeartbeatSnapshot::sample(&r);
        assert_eq!(s.requests, 3);
        assert_eq!(s.ts_issued, 8);
        assert_eq!(s.transitions, 1);
        assert_eq!(s.not_leader, 0);
        assert!(s.last_transition_unix_ms.is_some());
    }

    #[test]
    fn age_zero_on_future_timestamp() {
        let now = crate::reporter::now_unix_ms();
        // Pretend last transition is one full second in the future.
        let future = now + 1000;
        assert_eq!(age_secs_from(future), 0);
    }
}
