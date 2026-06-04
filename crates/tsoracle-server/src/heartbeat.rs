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

use std::sync::Arc;
use std::time::Duration;

use crate::reporter::Reporter;
use crate::server::ServingState;
use crate::serving_core::ServingCore;

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

/// Heartbeat loop: every `interval`, snapshot the Reporter, sample the
/// serving state, and emit one structured `tracing::info!` line.
///
/// Returns when `cancel` resolves (explicit send OR sender drop). The cancel
/// branch is `biased` first in the `select!`, so shutdown is always preferred
/// over an in-flight tick.
pub(crate) async fn run_heartbeat(
    interval: Duration,
    core: Arc<ServingCore>,
    reporter: Arc<Reporter>,
    cancel: tokio::sync::oneshot::Receiver<()>,
) {
    let mut prev = HeartbeatSnapshot::sample(&reporter);
    let started = reporter.started_at;
    let mut cancel = cancel;
    loop {
        tokio::select! {
            biased;
            _ = &mut cancel => break,
            _ = tokio::time::sleep(interval) => {
                let curr  = HeartbeatSnapshot::sample(&reporter);
                let state = core.serving_state();
                let epoch = core.current_epoch();
                let last_age = curr.last_transition_unix_ms.map(age_secs_from);

                tracing::info!(
                    target: "tsoracle::heartbeat",
                    uptime_secs              = started.elapsed().as_secs(),
                    serving                  = matches!(state, ServingState::Serving),
                    epoch                    = ?epoch.map(|e| e.0),
                    requests                 = curr.requests.wrapping_sub(prev.requests),
                    requests_total           = curr.requests,
                    ts_issued                = curr.ts_issued.wrapping_sub(prev.ts_issued),
                    not_leader               = curr.not_leader.wrapping_sub(prev.not_leader),
                    transitions              = curr.transitions.wrapping_sub(prev.transitions),
                    fence_retries            = curr.fence_retries.wrapping_sub(prev.fence_retries),
                    last_transition_age_secs = ?last_age,
                    "heartbeat"
                );
                prev = curr;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Mutex as StdMutex;
    use tracing_subscriber::fmt::MakeWriter;

    #[derive(Clone, Default)]
    struct BufWriter(std::sync::Arc<StdMutex<Vec<u8>>>);

    impl<'a> MakeWriter<'a> for BufWriter {
        type Writer = BufWriterHandle;
        fn make_writer(&'a self) -> Self::Writer {
            BufWriterHandle(self.0.clone())
        }
    }

    struct BufWriterHandle(std::sync::Arc<StdMutex<Vec<u8>>>);
    impl std::io::Write for BufWriterHandle {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(b);
            Ok(b.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn emits_after_each_interval() {
        let buf = BufWriter::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(buf.clone())
            .with_max_level(tracing::Level::INFO)
            .with_target(true)
            .with_ansi(false)
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let reporter = std::sync::Arc::new(Reporter::new());
        let core = std::sync::Arc::new(ServingCore::new(
            Duration::from_secs(3),
            tsoracle_core::DEFAULT_MAX_SEQ_COUNT,
            tsoracle_core::DEFAULT_MAX_SEQ_BATCH_KEYS,
        ));
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();

        // Drive heartbeat and test control in the same async context so the
        // thread-local subscriber set above is active during every poll.
        let hb_fut = run_heartbeat(
            Duration::from_millis(50),
            core.clone(),
            reporter.clone(),
            rx,
        );

        // Control future: advance time one tick at a time so the heartbeat
        // future gets woken and polled between advances.
        let ctrl_fut = async move {
            for _ in 0..3 {
                tokio::time::advance(Duration::from_millis(55)).await;
                tokio::task::yield_now().await;
            }
            drop(tx);
        };

        // join! drives both concurrently in the same async context (no spawn),
        // so the thread-local subscriber is active during run_heartbeat polls.
        tokio::join!(hb_fut, ctrl_fut);

        let output = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
        let lines = output
            .lines()
            .filter(|l| l.contains("tsoracle::heartbeat"))
            .count();
        assert!(
            lines >= 3,
            "expected >= 3 heartbeat lines, got {lines}.\nFull output:\n{output}"
        );
    }

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

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn idle_tick_still_emits_with_zero_deltas() {
        let buf = BufWriter::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(buf.clone())
            .with_max_level(tracing::Level::INFO)
            .with_target(true)
            .with_ansi(false)
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let reporter = std::sync::Arc::new(Reporter::new());
        let core = std::sync::Arc::new(ServingCore::new(
            Duration::from_secs(3),
            tsoracle_core::DEFAULT_MAX_SEQ_COUNT,
            tsoracle_core::DEFAULT_MAX_SEQ_BATCH_KEYS,
        ));
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();

        let hb_fut = run_heartbeat(
            Duration::from_millis(50),
            core.clone(),
            reporter.clone(),
            rx,
        );
        let ctrl_fut = async move {
            tokio::time::advance(Duration::from_millis(55)).await;
            tokio::task::yield_now().await;
            drop(tx);
        };
        tokio::join!(hb_fut, ctrl_fut);

        let output = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
        assert!(
            output.contains("requests=0"),
            "idle tick should report requests=0: {output}"
        );
        assert!(
            output.contains("requests_total=0"),
            "expected requests_total=0: {output}"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn deltas_reset_each_tick() {
        let buf = BufWriter::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(buf.clone())
            .with_max_level(tracing::Level::INFO)
            .with_target(true)
            .with_ansi(false)
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let reporter = std::sync::Arc::new(Reporter::new());
        let core = std::sync::Arc::new(ServingCore::new(
            Duration::from_secs(3),
            tsoracle_core::DEFAULT_MAX_SEQ_COUNT,
            tsoracle_core::DEFAULT_MAX_SEQ_BATCH_KEYS,
        ));
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();

        let reporter2 = reporter.clone();
        let hb_fut = run_heartbeat(
            Duration::from_millis(50),
            core.clone(),
            reporter.clone(),
            rx,
        );
        let ctrl_fut = async move {
            // Tick 1: 3 requests
            reporter2.get_ts_requests.increment(3);
            tokio::time::advance(Duration::from_millis(55)).await;
            tokio::task::yield_now().await;

            // Tick 2: 5 more requests (8 total). Delta should be 5, not 8.
            reporter2.get_ts_requests.increment(5);
            tokio::time::advance(Duration::from_millis(55)).await;
            tokio::task::yield_now().await;

            drop(tx);
        };
        tokio::join!(hb_fut, ctrl_fut);

        let output = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
        assert!(
            output.contains("requests=3"),
            "tick 1 should report requests=3: {output}"
        );
        assert!(
            output.contains("requests=5"),
            "tick 2 should report requests=5: {output}"
        );
        assert!(
            !output.contains("requests=8"),
            "delta should reset; got cumulative: {output}"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn cancel_terminates_promptly_on_sender_drop() {
        let reporter = std::sync::Arc::new(Reporter::new());
        let core = std::sync::Arc::new(ServingCore::new(
            Duration::from_secs(3),
            tsoracle_core::DEFAULT_MAX_SEQ_COUNT,
            tsoracle_core::DEFAULT_MAX_SEQ_BATCH_KEYS,
        ));
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();

        // Drop tx immediately — the biased cancel branch should win without
        // waiting for the 60s sleep in the other arm.
        drop(tx);
        tokio::time::timeout(
            Duration::from_millis(50),
            run_heartbeat(Duration::from_secs(60), core.clone(), reporter.clone(), rx),
        )
        .await
        .expect("heartbeat did not terminate after cancel sender dropped");
    }
}
