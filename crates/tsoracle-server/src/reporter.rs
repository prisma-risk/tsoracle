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

//! Typed metric facade for `tsoracle-server`.
//!
//! `Reporter` is the single place metric-name string literals live in this
//! crate. Each typed counter/histogram field forwards an increment/record to
//! both the global `metrics::` recorder (Prometheus path, unchanged) and a
//! local `Arc<AtomicU64>` that the heartbeat task can read without depending
//! on any installed recorder.
//!
//! **Scaffold note**: items are introduced here and wired up in later plan
//! tasks (T3–T11). `#![allow(dead_code)]` is present for that reason and
//! will become unnecessary once the call sites land in Task 7.
#![allow(dead_code)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use tsoracle_core::IgnoreReason;

pub struct ReporterCounter {
    name: &'static str,
    local: Arc<AtomicU64>,
}

impl ReporterCounter {
    pub(crate) fn new(name: &'static str) -> Self {
        Self {
            name,
            local: Arc::new(AtomicU64::new(0)),
        }
    }

    pub(crate) fn increment(&self, n: u64) {
        #[cfg(feature = "metrics")]
        metrics::counter!(self.name).increment(n);
        self.local.fetch_add(n, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> u64 {
        self.local.load(Ordering::Relaxed)
    }
}

pub struct ReporterHistogram {
    name: &'static str,
}

impl ReporterHistogram {
    pub(crate) fn new(name: &'static str) -> Self {
        Self { name }
    }

    pub(crate) fn record(&self, _v: f64) {
        #[cfg(feature = "metrics")]
        metrics::histogram!(self.name).record(_v);
    }
}

pub struct ReporterTimestamp {
    local: Arc<AtomicU64>,
}

impl ReporterTimestamp {
    pub(crate) fn new() -> Self {
        Self {
            local: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Record `SystemTime::now()` as unix-ms. Idempotent under concurrent
    /// callers — the last writer wins under `Relaxed`, which matches the
    /// "approximate last transition time" semantics the heartbeat needs.
    pub(crate) fn touch_now(&self) {
        self.local.store(now_unix_ms(), Ordering::Relaxed);
    }

    /// `None` if never touched, else the most recent unix-ms.
    pub(crate) fn snapshot(&self) -> Option<u64> {
        let v = self.local.load(Ordering::Relaxed);
        (v != 0).then_some(v)
    }
}

pub struct IgnoredCommitsByReason {
    pub not_leader: ReporterCounter,
    pub epoch_mismatch: ReporterCounter,
    pub not_advanced: ReporterCounter,
}

impl IgnoredCommitsByReason {
    fn new() -> Self {
        Self {
            not_leader: ReporterCounter::new("tsoracle.window.extensions.ignored.not_leader.total"),
            epoch_mismatch: ReporterCounter::new(
                "tsoracle.window.extensions.ignored.epoch_mismatch.total",
            ),
            not_advanced: ReporterCounter::new(
                "tsoracle.window.extensions.ignored.not_advanced.total",
            ),
        }
    }

    pub(crate) fn for_reason(&self, reason: IgnoreReason) -> &ReporterCounter {
        match reason {
            IgnoreReason::NotLeader => &self.not_leader,
            IgnoreReason::EpochMismatch { .. } => &self.epoch_mismatch,
            IgnoreReason::NotAdvanced { .. } => &self.not_advanced,
        }
    }
}

pub struct Reporter {
    // get_ts hot path
    pub get_ts_requests: ReporterCounter,
    pub get_ts_success: ReporterCounter,
    pub timestamps_issued: ReporterCounter,

    // get_seq hot path
    pub get_seq_requests: ReporterCounter,
    pub get_seq_success: ReporterCounter,
    pub seq_values_issued: ReporterCounter,
    pub seq_cardinality_rejected: ReporterCounter,

    // get_seq_batch hot path. Batch keeps its OWN values/cardinality counters
    // (rather than sharing the single-key ones) so the `tsoracle.get_seq.*`
    // single-key series stays free of batch traffic and each RPC is separately
    // observable.
    pub get_seq_batch_requests: ReporterCounter,
    pub get_seq_batch_success: ReporterCounter,
    pub seq_batch_keys: ReporterHistogram,
    pub seq_batch_values_issued: ReporterCounter,
    pub seq_batch_cardinality_rejected: ReporterCounter,

    // leader / fence
    pub not_leader: ReporterCounter,
    pub leader_transitions: ReporterCounter,
    pub fence_transient_retries: ReporterCounter,
    pub fence_latency: ReporterHistogram,

    // window
    pub window_extensions: ReporterCounter,
    pub window_extension_latency: ReporterHistogram,
    pub ignored_commits: IgnoredCommitsByReason,

    // lifecycle
    pub shutdown_watch_aborted: ReporterCounter,
    pub heartbeat_task_panicked: ReporterCounter,
    pub last_leader_transition: ReporterTimestamp,
    pub started_at: Instant,
}

impl Reporter {
    pub fn new() -> Self {
        Self {
            get_ts_requests: ReporterCounter::new("tsoracle.get_ts.requests.total"),
            get_ts_success: ReporterCounter::new("tsoracle.get_ts.success.total"),
            timestamps_issued: ReporterCounter::new("tsoracle.get_ts.timestamps_issued"),
            get_seq_requests: ReporterCounter::new("tsoracle.get_seq.requests.total"),
            get_seq_success: ReporterCounter::new("tsoracle.get_seq.success.total"),
            seq_values_issued: ReporterCounter::new("tsoracle.get_seq.values_issued"),
            seq_cardinality_rejected: ReporterCounter::new(
                "tsoracle.get_seq.cardinality_rejected.total",
            ),
            get_seq_batch_requests: ReporterCounter::new("tsoracle.get_seq.batch.requests.total"),
            get_seq_batch_success: ReporterCounter::new("tsoracle.get_seq.batch.success.total"),
            seq_batch_keys: ReporterHistogram::new("tsoracle.get_seq.batch.keys"),
            seq_batch_values_issued: ReporterCounter::new("tsoracle.get_seq.batch.values_issued"),
            seq_batch_cardinality_rejected: ReporterCounter::new(
                "tsoracle.get_seq.batch.cardinality_rejected.total",
            ),
            not_leader: ReporterCounter::new("tsoracle.not_leader.total"),
            leader_transitions: ReporterCounter::new("tsoracle.leader_transition.total"),
            fence_transient_retries: ReporterCounter::new(
                "tsoracle.leader_transition.fence_transient_retries.total",
            ),
            fence_latency: ReporterHistogram::new("tsoracle.leader_transition.fence_latency"),
            window_extensions: ReporterCounter::new("tsoracle.window.extensions.total"),
            window_extension_latency: ReporterHistogram::new("tsoracle.window.extension_latency"),
            ignored_commits: IgnoredCommitsByReason::new(),
            shutdown_watch_aborted: ReporterCounter::new("tsoracle.shutdown.watch_aborted.total"),
            heartbeat_task_panicked: ReporterCounter::new("tsoracle.heartbeat.task_panicked.total"),
            last_leader_transition: ReporterTimestamp::new(),
            started_at: Instant::now(),
        }
    }

    /// Convenience constructor for unit tests. Same as `Reporter::new()`; the
    /// dedicated name documents intent at call sites (tests that just need a
    /// Reporter to satisfy a struct field).
    #[cfg(any(test, feature = "test-support"))]
    pub fn for_tests() -> Self {
        Self::new()
    }
}

impl Default for Reporter {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counter_increment_bumps_local_snapshot() {
        let c = ReporterCounter::new("tsoracle.test.local");
        assert_eq!(c.snapshot(), 0);
        c.increment(1);
        c.increment(4);
        assert_eq!(c.snapshot(), 5);
    }

    #[test]
    #[cfg(feature = "metrics")]
    fn counter_increment_forwards_to_metrics_recorder() {
        use metrics::Key;
        use metrics_util::CompositeKey;
        use metrics_util::MetricKind;
        use metrics_util::debugging::{DebuggingRecorder, Snapshotter};

        let recorder = DebuggingRecorder::new();
        let snapshotter: Snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            let c = ReporterCounter::new("tsoracle.test.forwarded");
            c.increment(7);
        });

        let snap = snapshotter.snapshot().into_vec();
        let key = CompositeKey::new(
            MetricKind::Counter,
            Key::from_name("tsoracle.test.forwarded"),
        );
        let entry = snap
            .iter()
            .find(|(k, ..)| *k == key)
            .expect("counter not recorded by DebuggingRecorder");
        let (_, _, _, value) = entry;
        assert_eq!(
            format!("{value:?}"),
            "Counter(7)",
            "expected the recorder to observe Counter(7), got {value:?}"
        );
    }

    #[test]
    #[cfg(feature = "metrics")]
    fn histogram_record_forwards_to_metrics_recorder() {
        use metrics::Key;
        use metrics_util::CompositeKey;
        use metrics_util::MetricKind;
        use metrics_util::debugging::DebuggingRecorder;

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            let h = ReporterHistogram::new("tsoracle.test.hist");
            h.record(1.0);
            h.record(2.0);
            h.record(3.0);
        });

        let snap = snapshotter.snapshot().into_vec();
        let key = CompositeKey::new(MetricKind::Histogram, Key::from_name("tsoracle.test.hist"));
        assert!(
            snap.iter().any(|(k, ..)| *k == key),
            "histogram key not recorded"
        );
    }

    #[test]
    fn timestamp_zero_is_none() {
        let t = ReporterTimestamp::new();
        assert_eq!(t.snapshot(), None);
    }

    #[test]
    fn timestamp_touch_records_now() {
        let before = now_unix_ms();
        let t = ReporterTimestamp::new();
        t.touch_now();
        let after = now_unix_ms();
        let observed = t.snapshot().expect("touch_now should produce Some");
        assert!(
            observed >= before && observed <= after,
            "timestamp {observed} not within [{before}, {after}]"
        );
    }

    #[test]
    #[cfg(not(feature = "metrics"))]
    fn counter_works_without_metrics_feature() {
        // With the `metrics` feature off, the increment body skips the
        // recorder forward but the local atomic still bumps — the heartbeat
        // path stays functional in default tsoracle-bin builds (which do not
        // enable the `metrics` feature on tsoracle-server).
        let c = ReporterCounter::new("tsoracle.test.no_metrics");
        c.increment(3);
        assert_eq!(c.snapshot(), 3);
    }

    #[test]
    #[cfg(feature = "metrics")]
    fn reporter_new_resolves_distinct_metric_names() {
        use metrics::Key;
        use metrics_util::CompositeKey;
        use metrics_util::MetricKind;
        use metrics_util::debugging::DebuggingRecorder;
        use tsoracle_core::{Epoch, IgnoreReason};

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            let r = Reporter::new();
            r.ignored_commits
                .for_reason(IgnoreReason::NotLeader)
                .increment(1);
            r.ignored_commits
                .for_reason(IgnoreReason::EpochMismatch {
                    expected: Epoch(1),
                    current: Epoch(2),
                })
                .increment(1);
            r.ignored_commits
                .for_reason(IgnoreReason::NotAdvanced {
                    persisted: 1,
                    committed: 2,
                })
                .increment(1);
        });

        let snap = snapshotter.snapshot().into_vec();
        for name in [
            "tsoracle.window.extensions.ignored.not_leader.total",
            "tsoracle.window.extensions.ignored.epoch_mismatch.total",
            "tsoracle.window.extensions.ignored.not_advanced.total",
        ] {
            let key = CompositeKey::new(MetricKind::Counter, Key::from_name(name));
            assert!(
                snap.iter().any(|(k, ..)| *k == key),
                "expected metric {name} to be recorded distinctly"
            );
        }
    }
}
