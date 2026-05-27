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
use std::time::{SystemTime, UNIX_EPOCH};

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
}
