//! Single-consumer task that checks all four invariants.

use std::collections::HashMap;
use std::time::Instant;

use hdrhistogram::Histogram;
use tokio::sync::mpsc;
use tsoracle_core::Timestamp;

use crate::chaos::{ChaosEvent, ChaosKind, ChaosWindow};
use crate::event::SupervisorEvent;
use crate::sample::{IssuedSample, LivenessIncident};
use crate::types::{BatchId, ClientId};
use crate::violation::{Violation, ViolationKind};
use crate::{HISTO_MAX_US, new_histogram};

/// What the supervisor returns at end of run.
#[derive(Debug, Clone)]
pub struct SupervisorOutcome {
    pub violations: Vec<Violation>,
    pub high_water: Timestamp,
    pub events_observed: u64,
    /// Per-RPC latency in microseconds. One sample per batch (recorded on the
    /// first `IssuedSample` of each batch, since all samples in a batch share
    /// the same `issued_at`/`recv_time`). Values clamped to
    /// `[1, HISTO_MAX_US]` to stay within histogram bounds.
    pub latency: Histogram<u64>,
}

pub struct Supervisor {
    state: SupervisorState,
}

#[derive(Debug)]
struct SupervisorState {
    high_water: Timestamp,
    open_batches: HashMap<(ClientId, BatchId), OpenBatch>,
    open_windows: Vec<OpenChaosWindow>,
    violations: Vec<Violation>,
    events_observed: u64,
    latency: Histogram<u64>,
}

impl Default for SupervisorState {
    fn default() -> Self {
        Self {
            high_water: Timestamp(0),
            open_batches: HashMap::new(),
            open_windows: Vec::new(),
            violations: Vec::new(),
            events_observed: 0,
            latency: new_histogram(),
        }
    }
}

#[derive(Debug)]
struct OpenBatch {
    values: Vec<Timestamp>,
}

#[derive(Debug)]
struct OpenChaosWindow {
    window: ChaosWindow,
    pre_window_high_water: Timestamp,
    /// Once true, the fence-freshness check has fired for this window and we
    /// will not re-check it. Failpoint windows are pre-marked true since they
    /// don't change leadership.
    fence_freshness_checked: bool,
}

impl Default for Supervisor {
    fn default() -> Self {
        Self::new()
    }
}

impl Supervisor {
    pub fn new() -> Self {
        Self {
            state: SupervisorState::default(),
        }
    }

    pub async fn run(mut self, mut rx: mpsc::Receiver<SupervisorEvent>) -> SupervisorOutcome {
        while let Some(event) = rx.recv().await {
            self.state.events_observed += 1;
            match event {
                SupervisorEvent::Issued(sample) => self.on_issued(sample),
                SupervisorEvent::Chaos(ev) => self.on_chaos(ev),
                SupervisorEvent::Liveness(incident) => self.on_liveness(incident),
                SupervisorEvent::End => break,
            }
        }
        self.final_pass();
        SupervisorOutcome {
            violations: self.state.violations,
            high_water: self.state.high_water,
            events_observed: self.state.events_observed,
            latency: self.state.latency,
        }
    }

    /// Final-pass policy (spec § "Shutdown"): drop unsettled state without
    /// recording violations. Partial batches, unfinished fence checks, and
    /// in-progress chaos windows are inconclusive at end-of-run — absence of
    /// evidence is not evidence of absence.
    fn final_pass(&mut self) {
        self.state.open_batches.clear();
        self.state.open_windows.clear();
    }

    fn on_issued(&mut self, sample: IssuedSample) {
        // (0) Per-RPC latency. All samples in a batch share `issued_at` and
        // `recv_time`, so we record once per RPC by gating on `batch_idx == 0`.
        // `saturating_duration_since` handles the (unlikely) case where the
        // clock appears to go backward.
        if sample.batch_idx == 0 {
            let elapsed_us = sample
                .recv_time
                .saturating_duration_since(sample.issued_at)
                .as_micros();
            let clamped = (elapsed_us as u64).clamp(1, HISTO_MAX_US);
            let _ = self.state.latency.record(clamped);
        }

        // (1) Global monotonicity.
        if sample.ts <= self.state.high_water {
            self.state.violations.push(Violation {
                kind: ViolationKind::Monotonicity {
                    prev: self.state.high_water,
                    got: sample.ts,
                    sample: sample.clone(),
                },
                at: Instant::now(),
            });
        } else {
            self.state.high_water = sample.ts;
        }

        // (1.5) Fence freshness — first post-window-grace sample participates.
        // We collect violations into a local buffer to keep the borrow of
        // `self.state.open_windows` disjoint from the push into
        // `self.state.violations`.
        let pending_fence: Vec<Violation> = {
            let mut found = Vec::new();
            for open in self.state.open_windows.iter_mut() {
                let post_grace_at = open.window.ended_at + open.window.grace;
                if !open.fence_freshness_checked && sample.recv_time >= post_grace_at {
                    if sample.ts <= open.pre_window_high_water {
                        found.push(Violation {
                            kind: ViolationKind::FenceFreshness {
                                pre_window_high_water: open.pre_window_high_water,
                                first_post_window_ts: sample.ts,
                                window_kind: open.window.kind.clone(),
                            },
                            at: Instant::now(),
                        });
                    }
                    open.fence_freshness_checked = true;
                }
            }
            found
        };
        self.state.violations.extend(pending_fence);
        // Drop windows past grace whose fence check has fired (or didn't apply).
        self.state.open_windows.retain(|open| {
            let post_grace_at = open.window.ended_at + open.window.grace;
            sample.recv_time < post_grace_at || !open.fence_freshness_checked
        });

        // (2) Batch internal ordering.
        let key = (sample.client_id, sample.batch_id);
        self.state
            .open_batches
            .entry(key)
            .or_insert_with(|| OpenBatch { values: Vec::new() })
            .values
            .push(sample.ts);
        if sample.is_last {
            // The just-inserted entry must exist; the `if let Some` handles
            // the unreachable None case without triggering clippy::expect_used.
            if let Some(open) = self.state.open_batches.remove(&key) {
                check_batch(
                    &mut self.state.violations,
                    sample.client_id,
                    sample.batch_id,
                    open.values,
                );
            }
        }
    }

    fn on_chaos(&mut self, ev: ChaosEvent) {
        // Only Applied windows participate in invariant checks. Skipped/Failed
        // are recorded by the caller (in chaos_events vec); supervisor ignores.
        if !ev.outcome.is_applied() {
            return;
        }
        let pre_window_high_water = self.state.high_water;
        let fence_freshness_checked = match ev.window.kind {
            ChaosKind::LeaderKill | ChaosKind::LeaderPause => false,
            // Failpoint windows participate only in liveness gating (Task 9);
            // they don't change leadership so no fence check applies.
            ChaosKind::FailpointArm { .. } | ChaosKind::FailpointDisarm { .. } => true,
        };
        self.state.open_windows.push(OpenChaosWindow {
            window: ev.window,
            pre_window_high_water,
            fence_freshness_checked,
        });
    }

    fn on_liveness(&mut self, incident: LivenessIncident) {
        let inside_any_window = self
            .state
            .open_windows
            .iter()
            .any(|open| open.window.contains(incident.at));
        if !inside_any_window {
            self.state.violations.push(Violation {
                kind: ViolationKind::Liveness { incident },
                at: Instant::now(),
            });
        }
    }
}

fn check_batch(
    violations: &mut Vec<Violation>,
    client_id: ClientId,
    batch_id: BatchId,
    values: Vec<Timestamp>,
) {
    // Strictly increasing AND contiguous (each ts[i+1].0 == ts[i].0 + 1, per
    // tsoracle batch contract).
    for pair in values.windows(2) {
        let prev = pair[0];
        let next = pair[1];
        if next.0 != prev.0 + 1 {
            violations.push(Violation {
                kind: ViolationKind::BatchInternalOrdering {
                    client_id,
                    batch_id,
                    values: values.clone(),
                    detail: format!("non-contiguous: {} → {}", prev.0, next.0),
                },
                at: Instant::now(),
            });
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::SupervisorEvent;
    use crate::sample::IssuedSample;
    use crate::types::ClientId;
    use crate::violation::ViolationKind;
    use std::time::Instant;
    use tokio::sync::mpsc;
    use tsoracle_core::Timestamp;

    fn sample(client: u32, ts_raw: u64) -> IssuedSample {
        let now = Instant::now();
        IssuedSample {
            client_id: ClientId(client),
            batch_id: 0,
            batch_idx: 0,
            is_last: true,
            ts: Timestamp(ts_raw),
            issued_at: now,
            recv_time: now,
        }
    }

    #[tokio::test]
    async fn monotonicity_clean_run() {
        let (tx, rx) = mpsc::channel::<SupervisorEvent>(16);
        let supervisor = Supervisor::new();
        let handle = tokio::spawn(supervisor.run(rx));
        tx.send(SupervisorEvent::Issued(sample(0, 1)))
            .await
            .unwrap();
        tx.send(SupervisorEvent::Issued(sample(0, 2)))
            .await
            .unwrap();
        tx.send(SupervisorEvent::Issued(sample(0, 3)))
            .await
            .unwrap();
        tx.send(SupervisorEvent::End).await.unwrap();
        drop(tx);
        let outcome = handle.await.unwrap();
        assert!(outcome.violations.is_empty(), "no violations expected");
    }

    #[tokio::test]
    async fn monotonicity_detects_duplicate() {
        let (tx, rx) = mpsc::channel::<SupervisorEvent>(16);
        let supervisor = Supervisor::new();
        let handle = tokio::spawn(supervisor.run(rx));
        tx.send(SupervisorEvent::Issued(sample(0, 100)))
            .await
            .unwrap();
        tx.send(SupervisorEvent::Issued(sample(1, 100)))
            .await
            .unwrap();
        tx.send(SupervisorEvent::End).await.unwrap();
        drop(tx);
        let outcome = handle.await.unwrap();
        assert_eq!(outcome.violations.len(), 1);
        assert!(matches!(
            outcome.violations[0].kind,
            ViolationKind::Monotonicity { .. }
        ));
    }

    #[tokio::test]
    async fn monotonicity_detects_regression() {
        let (tx, rx) = mpsc::channel::<SupervisorEvent>(16);
        let supervisor = Supervisor::new();
        let handle = tokio::spawn(supervisor.run(rx));
        tx.send(SupervisorEvent::Issued(sample(0, 10)))
            .await
            .unwrap();
        tx.send(SupervisorEvent::Issued(sample(0, 9)))
            .await
            .unwrap();
        tx.send(SupervisorEvent::End).await.unwrap();
        drop(tx);
        let outcome = handle.await.unwrap();
        assert_eq!(outcome.violations.len(), 1);
    }

    fn batch_sample(
        client: u32,
        batch_id: u32,
        idx: u32,
        is_last: bool,
        ts_raw: u64,
    ) -> IssuedSample {
        let now = Instant::now();
        IssuedSample {
            client_id: ClientId(client),
            batch_id,
            batch_idx: idx,
            is_last,
            ts: Timestamp(ts_raw),
            issued_at: now,
            recv_time: now,
        }
    }

    #[tokio::test]
    async fn batch_ordering_clean_contiguous_batch() {
        let (tx, rx) = mpsc::channel::<SupervisorEvent>(16);
        let supervisor = Supervisor::new();
        let handle = tokio::spawn(supervisor.run(rx));
        tx.send(SupervisorEvent::Issued(batch_sample(0, 1, 0, false, 10)))
            .await
            .unwrap();
        tx.send(SupervisorEvent::Issued(batch_sample(0, 1, 1, false, 11)))
            .await
            .unwrap();
        tx.send(SupervisorEvent::Issued(batch_sample(0, 1, 2, true, 12)))
            .await
            .unwrap();
        tx.send(SupervisorEvent::End).await.unwrap();
        drop(tx);
        let outcome = handle.await.unwrap();
        assert!(
            outcome.violations.is_empty(),
            "got {:?}",
            outcome.violations
        );
    }

    #[tokio::test]
    async fn batch_ordering_detects_gap() {
        let (tx, rx) = mpsc::channel::<SupervisorEvent>(16);
        let supervisor = Supervisor::new();
        let handle = tokio::spawn(supervisor.run(rx));
        tx.send(SupervisorEvent::Issued(batch_sample(0, 1, 0, false, 20)))
            .await
            .unwrap();
        tx.send(SupervisorEvent::Issued(batch_sample(0, 1, 1, false, 22)))
            .await
            .unwrap();
        tx.send(SupervisorEvent::Issued(batch_sample(0, 1, 2, true, 23)))
            .await
            .unwrap();
        tx.send(SupervisorEvent::End).await.unwrap();
        drop(tx);
        let outcome = handle.await.unwrap();
        let has_batch_violation = outcome
            .violations
            .iter()
            .any(|v| matches!(v.kind, ViolationKind::BatchInternalOrdering { .. }));
        assert!(
            has_batch_violation,
            "expected batch violation, got {:?}",
            outcome.violations
        );
    }

    use crate::chaos::{ChaosEvent, ChaosKind, ChaosOutcome, ChaosWindow};
    use std::time::Duration;

    fn kill_window(started: Instant, ended: Instant, grace: Duration) -> ChaosEvent {
        ChaosEvent {
            window: ChaosWindow {
                kind: ChaosKind::LeaderKill,
                started_at: started,
                ended_at: ended,
                grace,
            },
            outcome: ChaosOutcome::Applied,
        }
    }

    #[tokio::test]
    async fn fence_freshness_clean_post_window() {
        let (tx, rx) = mpsc::channel::<SupervisorEvent>(16);
        let supervisor = Supervisor::new();
        let handle = tokio::spawn(supervisor.run(rx));
        let t0 = Instant::now();
        tx.send(SupervisorEvent::Issued(sample(0, 100)))
            .await
            .unwrap();
        let started = t0;
        let ended = t0 + Duration::from_millis(50);
        let grace = Duration::from_millis(10);
        tx.send(SupervisorEvent::Chaos(kill_window(started, ended, grace)))
            .await
            .unwrap();
        let mut after = sample(0, 200);
        after.recv_time = ended + grace + Duration::from_millis(1);
        tx.send(SupervisorEvent::Issued(after)).await.unwrap();
        tx.send(SupervisorEvent::End).await.unwrap();
        drop(tx);
        let outcome = handle.await.unwrap();
        assert!(
            outcome.violations.is_empty(),
            "got {:?}",
            outcome.violations
        );
    }

    #[tokio::test]
    async fn fence_freshness_detects_regression() {
        let (tx, rx) = mpsc::channel::<SupervisorEvent>(16);
        let supervisor = Supervisor::new();
        let handle = tokio::spawn(supervisor.run(rx));
        let t0 = Instant::now();
        tx.send(SupervisorEvent::Issued(sample(0, 500)))
            .await
            .unwrap();
        let started = t0;
        let ended = t0 + Duration::from_millis(50);
        let grace = Duration::from_millis(10);
        tx.send(SupervisorEvent::Chaos(kill_window(started, ended, grace)))
            .await
            .unwrap();
        let mut after = sample(0, 500);
        after.ts = Timestamp(450);
        after.recv_time = ended + grace + Duration::from_millis(1);
        tx.send(SupervisorEvent::Issued(after)).await.unwrap();
        tx.send(SupervisorEvent::End).await.unwrap();
        drop(tx);
        let outcome = handle.await.unwrap();
        let has_fence = outcome
            .violations
            .iter()
            .any(|v| matches!(v.kind, ViolationKind::FenceFreshness { .. }));
        assert!(
            has_fence,
            "expected fence violation: {:?}",
            outcome.violations
        );
    }

    use crate::sample::{LivenessIncident, LivenessIncidentKind};

    #[tokio::test]
    async fn liveness_incident_outside_window_is_violation() {
        let (tx, rx) = mpsc::channel::<SupervisorEvent>(16);
        let supervisor = Supervisor::new();
        let handle = tokio::spawn(supervisor.run(rx));
        let now = Instant::now();
        tx.send(SupervisorEvent::Liveness(LivenessIncident {
            kind: LivenessIncidentKind::DeadlineExceeded {
                client_id: ClientId(0),
                attempts: 7,
                last_error: "Unavailable".into(),
                started_at: now,
            },
            at: now,
        }))
        .await
        .unwrap();
        tx.send(SupervisorEvent::End).await.unwrap();
        drop(tx);
        let outcome = handle.await.unwrap();
        assert_eq!(outcome.violations.len(), 1);
        assert!(matches!(
            outcome.violations[0].kind,
            ViolationKind::Liveness { .. }
        ));
    }

    #[tokio::test]
    async fn liveness_incident_inside_window_is_discarded() {
        let (tx, rx) = mpsc::channel::<SupervisorEvent>(16);
        let supervisor = Supervisor::new();
        let handle = tokio::spawn(supervisor.run(rx));
        let started = Instant::now();
        let ended = started + Duration::from_millis(100);
        let grace = Duration::from_millis(50);
        tx.send(SupervisorEvent::Chaos(kill_window(started, ended, grace)))
            .await
            .unwrap();
        tx.send(SupervisorEvent::Liveness(LivenessIncident {
            kind: LivenessIncidentKind::DeadlineExceeded {
                client_id: ClientId(0),
                attempts: 3,
                last_error: "Unavailable".into(),
                started_at: started,
            },
            at: started + Duration::from_millis(30),
        }))
        .await
        .unwrap();
        tx.send(SupervisorEvent::End).await.unwrap();
        drop(tx);
        let outcome = handle.await.unwrap();
        assert!(
            outcome.violations.is_empty(),
            "got: {:?}",
            outcome.violations
        );
    }

    #[tokio::test]
    async fn unexpected_server_exit_becomes_liveness_violation() {
        let (tx, rx) = mpsc::channel::<SupervisorEvent>(16);
        let supervisor = Supervisor::new();
        let handle = tokio::spawn(supervisor.run(rx));
        let now = Instant::now();
        tx.send(SupervisorEvent::Liveness(LivenessIncident {
            kind: LivenessIncidentKind::UnexpectedServerExit {
                pid: 12345,
                last_log_lines: vec!["thread 'main' panicked".into()],
            },
            at: now,
        }))
        .await
        .unwrap();
        tx.send(SupervisorEvent::End).await.unwrap();
        drop(tx);
        let outcome = handle.await.unwrap();
        assert_eq!(outcome.violations.len(), 1);
    }

    #[tokio::test]
    async fn open_batch_at_end_is_dropped_without_violation() {
        let (tx, rx) = mpsc::channel::<SupervisorEvent>(16);
        let supervisor = Supervisor::new();
        let handle = tokio::spawn(supervisor.run(rx));
        tx.send(SupervisorEvent::Issued(batch_sample(0, 1, 0, false, 100)))
            .await
            .unwrap();
        tx.send(SupervisorEvent::End).await.unwrap();
        drop(tx);
        let outcome = handle.await.unwrap();
        assert!(outcome.violations.is_empty());
    }

    #[tokio::test]
    async fn unsettled_chaos_window_at_end_no_violation() {
        let (tx, rx) = mpsc::channel::<SupervisorEvent>(16);
        let supervisor = Supervisor::new();
        let handle = tokio::spawn(supervisor.run(rx));
        let t0 = Instant::now();
        tx.send(SupervisorEvent::Issued(sample(0, 100)))
            .await
            .unwrap();
        let started = t0 + Duration::from_millis(10);
        let ended = t0 + Duration::from_millis(60);
        let grace = Duration::from_millis(10);
        tx.send(SupervisorEvent::Chaos(kill_window(started, ended, grace)))
            .await
            .unwrap();
        tx.send(SupervisorEvent::End).await.unwrap();
        drop(tx);
        let outcome = handle.await.unwrap();
        assert!(
            outcome.violations.is_empty(),
            "got: {:?}",
            outcome.violations
        );
    }
}
