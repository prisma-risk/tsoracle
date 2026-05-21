//! Single-consumer task that checks all four invariants.

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use hdrhistogram::Histogram;
use tokio::sync::mpsc;
use tsoracle_core::Timestamp;

use crate::chaos::{ChaosEvent, ChaosKind, ChaosWindow};
use crate::event::SupervisorEvent;
use crate::sample::{IssuedSample, LivenessIncident, LivenessIncidentKind};
use crate::types::{BatchId, ClientId};
use crate::violation::{Violation, ViolationKind};
use crate::{HISTO_MAX_US, new_histogram};

/// What the supervisor returns at end of run.
#[derive(Debug, Clone)]
pub struct SupervisorOutcome {
    pub violations: Vec<Violation>,
    pub high_water: Timestamp,
    /// Count of `SupervisorEvent::Issued` events received — i.e. real
    /// timestamps returned to clients. Excludes `Chaos`, `Liveness`, and
    /// `End` events, which would otherwise inflate the count under chaos.
    pub issued_observed: u64,
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
    /// Maximum `ts` observed across all clients in arrival order. Used by the
    /// fence-freshness check as the pre-window snapshot. NOT used by the
    /// per-sample monotonicity check — arrival order at the supervisor's mpsc
    /// is not issue order at the TSO under concurrent clients, so a global
    /// arrival-order check would flag legitimate concurrent reorderings as
    /// regressions.
    high_water: Timestamp,
    /// Per-client maximum `ts` observed. A single client's RPCs are
    /// causally ordered (each awaits the previous), so a client never
    /// legitimately receives a `ts` ≤ its previous one — that is the actual
    /// per-client monotonicity invariant.
    per_client_high_water: HashMap<ClientId, Timestamp>,
    /// All observed timestamps. A duplicate insert is a cross-client
    /// uniqueness violation (the TSO must never issue the same `ts` twice).
    seen_timestamps: HashSet<Timestamp>,
    open_batches: HashMap<(ClientId, BatchId), OpenBatch>,
    open_windows: Vec<OpenChaosWindow>,
    /// Every applied chaos window seen so far, never pruned. Used only by
    /// `on_liveness` to retroactively attribute a `DeadlineExceeded`
    /// incident to chaos that overlapped the client's retry interval —
    /// `open_windows` gets pruned in `on_issued` once a window is past
    /// grace and fence-checked, so it can't be relied on after the fact.
    chaos_history: Vec<ChaosWindow>,
    violations: Vec<Violation>,
    issued_observed: u64,
    latency: Histogram<u64>,
}

impl Default for SupervisorState {
    fn default() -> Self {
        Self {
            high_water: Timestamp(0),
            per_client_high_water: HashMap::new(),
            seen_timestamps: HashSet::new(),
            open_batches: HashMap::new(),
            open_windows: Vec::new(),
            chaos_history: Vec::new(),
            violations: Vec::new(),
            issued_observed: 0,
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
            match event {
                SupervisorEvent::Issued(sample) => {
                    self.state.issued_observed += 1;
                    self.on_issued(sample);
                }
                SupervisorEvent::Chaos(ev) => self.on_chaos(ev),
                SupervisorEvent::Liveness(incident) => self.on_liveness(incident),
                SupervisorEvent::End => break,
            }
        }
        self.final_pass();
        SupervisorOutcome {
            violations: self.state.violations,
            high_water: self.state.high_water,
            issued_observed: self.state.issued_observed,
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

        // (1) Monotonicity — split into two client-observable invariants:
        //
        //   (1a) Per-client monotonicity. A client awaits each RPC before
        //   starting the next, so successive samples from the same client
        //   must strictly increase. Any tie or regression is a real TSO bug.
        //
        //   (1b) Cross-client uniqueness. The TSO must never issue the same
        //   `ts` twice. A duplicate `ts` across clients is a real TSO bug.
        //
        // We deliberately do NOT compare each sample against the global
        // arrival-order high-water: the supervisor mpsc has multiple
        // concurrent producers (one per client task), so arrival order at
        // this consumer is not issue order at the TSO. Under the raft +
        // killer-loop scenario, 8 stalled RPCs can unblock together with
        // contiguous ts assigned in issue order yet land at the supervisor
        // in an arbitrary permutation — that is legitimate, not a TSO
        // regression. The fence-freshness check (1.5 below) is the right
        // place for "no regression across a leadership change".
        let prev = self
            .state
            .per_client_high_water
            .get(&sample.client_id)
            .copied()
            .unwrap_or(Timestamp(0));
        if sample.ts <= prev {
            self.state.violations.push(Violation {
                kind: ViolationKind::Monotonicity {
                    prev,
                    got: sample.ts,
                    sample: sample.clone(),
                },
                at: Instant::now(),
            });
        } else {
            self.state
                .per_client_high_water
                .insert(sample.client_id, sample.ts);
        }
        if !self.state.seen_timestamps.insert(sample.ts) {
            self.state.violations.push(Violation {
                kind: ViolationKind::Monotonicity {
                    prev: sample.ts,
                    got: sample.ts,
                    sample: sample.clone(),
                },
                at: Instant::now(),
            });
        }
        if sample.ts > self.state.high_water {
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
            // Failpoint windows participate only in liveness gating; they don't
            // change leadership so no fence check applies.
            ChaosKind::FailpointArm { .. } | ChaosKind::FailpointDisarm { .. } => true,
        };
        self.state.chaos_history.push(ev.window.clone());
        self.state.open_windows.push(OpenChaosWindow {
            window: ev.window,
            pre_window_high_water,
            fence_freshness_checked,
        });
    }

    fn on_liveness(&mut self, incident: LivenessIncident) {
        // `DeadlineExceeded` is a retry-interval event: the client tried
        // repeatedly across [started_at, incident.at) and never succeeded.
        // Any applied chaos window whose live span
        // [w.started_at, w.ended_at + w.grace) overlapped that retry
        // interval was a legitimate cause of the unavailability — including
        // windows that closed before the deadline fired (e.g. consecutive
        // `killer-loop` kills whose grace tails pre-date the incident, but
        // whose churn prevented the cluster from ever stabilizing during
        // the retry). `UnexpectedServerExit` has no retry interval, so the
        // point-in-time containment check is the right shape there.
        let chaos_attributable = match &incident.kind {
            LivenessIncidentKind::DeadlineExceeded { started_at, .. } => {
                let retry_start = *started_at;
                let retry_end = incident.at;
                self.state
                    .chaos_history
                    .iter()
                    .any(|w| w.started_at < retry_end && retry_start < w.ended_at + w.grace)
            }
            LivenessIncidentKind::UnexpectedServerExit { .. } => self
                .state
                .chaos_history
                .iter()
                .any(|w| w.contains(incident.at)),
        };
        if !chaos_attributable {
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
        assert_eq!(outcome.issued_observed, 3);
    }

    #[tokio::test]
    async fn issued_observed_excludes_non_issued_events() {
        use crate::chaos::{ChaosEvent, ChaosKind, ChaosOutcome, ChaosWindow};
        use crate::sample::{LivenessIncident, LivenessIncidentKind};
        use std::time::Duration;
        // Mix 2 Issued with 1 Chaos + 1 Liveness; the supervisor must count
        // only the 2 Issued events as `issued_observed`. Pre-fix, the
        // counter incremented for every mpsc event variant, inflating the
        // user-visible "timestamps" by every chaos and liveness signal.
        let (tx, rx) = mpsc::channel::<SupervisorEvent>(16);
        let supervisor = Supervisor::new();
        let handle = tokio::spawn(supervisor.run(rx));
        tx.send(SupervisorEvent::Issued(sample(0, 1)))
            .await
            .unwrap();
        let now = Instant::now();
        tx.send(SupervisorEvent::Chaos(ChaosEvent {
            window: ChaosWindow {
                kind: ChaosKind::LeaderKill,
                started_at: now,
                ended_at: now + Duration::from_millis(10),
                grace: Duration::from_millis(50),
            },
            outcome: ChaosOutcome::Applied,
        }))
        .await
        .unwrap();
        tx.send(SupervisorEvent::Issued(sample(0, 2)))
            .await
            .unwrap();
        tx.send(SupervisorEvent::Liveness(LivenessIncident {
            kind: LivenessIncidentKind::DeadlineExceeded {
                client_id: ClientId(0),
                attempts: 3,
                last_error: "transient".into(),
                started_at: now,
            },
            at: Instant::now(),
        }))
        .await
        .unwrap();
        tx.send(SupervisorEvent::End).await.unwrap();
        drop(tx);
        let outcome = handle.await.unwrap();
        assert_eq!(
            outcome.issued_observed, 2,
            "must count only Issued events, not Chaos/Liveness"
        );
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
    async fn monotonicity_tolerates_cross_client_arrival_reorder() {
        // Reproduces the raft + killer-loop scenario where 8 concurrent
        // clients' stalled RPCs unblock together: the TSO assigns
        // contiguous, monotonically increasing ts in issue order, but the
        // per-client `IssuedSample`s land at the supervisor's mpsc in an
        // arbitrary permutation. That is legitimate observed behavior, not
        // a TSO regression — the supervisor must not flag it.
        let (tx, rx) = mpsc::channel::<SupervisorEvent>(16);
        let supervisor = Supervisor::new();
        let handle = tokio::spawn(supervisor.run(rx));
        // Client 7's sample arrives first with the highest ts.
        tx.send(SupervisorEvent::Issued(sample(7, 551)))
            .await
            .unwrap();
        // Clients 0..6 then post their (lower) samples in arbitrary order.
        for (client, ts) in [
            (3, 547),
            (0, 544),
            (6, 550),
            (1, 545),
            (4, 548),
            (2, 546),
            (5, 549),
        ] {
            tx.send(SupervisorEvent::Issued(sample(client, ts)))
                .await
                .unwrap();
        }
        tx.send(SupervisorEvent::End).await.unwrap();
        drop(tx);
        let outcome = handle.await.unwrap();
        assert!(
            outcome.violations.is_empty(),
            "got: {:?}",
            outcome.violations,
        );
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
    async fn liveness_deadline_exceeded_during_chaos_churn_is_tolerated() {
        // Reproduces the raft + killer-loop scenario where a client's
        // retry interval [started_at, incident.at) spans multiple kill
        // windows that have all closed (past `grace`) by the time the
        // deadline fires. The incident's instantaneous `at` falls in a
        // chaos-window gap, so a strict point-in-time `contains` check
        // would flag it as a violation — but the churn during the retry
        // interval is exactly what prevented progress, so it must NOT
        // be reported as a TSO liveness regression.
        let (tx, rx) = mpsc::channel::<SupervisorEvent>(16);
        let supervisor = Supervisor::new();
        let handle = tokio::spawn(supervisor.run(rx));
        let t0 = Instant::now();
        // Three kill windows at t+1s, t+3s, t+5s, each with 100ms duration
        // and 750ms grace. After t+5.85s no window is "open" anymore.
        for k in [1, 3, 5] {
            let started = t0 + Duration::from_secs(k);
            let ended = started + Duration::from_millis(100);
            let grace = Duration::from_millis(750);
            tx.send(SupervisorEvent::Chaos(kill_window(started, ended, grace)))
                .await
                .unwrap();
        }
        // Client started retrying at t+0.5s and deadlined at t+5.5s
        // (5s `liveness_deadline`). Last kill window ended its grace at
        // t+5.85s, so the deadline at t+5.5s falls within the last
        // window — but suppose the deadline fired at t+6.0s instead,
        // outside every window's grace. We test that case.
        let started_at = t0 + Duration::from_millis(500);
        let at = t0 + Duration::from_secs(6); // > last window's end+grace (5.85s)
        tx.send(SupervisorEvent::Liveness(LivenessIncident {
            kind: LivenessIncidentKind::DeadlineExceeded {
                client_id: ClientId(0),
                attempts: 1379,
                last_error: "Rpc(Status { code: FailedPrecondition, message: \"not leader\" })"
                    .into(),
                started_at,
            },
            at,
        }))
        .await
        .unwrap();
        tx.send(SupervisorEvent::End).await.unwrap();
        drop(tx);
        let outcome = handle.await.unwrap();
        assert!(
            outcome.violations.is_empty(),
            "got: {:?}",
            outcome.violations,
        );
    }

    #[tokio::test]
    async fn liveness_deadline_exceeded_with_no_overlapping_chaos_is_violation() {
        // A retry interval that does NOT overlap with any chaos window
        // must still be reported as a liveness violation — the fix
        // tolerates chaos-attributable failures, not all failures.
        let (tx, rx) = mpsc::channel::<SupervisorEvent>(16);
        let supervisor = Supervisor::new();
        let handle = tokio::spawn(supervisor.run(rx));
        let t0 = Instant::now();
        // One kill window early in the run.
        let started = t0;
        let ended = started + Duration::from_millis(100);
        let grace = Duration::from_millis(50);
        tx.send(SupervisorEvent::Chaos(kill_window(started, ended, grace)))
            .await
            .unwrap();
        // Client started retrying well after the kill's grace ended.
        let retry_start = t0 + Duration::from_secs(1);
        let retry_end = t0 + Duration::from_secs(6);
        tx.send(SupervisorEvent::Liveness(LivenessIncident {
            kind: LivenessIncidentKind::DeadlineExceeded {
                client_id: ClientId(0),
                attempts: 10,
                last_error: "Unavailable".into(),
                started_at: retry_start,
            },
            at: retry_end,
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
