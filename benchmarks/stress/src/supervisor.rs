//! Single-consumer task that checks all four invariants.

use std::collections::HashMap;
use std::time::Instant;

use tokio::sync::mpsc;
use tsoracle_core::Timestamp;

use crate::event::SupervisorEvent;
use crate::sample::IssuedSample;
use crate::types::{BatchId, ClientId};
use crate::violation::{Violation, ViolationKind};

/// What the supervisor returns at end of run.
#[derive(Debug, Clone)]
pub struct SupervisorOutcome {
    pub violations: Vec<Violation>,
    pub high_water: Timestamp,
    pub events_observed: u64,
}

pub struct Supervisor {
    state: SupervisorState,
}

#[derive(Debug)]
struct SupervisorState {
    high_water: Timestamp,
    open_batches: HashMap<(ClientId, BatchId), OpenBatch>,
    violations: Vec<Violation>,
    events_observed: u64,
}

impl Default for SupervisorState {
    fn default() -> Self {
        Self {
            high_water: Timestamp(0),
            open_batches: HashMap::new(),
            violations: Vec::new(),
            events_observed: 0,
        }
    }
}

#[derive(Debug)]
struct OpenBatch {
    values: Vec<Timestamp>,
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
                SupervisorEvent::Chaos(_) => {}    // Task 8
                SupervisorEvent::Liveness(_) => {} // Task 9
                SupervisorEvent::End => break,
            }
        }
        SupervisorOutcome {
            violations: self.state.violations,
            high_water: self.state.high_water,
            events_observed: self.state.events_observed,
        }
    }

    fn on_issued(&mut self, sample: IssuedSample) {
        // (1) Global monotonicity (unchanged from Task 6).
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
        tx.send(SupervisorEvent::Issued(sample(0, 1))).await.unwrap();
        tx.send(SupervisorEvent::Issued(sample(0, 2))).await.unwrap();
        tx.send(SupervisorEvent::Issued(sample(0, 3))).await.unwrap();
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
        tx.send(SupervisorEvent::Issued(sample(0, 100))).await.unwrap();
        tx.send(SupervisorEvent::Issued(sample(1, 100))).await.unwrap();
        tx.send(SupervisorEvent::End).await.unwrap();
        drop(tx);
        let outcome = handle.await.unwrap();
        assert_eq!(outcome.violations.len(), 1);
        assert!(matches!(outcome.violations[0].kind, ViolationKind::Monotonicity { .. }));
    }

    #[tokio::test]
    async fn monotonicity_detects_regression() {
        let (tx, rx) = mpsc::channel::<SupervisorEvent>(16);
        let supervisor = Supervisor::new();
        let handle = tokio::spawn(supervisor.run(rx));
        tx.send(SupervisorEvent::Issued(sample(0, 10))).await.unwrap();
        tx.send(SupervisorEvent::Issued(sample(0, 9))).await.unwrap();
        tx.send(SupervisorEvent::End).await.unwrap();
        drop(tx);
        let outcome = handle.await.unwrap();
        assert_eq!(outcome.violations.len(), 1);
    }

    fn batch_sample(client: u32, batch_id: u32, idx: u32, is_last: bool, ts_raw: u64) -> IssuedSample {
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
        tx.send(SupervisorEvent::Issued(batch_sample(0, 1, 0, false, 10))).await.unwrap();
        tx.send(SupervisorEvent::Issued(batch_sample(0, 1, 1, false, 11))).await.unwrap();
        tx.send(SupervisorEvent::Issued(batch_sample(0, 1, 2, true,  12))).await.unwrap();
        tx.send(SupervisorEvent::End).await.unwrap();
        drop(tx);
        let outcome = handle.await.unwrap();
        assert!(outcome.violations.is_empty(), "got {:?}", outcome.violations);
    }

    #[tokio::test]
    async fn batch_ordering_detects_gap() {
        let (tx, rx) = mpsc::channel::<SupervisorEvent>(16);
        let supervisor = Supervisor::new();
        let handle = tokio::spawn(supervisor.run(rx));
        tx.send(SupervisorEvent::Issued(batch_sample(0, 1, 0, false, 20))).await.unwrap();
        tx.send(SupervisorEvent::Issued(batch_sample(0, 1, 1, false, 22))).await.unwrap();
        tx.send(SupervisorEvent::Issued(batch_sample(0, 1, 2, true,  23))).await.unwrap();
        tx.send(SupervisorEvent::End).await.unwrap();
        drop(tx);
        let outcome = handle.await.unwrap();
        let has_batch_violation = outcome.violations.iter().any(|v| {
            matches!(v.kind, ViolationKind::BatchInternalOrdering { .. })
        });
        assert!(has_batch_violation, "expected batch violation, got {:?}", outcome.violations);
    }
}
