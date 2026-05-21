//! Single-consumer task that checks all four invariants.

use std::time::Instant;

use tokio::sync::mpsc;
use tsoracle_core::Timestamp;

use crate::event::SupervisorEvent;
use crate::sample::IssuedSample;
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
    violations: Vec<Violation>,
    events_observed: u64,
}

impl SupervisorState {
    fn new() -> Self {
        Self {
            high_water: Timestamp(0),
            violations: Vec::new(),
            events_observed: 0,
        }
    }
}

impl Default for Supervisor {
    fn default() -> Self {
        Self::new()
    }
}

impl Supervisor {
    pub fn new() -> Self {
        Self {
            state: SupervisorState::new(),
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
}
