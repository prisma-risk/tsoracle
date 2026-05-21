//! Server topology abstractions: spawn, chaos vocabulary, endpoints.

pub mod mem;
pub mod process;
pub mod raft;

use std::time::{Duration, Instant};

use async_trait::async_trait;

use crate::chaos::{ChaosEvent, ChaosKind, ChaosOutcome, ChaosWindow};

/// Best-effort identifier for a node within a topology. Mem uses always `0`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(pub u32);

#[async_trait]
pub trait ChaosController: Send + Sync {
    async fn kill_leader(&self) -> ChaosEvent;
    async fn pause_leader(&self, dur: Duration) -> ChaosEvent;
    async fn arm_failpoint(&self, name: &str, action: &str) -> ChaosEvent;
    async fn disarm_failpoint(&self, name: &str) -> ChaosEvent;

    fn endpoints(&self) -> Vec<String>;
    fn current_leader(&self) -> Option<NodeId>;
    async fn shutdown(self: Box<Self>);
}

/// Helper for topology impls: build a `ChaosEvent` with `started_at`/`ended_at`
/// captured around the closure body. Each topology's chaos methods build
/// their ChaosEvents through this helper.
pub async fn timed_event<F, Fut>(kind: ChaosKind, grace: Duration, f: F) -> ChaosEvent
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = ChaosOutcome>,
{
    let started_at = Instant::now();
    let outcome = f().await;
    let ended_at = Instant::now();
    ChaosEvent {
        window: ChaosWindow { kind, started_at, ended_at, grace },
        outcome,
    }
}
