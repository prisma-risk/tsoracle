//! Plays a `Schedule` against a `ChaosController`.

pub mod random;
pub mod scenario;

use std::time::Instant;

use tokio::sync::mpsc;
use tracing::warn;

use crate::chaos::ChaosOp;
use crate::event::SupervisorEvent;
use crate::schedule::{Schedule, ScheduledOp};
use crate::topology::ChaosController;

/// Play a schedule against `controller`, pushing each resulting `ChaosEvent`
/// into `tx`. Honors `t0` so the schedule's relative `at` values are measured
/// from the barrier release, not from `play`'s entry.
///
/// Skipped/Failed chaos ops are logged via `tracing::warn!` and do NOT
/// produce a SupervisorEvent — only Applied windows reach the supervisor.
/// (The chaos_events vec in the final Report carries the full history,
/// including skipped ones; that's wired by lib::run in T21.)
pub async fn play(
    schedule: &Schedule,
    controller: &dyn ChaosController,
    tx: mpsc::Sender<SupervisorEvent>,
    t0: Instant,
) {
    for ScheduledOp { at, op } in &schedule.ops {
        let target = t0 + *at;
        let now = Instant::now();
        if target > now {
            tokio::time::sleep(target - now).await;
        }
        let event = dispatch(controller, op).await;
        if event.outcome.is_applied() {
            if tx.send(SupervisorEvent::Chaos(event)).await.is_err() {
                break;
            }
        } else {
            warn!(?event, "nemesis op not applied");
        }
    }
}

async fn dispatch(controller: &dyn ChaosController, op: &ChaosOp) -> crate::chaos::ChaosEvent {
    match op {
        ChaosOp::KillLeader => controller.kill_leader().await,
        ChaosOp::PauseLeader { dur } => controller.pause_leader(*dur).await,
        ChaosOp::ArmFailpoint { name, action } => controller.arm_failpoint(name, action).await,
        ChaosOp::DisarmFailpoint { name } => controller.disarm_failpoint(name).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chaos::{ChaosEvent, ChaosKind, ChaosOp, ChaosOutcome, ChaosWindow};
    use crate::schedule::{Schedule, ScheduleSource};
    use crate::topology::NodeId;
    use async_trait::async_trait;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    struct FakeController {
        kills: AtomicU32,
        pauses: AtomicU32,
        events: Mutex<Vec<String>>,
    }

    impl FakeController {
        fn new() -> Self {
            Self {
                kills: AtomicU32::new(0),
                pauses: AtomicU32::new(0),
                events: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl ChaosController for FakeController {
        async fn kill_leader(&self) -> ChaosEvent {
            self.kills.fetch_add(1, Ordering::Relaxed);
            self.events.lock().unwrap().push("kill".into());
            timed_chaos(ChaosKind::LeaderKill).await
        }
        async fn pause_leader(&self, _: Duration) -> ChaosEvent {
            self.pauses.fetch_add(1, Ordering::Relaxed);
            self.events.lock().unwrap().push("pause".into());
            timed_chaos(ChaosKind::LeaderPause).await
        }
        async fn arm_failpoint(&self, name: &str, _: &str) -> ChaosEvent {
            timed_chaos(ChaosKind::FailpointArm { name: name.into() }).await
        }
        async fn disarm_failpoint(&self, name: &str) -> ChaosEvent {
            timed_chaos(ChaosKind::FailpointDisarm { name: name.into() }).await
        }
        fn endpoints(&self) -> Vec<String> {
            vec![]
        }
        fn current_leader(&self) -> Option<NodeId> {
            Some(NodeId(0))
        }
        async fn shutdown(self: Box<Self>) {}
    }

    async fn timed_chaos(kind: ChaosKind) -> ChaosEvent {
        let now = Instant::now();
        ChaosEvent {
            window: ChaosWindow {
                kind,
                started_at: now,
                ended_at: now,
                grace: Duration::ZERO,
            },
            outcome: ChaosOutcome::Applied,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn playback_dispatches_ops_in_order() {
        let schedule = Schedule {
            source: ScheduleSource::Named {
                scenario: "test".into(),
            },
            ops: vec![
                ScheduledOp {
                    at: Duration::from_millis(10),
                    op: ChaosOp::KillLeader,
                },
                ScheduledOp {
                    at: Duration::from_millis(20),
                    op: ChaosOp::PauseLeader {
                        dur: Duration::from_millis(5),
                    },
                },
                ScheduledOp {
                    at: Duration::from_millis(30),
                    op: ChaosOp::KillLeader,
                },
            ],
            total: Duration::from_millis(50),
            loadgen_pause: None,
        };
        let controller = FakeController::new();
        let (tx, mut rx) = mpsc::channel::<SupervisorEvent>(16);
        let t0 = Instant::now();
        let handle = tokio::spawn(async move {
            play(&schedule, &controller, tx, t0).await;
            controller
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let controller = handle.await.unwrap();
        assert_eq!(controller.kills.load(Ordering::Relaxed), 2);
        assert_eq!(controller.pauses.load(Ordering::Relaxed), 1);
        let mut count = 0;
        while rx.try_recv().is_ok() {
            count += 1;
        }
        assert_eq!(count, 3);
    }
}
