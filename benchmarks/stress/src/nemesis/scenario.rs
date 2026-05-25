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

//! Named, hand-written scenarios.

use std::time::Duration;

use crate::chaos::ChaosOp;
use crate::schedule::{LoadgenPause, Schedule, ScheduleSource, ScheduledOp};

/// One named scenario for `--help` output and `list-scenarios`.
pub struct ScenarioInfo {
    pub name: &'static str,
    pub summary: &'static str,
}

pub fn catalog() -> Vec<ScenarioInfo> {
    vec![
        ScenarioInfo {
            name: "steady",
            summary: "Pure load, no chaos. Sanity-checks clean runs report as clean.",
        },
        ScenarioInfo {
            name: "burst",
            summary: "Load with a 5-second loadgen pause at t=30s. Tests resumption behavior.",
        },
        ScenarioInfo {
            name: "killer-loop",
            summary: "KillLeader every 2s. Stresses failover-fence under continuous churn.",
        },
        ScenarioInfo {
            name: "fence-stress",
            summary: "Alternating PauseLeader 500ms and KillLeader 1s apart. Fence interlock + re-election.",
        },
        ScenarioInfo {
            name: "failpoint-cycle",
            summary: "Arms each known failpoint for 5s, 10s recovery between. Driver fault recovery.",
        },
    ]
}

pub fn catalog_names() -> Vec<&'static str> {
    catalog().iter().map(|s| s.name).collect()
}

pub fn build(name: &str, total: Duration) -> Result<Schedule, String> {
    let source = ScheduleSource::Named {
        scenario: name.into(),
    };
    let (ops, loadgen_pause) = match name {
        "steady" => (Vec::new(), None),
        "burst" => (
            Vec::new(),
            Some(LoadgenPause {
                at: Duration::from_secs(30),
                dur: Duration::from_secs(5),
            }),
        ),
        "killer-loop" => (
            interval_ops(total, Duration::from_secs(2), |_| ChaosOp::KillLeader),
            None,
        ),
        "fence-stress" => {
            let mut ops = Vec::new();
            let mut t = Duration::from_secs(1);
            let mut kill_next = false;
            while t < total {
                ops.push(ScheduledOp {
                    at: t,
                    op: if kill_next {
                        ChaosOp::KillLeader
                    } else {
                        ChaosOp::PauseLeader {
                            dur: Duration::from_millis(500),
                        }
                    },
                });
                kill_next = !kill_next;
                t += Duration::from_secs(1);
            }
            (ops, None)
        }
        "failpoint-cycle" => {
            // Failpoint names mirror `docs/failpoint-testing.md` — keep this
            // list in sync if new failpoints land in tsoracle proper.
            let names = [
                "tsoracle::driver_file::write_record::after_fsync",
                "tsoracle::driver_file::write_record::before_fsync",
                "tsoracle::server::leader_watch::on_loss",
            ];
            let mut ops = Vec::new();
            let mut t = Duration::from_secs(1);
            let mut i = 0usize;
            while t < total {
                let name = names[i % names.len()];
                ops.push(ScheduledOp {
                    at: t,
                    op: ChaosOp::ArmFailpoint {
                        name: name.into(),
                        action: "panic".into(),
                    },
                });
                ops.push(ScheduledOp {
                    at: t + Duration::from_secs(5),
                    op: ChaosOp::DisarmFailpoint { name: name.into() },
                });
                t += Duration::from_secs(15);
                i += 1;
            }
            (ops, None)
        }
        other => return Err(format!("unknown scenario: {other}")),
    };
    Ok(Schedule {
        source,
        ops,
        total,
        loadgen_pause,
    })
}

fn interval_ops(
    total: Duration,
    gap: Duration,
    mut mk: impl FnMut(usize) -> ChaosOp,
) -> Vec<ScheduledOp> {
    let mut ops = Vec::new();
    let mut t = gap;
    let mut i = 0usize;
    while t < total {
        ops.push(ScheduledOp { at: t, op: mk(i) });
        t += gap;
        i += 1;
    }
    ops
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn catalog_contains_all_five() {
        let names = catalog_names();
        let expected = [
            "steady",
            "burst",
            "killer-loop",
            "fence-stress",
            "failpoint-cycle",
        ];
        for e in expected {
            assert!(names.contains(&e), "missing scenario: {e}, got: {names:?}");
        }
    }

    #[test]
    fn steady_has_no_ops() {
        let s = build("steady", Duration::from_secs(20)).unwrap();
        assert!(s.ops.is_empty());
        assert!(s.loadgen_pause.is_none());
    }

    #[test]
    fn burst_has_loadgen_pause() {
        let s = build("burst", Duration::from_secs(60)).unwrap();
        assert!(s.loadgen_pause.is_some());
    }

    #[test]
    fn killer_loop_kills_periodically() {
        let s = build("killer-loop", Duration::from_secs(20)).unwrap();
        assert!(s.ops.len() >= 8, "got {} ops", s.ops.len());
        for op in &s.ops {
            assert!(matches!(op.op, crate::chaos::ChaosOp::KillLeader));
        }
    }

    #[test]
    fn unknown_scenario_returns_err() {
        assert!(build("nonsense", Duration::from_secs(20)).is_err());
    }

    #[test]
    fn fence_stress_alternates_kill_and_pause() {
        let s = build("fence-stress", Duration::from_secs(10)).unwrap();
        assert!(s.ops.len() >= 2, "got {} ops", s.ops.len());
        assert!(s.loadgen_pause.is_none());
        let mut saw_kill = false;
        let mut saw_pause = false;
        for op in &s.ops {
            match &op.op {
                crate::chaos::ChaosOp::KillLeader => saw_kill = true,
                crate::chaos::ChaosOp::PauseLeader { dur } => {
                    saw_pause = true;
                    assert_eq!(*dur, Duration::from_millis(500));
                }
                other => panic!("unexpected op: {other:?}"),
            }
        }
        assert!(saw_kill && saw_pause, "expected both kill and pause ops");
    }

    #[test]
    fn failpoint_cycle_arms_and_disarms() {
        let s = build("failpoint-cycle", Duration::from_secs(40)).unwrap();
        assert!(s.loadgen_pause.is_none());
        let mut arm_count = 0;
        let mut disarm_count = 0;
        for op in &s.ops {
            match &op.op {
                crate::chaos::ChaosOp::ArmFailpoint { action, .. } => {
                    arm_count += 1;
                    assert_eq!(action, "panic");
                }
                crate::chaos::ChaosOp::DisarmFailpoint { .. } => disarm_count += 1,
                other => panic!("unexpected op: {other:?}"),
            }
        }
        assert!(arm_count > 0, "no arm ops emitted");
        assert_eq!(
            arm_count, disarm_count,
            "every arm must have a matching disarm",
        );
    }

    #[test]
    fn catalog_summaries_are_non_empty() {
        for info in catalog() {
            assert!(!info.summary.is_empty(), "{} has empty summary", info.name);
        }
    }
}
