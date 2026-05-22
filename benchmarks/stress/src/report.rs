//
//  ░▀█▀░█▀▀░█▀█░█▀▄░█▀█░█▀▀░█░░░█▀▀
//  ░░█░░▀▀█░█░█░█▀▄░█▀█░█░░░█░░░█▀▀
//  ░░▀░░▀▀▀░▀▀▀░▀░▀░▀░▀░▀▀▀░▀▀▀░▀▀▀
//
//  tsoracle — Distributed Timestamp Oracle
//
//  Copyright (c) 2026 Prisma Risk
//  Licensed under the Apache License, Version 2.0
//  https://github.com/prisma-risk/tsoracle
//

//! Final report: outcome, latency, throughput, violations, chaos events.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::chaos::ChaosEvent;
use crate::config::{StressConfig, TopologyKind};
use crate::git::GitInfo;
use crate::schedule::Schedule;
use crate::violation::Violation;

#[derive(Debug, Clone)]
pub struct Report {
    pub config: StressConfig,
    pub git: GitInfo,
    pub hostname: String,
    pub topology: TopologyKind,
    pub elapsed: Duration,
    pub recorded: RecordedCounts,
    pub throughput: Throughput,
    pub latency_per_call_us: LatencyStats,
    pub transient_retries: u64,
    pub out_of_range_samples: u64,
    pub violations: Vec<Violation>,
    pub chaos_events: Vec<ChaosEvent>,
    pub schedule: Schedule,
    pub outcome: Outcome,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct RecordedCounts {
    pub client_calls: u64,
    pub timestamps: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Throughput {
    pub client_calls_per_sec: f64,
    pub timestamps_per_sec: f64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct LatencyStats {
    pub p50: u64,
    pub p90: u64,
    pub p99: u64,
    pub p999: u64,
    pub min: u64,
    pub max: u64,
    pub mean: u64,
}

#[derive(Debug, Clone)]
pub enum Outcome {
    Ok,
    InvariantViolation,
    ProgrammerError { reason: String },
    HarnessError { kind: HarnessErrorKind },
    Interrupted,
}

#[derive(Debug, Clone)]
pub enum HarnessErrorKind {
    ServerPanic {
        topology: TopologyKind,
        detail: String,
    },
    SpawnFailure {
        topology: TopologyKind,
        detail: String,
    },
    TokioTaskPanic {
        task: &'static str,
        detail: String,
    },
    HostFault {
        detail: String,
    },
}

impl Outcome {
    /// Map `Outcome` to the process exit code documented in the spec.
    pub fn exit_code(&self) -> i32 {
        match self {
            Outcome::Ok => 0,
            Outcome::InvariantViolation => 1,
            Outcome::ProgrammerError { .. } => 2,
            Outcome::HarnessError { .. } => 3,
            Outcome::Interrupted => 130,
        }
    }
}

impl Report {
    pub fn render_text(&self) -> String {
        let outcome_str = match &self.outcome {
            Outcome::Ok => "Ok".to_string(),
            Outcome::InvariantViolation => "InvariantViolation".to_string(),
            Outcome::ProgrammerError { reason } => format!("ProgrammerError: {reason}"),
            Outcome::HarnessError { kind } => format!("HarnessError: {kind:?}"),
            Outcome::Interrupted => "Interrupted".to_string(),
        };
        let topo = match self.topology {
            TopologyKind::Mem => "Mem",
            TopologyKind::Raft => "Raft",
            TopologyKind::Process => "Process",
        };
        format!(
            "tsoracle stress — git rev {} (dirty={}), hostname={} topology={topo}\n\
             outcome={outcome_str}\n\
             elapsed:       {:.3} s\n\
             recorded:      client_calls={} timestamps={}\n\
             throughput:    client_calls/s: {:.2}        timestamps/s: {:.2}\n\
             latency per client call:\n  \
             p50: {} µs          p90: {} µs           p99: {} µs           p999: {} µs\n  \
             min: {} µs           max: {} µs          mean: {} µs\n\
             transient retries:    {}\n\
             out-of-range samples: {}\n\
             violations: {}\n\
             chaos events: {}\n",
            self.git.rev,
            self.git.dirty,
            self.hostname,
            self.elapsed.as_secs_f64(),
            self.recorded.client_calls,
            self.recorded.timestamps,
            self.throughput.client_calls_per_sec,
            self.throughput.timestamps_per_sec,
            self.latency_per_call_us.p50,
            self.latency_per_call_us.p90,
            self.latency_per_call_us.p99,
            self.latency_per_call_us.p999,
            self.latency_per_call_us.min,
            self.latency_per_call_us.max,
            self.latency_per_call_us.mean,
            self.transient_retries,
            self.out_of_range_samples,
            self.violations.len(),
            self.chaos_events.len(),
        )
    }

    pub fn render_json(&self) -> String {
        let outcome_str = match &self.outcome {
            Outcome::Ok => "Ok",
            Outcome::InvariantViolation => "InvariantViolation",
            Outcome::ProgrammerError { .. } => "ProgrammerError",
            Outcome::HarnessError { .. } => "HarnessError",
            Outcome::Interrupted => "Interrupted",
        };
        let topology = match self.topology {
            TopologyKind::Mem => "Mem",
            TopologyKind::Raft => "Raft",
            TopologyKind::Process => "Process",
        };
        let value = serde_json::json!({
            "outcome": outcome_str,
            "topology": topology,
            "git_rev": self.git.rev,
            "git_dirty": self.git.dirty,
            "hostname": self.hostname,
            "elapsed_s": self.elapsed.as_secs_f64(),
            "recorded": {
                "client_calls": self.recorded.client_calls,
                "timestamps": self.recorded.timestamps,
            },
            "throughput": {
                "client_calls_per_sec": self.throughput.client_calls_per_sec,
                "timestamps_per_sec": self.throughput.timestamps_per_sec,
            },
            "latency_per_call_us": self.latency_per_call_us,
            "transient_retries": self.transient_retries,
            "out_of_range_samples": self.out_of_range_samples,
            "violations": self.violations.iter().map(violation_summary).collect::<Vec<_>>(),
            "chaos_events": self.chaos_events.iter().map(chaos_event_summary).collect::<Vec<_>>(),
            "schedule_summary": {
                "source": match &self.schedule.source {
                    crate::schedule::ScheduleSource::Named { scenario } => format!("named:{scenario}"),
                    crate::schedule::ScheduleSource::Random { seed, .. } => format!("random:{seed}"),
                },
                "op_count": self.schedule.ops.len(),
            },
        });
        value.to_string()
    }
}

fn chaos_event_summary(ev: &crate::chaos::ChaosEvent) -> serde_json::Value {
    use crate::chaos::{ChaosKind, ChaosOutcome};
    // `Instant` isn't serializable and ms-since-Unix-epoch isn't recoverable
    // from `Instant`. We surface only what's useful for triage: the kind
    // (KillLeader / PauseLeader / FailpointArm{name} / FailpointDisarm{name})
    // and the outcome label. Window timing is implicit in the schedule.
    let kind = match &ev.window.kind {
        ChaosKind::LeaderKill => serde_json::json!({ "kind": "LeaderKill" }),
        ChaosKind::LeaderPause => serde_json::json!({ "kind": "LeaderPause" }),
        ChaosKind::FailpointArm { name } => {
            serde_json::json!({ "kind": "FailpointArm", "name": name })
        }
        ChaosKind::FailpointDisarm { name } => {
            serde_json::json!({ "kind": "FailpointDisarm", "name": name })
        }
    };
    let outcome = match &ev.outcome {
        ChaosOutcome::Applied => serde_json::json!({ "outcome": "Applied" }),
        ChaosOutcome::Skipped { reason } => {
            serde_json::json!({ "outcome": "Skipped", "reason": reason })
        }
        ChaosOutcome::Failed { reason } => {
            serde_json::json!({ "outcome": "Failed", "reason": reason })
        }
    };
    let mut out = kind;
    if let (Some(out_obj), Some(outcome_obj)) = (out.as_object_mut(), outcome.as_object()) {
        for (k, v) in outcome_obj {
            out_obj.insert(k.clone(), v.clone());
        }
    }
    out
}

fn violation_summary(v: &crate::violation::Violation) -> serde_json::Value {
    use crate::violation::ViolationKind;
    match &v.kind {
        ViolationKind::Monotonicity { prev, got, .. } => {
            serde_json::json!({ "kind": "Monotonicity", "prev": prev.0, "got": got.0 })
        }
        ViolationKind::BatchInternalOrdering {
            client_id,
            batch_id,
            detail,
            ..
        } => {
            serde_json::json!({
                "kind": "BatchInternalOrdering",
                "client_id": client_id.0,
                "batch_id": batch_id,
                "detail": detail,
            })
        }
        ViolationKind::FenceFreshness {
            pre_window_high_water,
            first_post_window_ts,
            ..
        } => {
            serde_json::json!({
                "kind": "FenceFreshness",
                "pre_window_high_water": pre_window_high_water.0,
                "first_post_window_ts": first_post_window_ts.0,
            })
        }
        ViolationKind::Liveness { incident } => {
            serde_json::json!({ "kind": "Liveness", "incident": format!("{:?}", incident.kind) })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ScenarioKind, TopologyKind};
    use crate::schedule::{Schedule, ScheduleSource};
    use std::net::SocketAddr;
    use std::time::Duration;

    #[test]
    fn exit_code_mapping() {
        assert_eq!(Outcome::Ok.exit_code(), 0);
        assert_eq!(Outcome::InvariantViolation.exit_code(), 1);
        assert_eq!(
            Outcome::ProgrammerError { reason: "x".into() }.exit_code(),
            2
        );
        assert_eq!(
            Outcome::HarnessError {
                kind: HarnessErrorKind::HostFault { detail: "y".into() }
            }
            .exit_code(),
            3
        );
        assert_eq!(Outcome::Interrupted.exit_code(), 130);
    }

    fn sample_report() -> Report {
        Report {
            config: StressConfig {
                topology: TopologyKind::Mem,
                scenario: ScenarioKind::Named("steady".into()),
                duration: Some(Duration::from_secs(20)),
                ops: None,
                clients: 16,
                batch_size: 4,
                warmup: 1000,
                client_threads: 1,
                server_threads: 4,
                liveness_deadline: Duration::from_secs(5),
                grace_mem: Duration::from_millis(100),
                grace_raft: Duration::from_millis(750),
                grace_process: Duration::from_secs(2),
                nodes: 1,
                bind: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
                json: false,
                json_stream: false,
                print_interval: Duration::from_secs(1),
                seed: 0,
                schedule_out: None,
                ci_smoke: false,
            },
            git: GitInfo {
                rev: "c0ffee".into(),
                dirty: false,
            },
            hostname: "test-host".into(),
            topology: TopologyKind::Mem,
            elapsed: Duration::from_secs(20),
            recorded: RecordedCounts {
                client_calls: 1_000,
                timestamps: 4_000,
            },
            throughput: Throughput {
                client_calls_per_sec: 50.0,
                timestamps_per_sec: 200.0,
            },
            latency_per_call_us: LatencyStats {
                p50: 100,
                p90: 200,
                p99: 500,
                p999: 1000,
                min: 50,
                max: 2000,
                mean: 120,
            },
            transient_retries: 0,
            out_of_range_samples: 0,
            violations: Vec::new(),
            chaos_events: Vec::new(),
            schedule: Schedule {
                source: ScheduleSource::Named {
                    scenario: "steady".into(),
                },
                ops: Vec::new(),
                total: Duration::from_secs(20),
                loadgen_pause: None,
            },
            outcome: Outcome::Ok,
        }
    }

    #[test]
    fn text_contains_key_fields() {
        let s = sample_report().render_text();
        assert!(s.contains("topology=Mem"));
        assert!(s.contains("outcome=Ok"));
        assert!(s.contains("client_calls=1000") || s.contains("client_calls=1_000"));
        assert!(s.contains("p50: 100"));
        assert!(s.contains("violations: 0"));
    }

    #[test]
    fn json_parses_and_has_expected_shape() {
        let raw = sample_report().render_json();
        let parsed: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(parsed["outcome"], "Ok");
        assert_eq!(parsed["topology"], "Mem");
        assert_eq!(parsed["recorded"]["client_calls"], 1000);
        assert_eq!(parsed["violations"].as_array().unwrap().len(), 0);
        assert!(parsed["throughput"]["timestamps_per_sec"].as_f64().unwrap() > 0.0);
    }

    #[test]
    fn json_is_single_line() {
        let raw = sample_report().render_json();
        assert!(!raw.contains('\n'), "json must be one line: {raw}");
    }

    #[test]
    fn text_renders_all_outcome_variants() {
        for (outcome, want) in [
            (Outcome::Ok, "outcome=Ok"),
            (Outcome::InvariantViolation, "outcome=InvariantViolation"),
            (
                Outcome::ProgrammerError {
                    reason: "boom".into(),
                },
                "ProgrammerError: boom",
            ),
            (
                Outcome::HarnessError {
                    kind: HarnessErrorKind::HostFault {
                        detail: "disk".into(),
                    },
                },
                "HarnessError:",
            ),
            (Outcome::Interrupted, "outcome=Interrupted"),
        ] {
            let mut r = sample_report();
            r.outcome = outcome;
            let s = r.render_text();
            assert!(s.contains(want), "missing {want:?} in:\n{s}");
        }
    }

    #[test]
    fn json_renders_all_outcome_variants() {
        for (outcome, want) in [
            (Outcome::Ok, "Ok"),
            (Outcome::InvariantViolation, "InvariantViolation"),
            (
                Outcome::ProgrammerError {
                    reason: "boom".into(),
                },
                "ProgrammerError",
            ),
            (
                Outcome::HarnessError {
                    kind: HarnessErrorKind::HostFault {
                        detail: "disk".into(),
                    },
                },
                "HarnessError",
            ),
            (Outcome::Interrupted, "Interrupted"),
        ] {
            let mut r = sample_report();
            r.outcome = outcome;
            let v: serde_json::Value = serde_json::from_str(&r.render_json()).unwrap();
            assert_eq!(v["outcome"], want);
        }
    }

    #[test]
    fn renders_non_mem_topologies() {
        for topo in [TopologyKind::Raft, TopologyKind::Process] {
            let mut r = sample_report();
            r.topology = topo;
            let text = r.render_text();
            let expect = match topo {
                TopologyKind::Mem => "Mem",
                TopologyKind::Raft => "Raft",
                TopologyKind::Process => "Process",
            };
            assert!(text.contains(&format!("topology={expect}")), "{text}");
            let json: serde_json::Value = serde_json::from_str(&r.render_json()).unwrap();
            assert_eq!(json["topology"], expect);
        }
    }

    #[test]
    fn json_renders_random_schedule_source() {
        let mut r = sample_report();
        r.schedule.source = ScheduleSource::Random {
            seed: 7,
            params: crate::schedule::RandomParams::default(),
        };
        let v: serde_json::Value = serde_json::from_str(&r.render_json()).unwrap();
        assert_eq!(v["schedule_summary"]["source"], "random:7");
    }

    #[test]
    fn chaos_event_summary_covers_all_kinds_and_outcomes() {
        // Before this PR, `render_json` emitted `chaos_events: <len>` —
        // hiding everything that actually fired. The artifact in run
        // 26268395168 showed `chaos_events: 0` despite an `op_count: 612`
        // schedule, which made root-causing the failure harder. This test
        // pins the typed-list shape and exercises every variant.
        use crate::chaos::{ChaosEvent, ChaosKind, ChaosOutcome, ChaosWindow};
        use std::time::Instant;

        let now = Instant::now();
        let make = |kind, outcome| ChaosEvent {
            window: ChaosWindow {
                kind,
                started_at: now,
                ended_at: now,
                grace: Duration::from_millis(50),
            },
            outcome,
        };

        let mut r = sample_report();
        r.chaos_events = vec![
            make(ChaosKind::LeaderKill, ChaosOutcome::Applied),
            make(
                ChaosKind::LeaderPause,
                ChaosOutcome::Skipped {
                    reason: "no leader".into(),
                },
            ),
            make(
                ChaosKind::FailpointArm {
                    name: "stress::fp_x".into(),
                },
                ChaosOutcome::Applied,
            ),
            make(
                ChaosKind::FailpointDisarm {
                    name: "stress::fp_x".into(),
                },
                ChaosOutcome::Failed {
                    reason: "fail::remove failed".into(),
                },
            ),
        ];

        let v: serde_json::Value = serde_json::from_str(&r.render_json()).unwrap();
        let arr = v["chaos_events"]
            .as_array()
            .expect("chaos_events is a list");
        assert_eq!(arr.len(), 4);
        assert_eq!(arr[0]["kind"], "LeaderKill");
        assert_eq!(arr[0]["outcome"], "Applied");
        assert_eq!(arr[1]["kind"], "LeaderPause");
        assert_eq!(arr[1]["outcome"], "Skipped");
        assert_eq!(arr[1]["reason"], "no leader");
        assert_eq!(arr[2]["kind"], "FailpointArm");
        assert_eq!(arr[2]["name"], "stress::fp_x");
        assert_eq!(arr[3]["kind"], "FailpointDisarm");
        assert_eq!(arr[3]["outcome"], "Failed");

        // Text renderer keeps a count-only line; preserve it.
        let text = r.render_text();
        assert!(text.contains("chaos events: 4"), "{text}");
    }

    #[test]
    fn violation_summary_covers_all_kinds() {
        use crate::chaos::ChaosKind;
        use crate::sample::{IssuedSample, LivenessIncident, LivenessIncidentKind};
        use crate::types::ClientId;
        use crate::violation::{Violation, ViolationKind};
        use tsoracle_core::Timestamp;

        let now = std::time::Instant::now();
        let sample = IssuedSample {
            client_id: ClientId(0),
            batch_id: 0,
            batch_idx: 0,
            is_last: true,
            ts: Timestamp(42),
            issued_at: now,
            recv_time: now,
        };

        let mut r = sample_report();
        r.violations = vec![
            Violation {
                kind: ViolationKind::Monotonicity {
                    prev: Timestamp(100),
                    got: Timestamp(50),
                    sample: sample.clone(),
                },
                at: now,
            },
            Violation {
                kind: ViolationKind::BatchInternalOrdering {
                    client_id: ClientId(1),
                    batch_id: 7,
                    values: vec![Timestamp(3), Timestamp(2)],
                    detail: "out of order".into(),
                },
                at: now,
            },
            Violation {
                kind: ViolationKind::FenceFreshness {
                    pre_window_high_water: Timestamp(99),
                    first_post_window_ts: Timestamp(50),
                    window_kind: ChaosKind::LeaderKill,
                },
                at: now,
            },
            Violation {
                kind: ViolationKind::Liveness {
                    incident: LivenessIncident {
                        kind: LivenessIncidentKind::DeadlineExceeded {
                            client_id: ClientId(2),
                            attempts: 3,
                            last_error: "Unavailable".into(),
                            started_at: now,
                        },
                        at: now,
                    },
                },
                at: now,
            },
        ];

        let v: serde_json::Value = serde_json::from_str(&r.render_json()).unwrap();
        let arr = v["violations"].as_array().unwrap();
        assert_eq!(arr.len(), 4);
        let kinds: Vec<&str> = arr.iter().map(|v| v["kind"].as_str().unwrap()).collect();
        assert!(kinds.contains(&"Monotonicity"));
        assert!(kinds.contains(&"BatchInternalOrdering"));
        assert!(kinds.contains(&"FenceFreshness"));
        assert!(kinds.contains(&"Liveness"));

        // Also covers the text-renderer count line.
        let text = r.render_text();
        assert!(text.contains("violations: 4"), "{text}");
    }
}
