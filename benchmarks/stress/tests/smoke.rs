//! End-to-end smoke tests for the mem topology. ≤ 30s budget each.

use std::net::SocketAddr;
use std::time::Duration;

use stress::config::{ScenarioKind, StressConfig, TopologyKind};
use stress::report::Outcome;

fn base_cfg(scenario: &str, duration_s: u64) -> StressConfig {
    StressConfig {
        topology: TopologyKind::Mem,
        scenario: ScenarioKind::Named(scenario.into()),
        duration: Some(Duration::from_secs(duration_s)),
        ops: None,
        clients: 8,
        batch_size: 1,
        warmup: 100,
        client_threads: 1,
        server_threads: 2,
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
    }
}

#[test]
fn steady_smoke_completes_clean() {
    let report = stress::run(base_cfg("steady", 5)).unwrap();
    assert!(
        matches!(report.outcome, Outcome::Ok),
        "got: {:?}",
        report.outcome
    );
    assert!(report.violations.is_empty(), "got: {:?}", report.violations);
    assert!(report.recorded.timestamps > 0, "no timestamps issued");
}

#[test]
fn killer_loop_smoke_maintains_invariants() {
    let report = stress::run(base_cfg("killer-loop", 10)).unwrap();
    assert!(
        matches!(report.outcome, Outcome::Ok),
        "outcome: {:?}, violations: {:?}",
        report.outcome,
        report.violations,
    );
    assert!(report.recorded.timestamps > 0);
}

#[test]
fn inject_violation_self_test_exits_with_violation() {
    let report = stress::run_inject_violation(base_cfg("steady", 3)).unwrap();
    assert!(matches!(report.outcome, Outcome::InvariantViolation));
    assert!(!report.violations.is_empty());
}
