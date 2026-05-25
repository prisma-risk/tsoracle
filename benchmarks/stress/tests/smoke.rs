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
        grace_paxos: Duration::from_millis(1000),
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

#[test]
fn raft_steady_smoke_completes_clean() {
    let mut cfg = base_cfg("steady", 5);
    cfg.topology = TopologyKind::Raft;
    cfg.nodes = 3;
    let report = stress::run(cfg).unwrap();
    assert!(
        matches!(report.outcome, Outcome::Ok),
        "got: {:?}",
        report.outcome
    );
    assert!(report.violations.is_empty(), "got: {:?}", report.violations);
    assert!(report.recorded.timestamps > 0, "no timestamps issued");
}

#[test]
fn raft_killer_loop_smoke_maintains_invariants() {
    let mut cfg = base_cfg("killer-loop", 10);
    cfg.topology = TopologyKind::Raft;
    cfg.nodes = 3;
    let report = stress::run(cfg).unwrap();
    assert!(
        matches!(report.outcome, Outcome::Ok),
        "outcome: {:?}, violations: {:?}",
        report.outcome,
        report.violations,
    );
    assert!(report.recorded.timestamps > 0);
}

// ---- process topology --------------------------------------------------
//
// The process tests shell out to the `tsoracle` binary. Cargo builds
// workspace binaries during `cargo test --workspace` but NOT during
// `cargo test -p stress`; in the latter case the operator must run
// `cargo build --bin tsoracle` first or the harness will fail to locate
// it. Gated on `cfg(unix)` because POSIX-signal chaos has no Windows
// analogue.

#[cfg(unix)]
#[test]
fn process_steady_smoke_completes_clean() {
    let mut cfg = base_cfg("steady", 5);
    cfg.topology = TopologyKind::Process;
    cfg.nodes = 1;
    let report = stress::run(cfg).unwrap();
    assert!(
        matches!(report.outcome, Outcome::Ok),
        "got: {:?}, violations: {:?}",
        report.outcome,
        report.violations,
    );
    assert!(report.violations.is_empty(), "got: {:?}", report.violations);
    assert!(report.recorded.timestamps > 0, "no timestamps issued");
}

#[cfg(unix)]
#[test]
fn process_killer_loop_smoke_maintains_invariants() {
    let mut cfg = base_cfg("killer-loop", 10);
    cfg.topology = TopologyKind::Process;
    cfg.nodes = 1;
    let report = stress::run(cfg).unwrap();
    assert!(
        matches!(report.outcome, Outcome::Ok),
        "outcome: {:?}, violations: {:?}",
        report.outcome,
        report.violations,
    );
    assert!(report.recorded.timestamps > 0);
}
