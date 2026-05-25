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

//! End-to-end smoke for `harness::run`. Asserts the harness completes a tiny
//! workload, produces the right RECORDED counts (post-warmup, integer-floored),
//! and emits no transient retries or out-of-range latencies.

use bench_minimal::{RunConfig, harness};
use std::time::Duration;

fn tiny_config() -> RunConfig {
    RunConfig {
        clients: 2,
        ops: 200,
        batch_size: 1,
        client_threads: 1,
        server_threads: 1,
        warmup: 10,
        bind: "127.0.0.1:0".parse().unwrap(),
        print_interval: Duration::from_millis(500),
        json: false,
        seed: 0,
    }
}

#[test]
fn end_to_end_records_expected_calls() {
    let report = harness::run(tiny_config()).expect("harness::run");
    // (200 - 10) / 1 / 2 = 95 per task * 2 tasks = 190 recorded calls.
    assert_eq!(report.recorded.client_calls, 190);
    assert_eq!(report.recorded.timestamps, 190); // batch_size=1
    assert_eq!(report.transient_retries, 0);
    assert_eq!(report.out_of_range_samples, 0);
    assert!(
        report.throughput.timestamps_per_sec > 0.0,
        "throughput must be > 0: {:?}",
        report.throughput
    );
}

#[test]
fn validate_rejects_zero_clients() {
    let mut cfg = tiny_config();
    cfg.clients = 0;
    assert!(cfg.validate().is_err());
}
