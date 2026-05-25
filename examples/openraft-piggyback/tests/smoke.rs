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

//! End-to-end guard: piggyback envelope preserves the freshness invariant
//! across failover and isolates host KV state from the TSO field.

use std::time::Duration;

use tokio::time::timeout;

// Bring in the binary crate's library-shaped run_demo. `main.rs` defines the
// helper in the bin namespace, so we re-declare it here as a path import via
// the bin target's auto-detected lib alias. If the build complains, see the
// note below for the alternative wiring.
//
// `example-openraft-piggyback` exposes `run_demo` directly through its bin
// target, accessible from the `tests/` integration harness as a path module.
use example_openraft_piggyback::run_demo;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn piggyback_demo_preserves_freshness_invariant() {
    let outcome = timeout(Duration::from_secs(30), run_demo())
        .await
        .expect("demo finished within 30s")
        .expect("demo ran without error");

    assert!(
        outcome.post_failover_high_water > outcome.pre_failover_high_water,
        "post-failover high-water ({}) must exceed pre-failover ({})",
        outcome.post_failover_high_water,
        outcome.pre_failover_high_water,
    );
    assert!(
        outcome.post_failover_first_ts > outcome.pre_failover_last_ts,
        "post-failover timestamp ({:?}) must exceed last pre-failover timestamp ({:?})",
        outcome.post_failover_first_ts,
        outcome.pre_failover_last_ts,
    );
    assert!(
        outcome.high_water_unchanged_across_getts,
        "steady-state GetTs must not advance the durable high-water",
    );
    assert!(
        !outcome.kv_after_writes.is_empty(),
        "host KV writes must land in the host SM",
    );
}
