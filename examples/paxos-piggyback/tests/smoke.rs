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

//! End-to-end guard: the piggyback envelope preserves the freshness
//! invariant across failover and isolates host KV state from the TSO field.

use std::time::Duration;

use tokio::time::timeout;

use example_paxos_piggyback::run_demo;

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
        "host KV writes must land in the host state",
    );
}
