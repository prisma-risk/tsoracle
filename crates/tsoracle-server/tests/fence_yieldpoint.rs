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

//! Smoke test for the `server::fence::after_load_before_persist` yield
//! point. Exercises the mechanism end-to-end against the real
//! `run_leader_watch` task: arms the gate, drives a leader transition,
//! verifies the fence is parked (state stays `NotServing`), releases the
//! gate, and confirms the fence completes through to `Serving`.
//!
//! This is the async sibling of the sync failpoint at the same call
//! site. The failpoint variant covers error injection (typed-error
//! return / panic / sleep); the yield-point variant unlocks tests that
//! need to deliver a *concurrent driver event* — a follower transition
//! or a second leader event at a different epoch — while the fence is
//! parked mid-iteration. A `fail`-crate `pause` action would have
//! blocked the worker thread and wedged the timer driver. See
//! `docs/yieldpoint-testing.md` for the full rationale.

#![cfg(feature = "yieldpoints")]

use std::sync::Arc;
use std::time::Duration;

use tsoracle_consensus::ConsensusDriver;
use tsoracle_core::Epoch;
use tsoracle_server::test_fakes::InMemoryDriver;
use tsoracle_server::{Server, ServingState};
use tsoracle_yieldpoint as yieldpoint;

const FENCE_GATE: &str = "server::fence::after_load_before_persist";

#[tokio::test]
async fn fence_parks_at_after_load_yieldpoint_until_released() {
    let driver = Arc::new(InMemoryDriver::new());

    // Seed a prior high-water so the load step has something to return.
    driver.persist_high_water(1_000, Epoch(0)).await.unwrap();

    let server = Arc::new(
        Server::builder()
            .consensus_driver(driver.clone())
            .failover_advance(Duration::from_millis(0))
            .build()
            .unwrap(),
    );
    let mut state_rx = server.subscribe();

    // Arm the yield point BEFORE spawning the watch task so the first
    // Leader iteration is guaranteed to park.
    let gate = yieldpoint::cfg(FENCE_GATE);

    let watch_server = server.clone();
    let watch_handle = tokio::spawn(async move { watch_server.run_leader_watch_for_tests().await });

    driver.become_leader(Epoch(1));

    // Give the fence ample time to reach the yield point. Without the
    // yield point the fence would reach `Serving` within a handful of
    // milliseconds; if `Serving` is observed before we release, the gate
    // didn't fire and the test should fail.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let state = state_rx.borrow().clone();
    assert!(
        matches!(state, ServingState::NotServing { .. }),
        "fence must still be parked at the yield point; observed {state:?}"
    );

    gate.notify_one();

    // After release the fence persists, seeds the allocator, and publishes
    // `Serving`. Wait for that to land.
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if matches!(*state_rx.borrow_and_update(), ServingState::Serving) {
                return;
            }
            state_rx.changed().await.unwrap();
        }
    })
    .await
    .expect("fence must reach Serving within 2s of yield-point release");

    yieldpoint::remove(FENCE_GATE);
    watch_handle.abort();
}
