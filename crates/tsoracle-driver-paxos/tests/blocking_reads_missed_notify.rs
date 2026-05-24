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

//! Regression: the blocking-read methods on `PaxosHighWaterHost` must
//! observe the apply task's `notify_waiters` even when the apply task
//! drained the just-appended entry *before* the reader registered as a
//! `Notified` waiter.
//!
//! The pre-fix loop was:
//!
//! ```text
//! 1. snapshot decided_idx
//! 2. append Barrier / Advance
//! 3. notifier.notified().await   <-- only here do we become a waiter
//! 4. recheck decided_idx
//! ```
//!
//! `apply_notifier` uses `notify_waiters` (which does NOT store a permit)
//! and `drain_decided_into` early-returns without notifying when there
//! are no new entries to drain. So if the apply task drained between (2)
//! and (3), the single notify for the reader's own entry was lost and the
//! reader hung — no further drain would happen on an idle cluster.
//!
//! The fix uses `Notified::enable()` to register as a waiter BEFORE the
//! synchronous `decided_idx` check, closing the race window. These tests
//! arm yield points between (2) and (3) so the apply task is forced to
//! drain (and emit the now-lost notify) before the reader can register;
//! with the fix in place, the recheck after `enable()` returns the value
//! immediately. Without the fix the reader hangs and the test panics on
//! the `tokio::time::timeout`.
//!
//! Both tests run under tokio virtual time (`start_paused`): the runner's
//! `interval` ticks, the apply task's `notify_waiters`, the `spawn_release`
//! delay, and the `timeout(2s)` guard all advance in simulated time the
//! moment the runtime goes idle. The async apply task — the very thing
//! whose drain-before-register the reader must not miss — still runs as a
//! real task, so the race is preserved; only the wall-clock variance is
//! removed. The paused clock guarantees the releaser's delay outlasts the
//! apply drain without a real-time gamble. `start_paused` implies the
//! current-thread runtime.

#![cfg(feature = "yieldpoints")]

use std::time::Duration;

use tsoracle_driver_paxos::host::PaxosHighWaterHost;
use tsoracle_yieldpoint as yieldpoint;

#[path = "common/mod.rs"]
mod common;

const CURRENT_HW_YIELD: &str = "standalone_host::current_high_water::after_append_before_await";
const SUBMIT_ADVANCE_YIELD: &str = "standalone_host::submit_advance::after_append_before_await";

/// Spawns a task that releases `gate` after `release_after`, giving the
/// apply task ample time to drain the reader's appended entry while the
/// reader is parked at the yield point. Returns the join handle so the
/// test can `.await` it before exiting (otherwise the spawn may outlive
/// the test if dropping the runtime didn't get to it).
fn spawn_release(
    gate: std::sync::Arc<tokio::sync::Notify>,
    release_after: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        tokio::time::sleep(release_after).await;
        gate.notify_one();
    })
}

#[tokio::test(start_paused = true)]
async fn current_high_water_returns_when_apply_drained_before_register() {
    let mut cluster = common::build_mem_cluster(3);
    cluster.start_all();
    cluster
        .drive_until(common::some_leader_elected(), 1_000)
        .await;
    let leader_id = cluster.leader();

    // Arm the yield point. The next `current_high_water` call will append
    // a Barrier and park here before becoming an `apply_notifier` waiter.
    let gate = yieldpoint::cfg(CURRENT_HW_YIELD);

    // 200 ms is comfortably more than one tick interval (2 ms) plus a
    // round-trip across the 3-node MemNetwork — the apply task will have
    // drained the Barrier and fired `apply_notifier.notify_waiters()` long
    // before this releases.
    let releaser = spawn_release(gate, Duration::from_millis(200));

    let host_ref = cluster
        .node(leader_id)
        .host
        .as_ref()
        .expect("leader host present");

    tokio::time::timeout(Duration::from_secs(2), host_ref.current_high_water())
        .await
        .expect("current_high_water must complete within 2s of yield-point release")
        .expect("current_high_water must not return an error");

    let _ = releaser.await;
    yieldpoint::remove(CURRENT_HW_YIELD);

    cluster.stop_all().await;
}

#[tokio::test(start_paused = true)]
async fn submit_advance_returns_when_apply_drained_before_register() {
    let mut cluster = common::build_mem_cluster(3);
    cluster.start_all();
    cluster
        .drive_until(common::some_leader_elected(), 1_000)
        .await;
    let leader_id = cluster.leader();

    let gate = yieldpoint::cfg(SUBMIT_ADVANCE_YIELD);
    let releaser = spawn_release(gate, Duration::from_millis(200));

    let host_ref = cluster
        .node(leader_id)
        .host
        .as_ref()
        .expect("leader host present");

    let observed = tokio::time::timeout(Duration::from_secs(2), host_ref.submit_advance(42))
        .await
        .expect("submit_advance must complete within 2s of yield-point release")
        .expect("submit_advance must not return an error");

    assert!(
        observed >= 42,
        "submit_advance must return a high-water >= the requested floor (saw {observed})",
    );

    let _ = releaser.await;
    yieldpoint::remove(SUBMIT_ADVANCE_YIELD);

    cluster.stop_all().await;
}
