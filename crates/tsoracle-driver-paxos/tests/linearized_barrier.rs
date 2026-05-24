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
//
//! Regression: `current_high_water` must wait for its own appended
//! Barrier to be applied, not merely for `decided_idx` to advance past
//! the pre-append snapshot.
//!
//! The pre-fix predicate was `decided_idx > snapshot_decided`. That fires
//! whenever any entry decides — including entries appended by a prior
//! leader that the local node was lagging on. If the apply task has not
//! yet folded the reader's Barrier into state, the reader can return a
//! stale `high_water()` while the Barrier remains undecided. In failover
//! this seeds the new leader's allocator below the prior leader's true
//! ceiling and lets it issue timestamps the prior leader already served.
//!
//! These tests construct that interleaving with yield-point gating: park
//! every node's apply task between iterations, decide an Advance beneath
//! the reader's eventual Barrier, park the reader between its
//! `append(Barrier)` and its wait, let the Barrier decide while apply
//! stays paused, then release. With the pre-fix predicate the reader
//! returns the stale `high_water` (0) the moment `decided_idx` advances;
//! with the fix it waits until the apply task folds its own barrier and
//! returns the correct post-fold value (500).

#![cfg(feature = "yieldpoints")]

use std::time::Duration;

use tsoracle_driver_paxos::AdvancePayload;
use tsoracle_driver_paxos::HighWaterCommand;
use tsoracle_driver_paxos::host::PaxosHighWaterHost;
use tsoracle_yieldpoint as yieldpoint;

#[path = "common/mod.rs"]
mod common;

const APPLY_TASK_YIELD: &str = "standalone_host::apply_task::between_iterations";
const CURRENT_HW_YIELD: &str = "standalone_host::current_high_water::after_append_before_await";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn current_high_water_returns_value_advanced_before_call_under_paused_apply() {
    let mut cluster = common::build_mem_cluster(3);
    cluster.start_all();
    cluster
        .drive_until(common::some_leader_elected(), 1_000)
        .await;
    let leader_id = cluster.leader();

    // Pause every node's apply task between iterations. Until released,
    // decided entries are NOT folded into apply state.
    let apply_gate = yieldpoint::cfg(APPLY_TASK_YIELD);

    // Wait for every apply task to actually hit and park at the yield
    // point. Without this beat, the upcoming append can race the in-
    // flight drain and get folded before apply ever parks.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Decide an Advance(500) the apply task cannot yet fold.
    cluster
        .node(leader_id)
        .omnipaxos()
        .lock()
        .append(HighWaterCommand::Advance(AdvancePayload { at_least: 500 }))
        .expect("append Advance on leader");
    cluster
        .drive_until(common::all_decided_at_least(1), 1_000)
        .await;

    assert_eq!(
        cluster.high_water_on(leader_id),
        0,
        "apply paused: Advance(500) decided but not yet folded",
    );

    // Park the reader between its `append(Barrier)` and its wait so the
    // Barrier can decide while the reader's loop has not yet had a
    // chance to observe it.
    let reader_park_gate = yieldpoint::cfg(CURRENT_HW_YIELD);

    let observed = {
        let host_ref = cluster
            .node(leader_id)
            .host
            .as_ref()
            .expect("leader host present");
        let mut read_future = Box::pin(host_ref.current_high_water());

        // Drive the reader to its yield-point park, then let the runner
        // tick the Barrier to a decision (decided_idx 1 → 2) in the
        // background. The reader must not return during this window.
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(300)) => {}
            _ = &mut read_future => panic!("read returned before yieldpoint was released"),
        }

        // Release the parked reader. Under the pre-fix predicate the
        // next loop iteration sees `decided_idx (2) > snapshot_decided
        // (1)` and returns the stale `high_water()` (0). Under the fix
        // it sees `applied_barrier_seq(self) < seq` and keeps waiting.
        reader_park_gate.notify_one();

        // Release the apply task shortly after. notify_waiters wakes
        // every parked node's apply task at once — notify_one would
        // release only one and there's no guarantee it's the leader's.
        // remove() drops the registry entry so the next yieldpoint hit
        // is a no-op; otherwise the apply task would re-park and miss
        // the shutdown signal during cluster.stop_all().
        let apply_release_gate = apply_gate.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            apply_release_gate.notify_waiters();
            yieldpoint::remove(APPLY_TASK_YIELD);
        });

        tokio::time::timeout(Duration::from_secs(3), &mut read_future)
            .await
            .expect("current_high_water must complete")
            .expect("current_high_water must not error")
    };

    assert_eq!(
        observed, 500,
        "current_high_water must reflect entries decided before the call (Advance(500) folded after the apply task drains)",
    );

    yieldpoint::remove(CURRENT_HW_YIELD);
    cluster.stop_all().await;
}
