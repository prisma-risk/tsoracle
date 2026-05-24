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
//! Regression: a process restart resets `StandaloneHost::barrier_seq` to
//! 0, but the `applied_barriers` ledger is restored from durable state
//! (decided-log replay + snapshot transfer). `current_high_water` returns
//! once `applied_barrier_seq(self) >= minted_seq`. If the freshly minted
//! seq is not lifted above the recovered ledger, a `(self, old_seq)` entry
//! from a PRIOR process lifetime satisfies the predicate immediately, so
//! the read returns the stale `high_water()` before its own barrier is
//! applied — exactly the failover hazard the per-node nonce was meant to
//! close, reopened across a restart.
//!
//! This test reproduces the interleaving with RocksDB-backed storage and
//! yield-point gating:
//!   1. Durably record seven barriers attributed to a follower plus an
//!      Advance(100); the follower folds them, so its recovered ledger is
//!      `reader -> 7` and its recovered high-water is 100.
//!   2. Stop the follower, decide Advance(500) on the surviving majority
//!      (durable, but absent from the follower's recovered log), then
//!      rebuild the follower from disk — its `barrier_seq` is back to 0.
//!   3. Pause every apply task so the recovered suffix folds (ledger=7,
//!      hw=100) but the caught-up Advance(500) stays unfolded.
//!   4. Issue `current_high_water` on the follower, parked between its
//!      `append(Barrier)` and its wait, and let the barrier decide.
//!
//! Pre-fix the follower mints seq=1, the recovered `reader -> 7` satisfies
//! `7 >= 1` the instant the reader is released, and it returns the stale
//! 100. With the fix the minting counter resumes at 7, the read mints
//! seq=8, `7 >= 8` is false, so it waits until its own barrier folds and
//! returns the linearized 500.

#![cfg(all(feature = "rocksdb-storage", feature = "yieldpoints"))]

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
async fn recovered_barrier_ledger_does_not_satisfy_a_fresh_read_after_restart() {
    let mut cluster = common::build_rocksdb_cluster(3);
    cluster.start_all();
    cluster
        .drive_until(common::some_leader_elected(), 2_000)
        .await;
    let leader_id = cluster.leader();
    let reader_id = cluster
        .nodes
        .iter()
        .map(|node| node.node_id)
        .find(|id| *id != leader_id)
        .expect("at least one follower");

    // Phase A: durably record seven barriers attributed to `reader_id`
    // plus Advance(100). Injecting via the leader keeps the follower out
    // of election churn — the `node` field is what keys the ledger, not
    // which node appended the entry.
    {
        let leader = cluster.node(leader_id).omnipaxos();
        let mut handle = leader.lock();
        for seq in 1..=7u64 {
            handle
                .append(HighWaterCommand::Barrier {
                    node: reader_id,
                    seq,
                })
                .expect("append barrier on leader");
        }
        handle
            .append(HighWaterCommand::Advance(AdvancePayload { at_least: 100 }))
            .expect("append advance on leader");
    }
    cluster
        .drive_until(|state| state.high_water_on(reader_id) >= 100, 3_000)
        .await;
    cluster
        .drive_until(common::all_decided_at_least(8), 3_000)
        .await;
    let reader_decided_before = cluster.decided_idx_on(reader_id);
    assert!(reader_decided_before >= 8);

    // Phase B: stop the follower and rebuild it from disk so its
    // barrier_seq resets to 0 while its durable ledger (reader -> 7) and
    // high-water (100) survive. Pause every apply task BEFORE the follower
    // starts so that, once started, it folds its recovered suffix
    // (ledger=7, hw=100) on the first drain and then parks.
    cluster.stop_node(reader_id).await;
    let apply_gate = yieldpoint::cfg(APPLY_TASK_YIELD);
    cluster.rebuild_rocksdb_node(reader_id);
    let reader_recovered_decided = cluster.decided_idx_on(reader_id);
    assert_eq!(
        reader_recovered_decided, reader_decided_before,
        "recovered decided_idx must reflect exactly the durable pre-stop suffix",
    );
    cluster.start_node(reader_id);
    cluster
        .drive_until(|state| state.high_water_on(reader_id) >= 100, 5_000)
        .await;
    // Beat to let the apply task fold the recovered suffix and park at the
    // yield point before any further entries decide.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Decide Advance(500) AFTER the follower's apply task is parked. The
    // follower learns it through consensus (decided_idx advances) but, with
    // apply parked, never folds it — so its high_water stays the recovered
    // 100. This is the entry a correct linearized read must reflect.
    {
        let leader = cluster.node(leader_id).omnipaxos();
        leader
            .lock()
            .append(HighWaterCommand::Advance(AdvancePayload { at_least: 500 }))
            .expect("append advance(500) on leader");
    }
    cluster
        .drive_until(
            |state| state.decided_idx_on(reader_id) > reader_recovered_decided,
            5_000,
        )
        .await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        cluster.high_water_on(reader_id),
        100,
        "apply parked: Advance(500) decided + replicated to the follower but not yet folded",
    );

    // Phase C: parked read on the recovered follower.
    let reader_park_gate = yieldpoint::cfg(CURRENT_HW_YIELD);
    let observed = {
        let host_ref = cluster
            .node(reader_id)
            .host
            .as_ref()
            .expect("reader host present");
        let mut read_future = Box::pin(host_ref.current_high_water());

        // Drive the reader to its park; the appended barrier is forwarded
        // to the leader, decided, and replicated back to the follower
        // (its decided_idx advances) while apply stays parked. The reader
        // must not return during this window.
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(300)) => {}
            _ = &mut read_future => panic!("read returned before yieldpoint was released"),
        }

        // Release the parked reader. Pre-fix the next loop iteration sees
        // the recovered `reader -> 7` ledger satisfy `7 >= 1` and returns
        // the stale `high_water()` (100). With the fix the minted seq is 8,
        // so `7 >= 8` is false and the reader keeps waiting.
        reader_park_gate.notify_one();

        // Release the apply tasks shortly after so a correct (waiting)
        // reader can make progress: its own barrier and the Advance(500)
        // fold, and the read returns 500. Disarm the yield point BEFORE
        // notifying: the apply loop re-checks the registry on every hit,
        // so notifying first would let a woken task re-park before remove()
        // lands and never fold the reader's barrier.
        let apply_release_gate = apply_gate.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(150)).await;
            yieldpoint::remove(APPLY_TASK_YIELD);
            apply_release_gate.notify_waiters();
        });

        tokio::time::timeout(Duration::from_secs(5), &mut read_future)
            .await
            .expect("current_high_water must complete")
            .expect("current_high_water must not error")
    };

    assert_eq!(
        observed, 500,
        "post-restart read must wait for its own barrier (seq resumed above the recovered \
         ledger) and reflect Advance(500); the recovered ledger must not short-circuit it to \
         the stale 100",
    );

    yieldpoint::remove(CURRENT_HW_YIELD);
    cluster.stop_all().await;
}
