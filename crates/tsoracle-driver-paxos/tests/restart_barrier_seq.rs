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
//! Regression: a process restart resets `StandaloneHost::barrier_seq` to 0, but
//! the `applied_barriers` ledger is restored from durable state (decided-log
//! replay + snapshot transfer). `current_high_water` returns once
//! `applied_barrier_seq(self) >= minted_seq`. If the freshly minted seq is not
//! lifted above the recovered ledger, a `(self, old_seq)` entry from a PRIOR
//! lifetime satisfies the predicate immediately, so the read returns the stale
//! `high_water()` before its own barrier is applied — the failover hazard the
//! per-node nonce closes, reopened across a restart.
//!
//! Driven deterministically with the step-driver rather than yield-point
//! parking and real-time sleeps. The reader's host is taken out of the cluster
//! and driven by hand with its apply held "parked" so that its decided_idx
//! advances via consensus while its high-water and barrier ledger do not fold;
//! an explicit `apply_once` then releases it. This gives exact, instant control
//! over the interleaving the original reproduced with `tokio::time::sleep`.

#![cfg(feature = "rocksdb-storage")]

use std::future::Future;
use std::task::{Context, Poll};

use tsoracle_driver_paxos::AdvancePayload;
use tsoracle_driver_paxos::HighWaterCommand;
use tsoracle_driver_paxos::host::PaxosHighWaterHost;

#[path = "common/mod.rs"]
mod common;

#[tokio::test]
async fn recovered_barrier_ledger_does_not_satisfy_a_fresh_read_after_restart() {
    let mut cluster = common::build_rocksdb_cluster(3);
    cluster.step_until(common::some_leader_elected(), 2_000);
    let leader_id = cluster.leader();
    let reader_id = cluster
        .nodes
        .iter()
        .map(|node| node.node_id)
        .find(|id| *id != leader_id)
        .expect("at least one follower");

    // Phase A: durably record seven barriers attributed to `reader_id` plus
    // Advance(100). The `node` field keys the ledger, not which node appended,
    // so injecting via the leader keeps the follower out of election churn.
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
    cluster.step_until(|state| state.high_water_on(reader_id) >= 100, 3_000);
    cluster.step_until(common::all_decided_at_least(8), 3_000);
    let reader_decided_before = cluster.decided_idx_on(reader_id);
    assert!(reader_decided_before >= 8);

    // Phase B: stop the follower and rebuild it from disk. Its `barrier_seq`
    // resets to 0 in construction, but `new()` resumes the counter to `reader
    // -> 7` by scanning the durable log for this node's highest barrier seq,
    // while the recovery fold restores the high-water to 100. Stepping the
    // cluster lets the rebuilt node fold.
    cluster.stop_node(reader_id).await;
    cluster.rebuild_rocksdb_node(reader_id);
    assert_eq!(
        cluster.decided_idx_on(reader_id),
        reader_decided_before,
        "recovered decided_idx must reflect exactly the durable pre-stop suffix",
    );
    cluster.step_until(|state| state.high_water_on(reader_id) >= 100, 5_000);

    // Take the reader's host out of the cluster so we can drive it by hand with
    // apply held parked. `step()` now skips it (host-less); we route its
    // messages manually through the shared MemNetwork.
    let reader_host = cluster
        .node_mut(reader_id)
        .host
        .take()
        .expect("reader host present");
    let reader_recovered_decided = reader_host.omnipaxos_handle().lock().get_decided_idx();

    // Decide Advance(500) on the surviving majority, then drive the reader to
    // DECIDE it (its decided_idx advances) without applying — so its high-water
    // stays the recovered 100. This is the entry a correct linearized read must
    // reflect.
    {
        let leader = cluster.node(leader_id).omnipaxos();
        leader
            .lock()
            .append(HighWaterCommand::Advance(AdvancePayload { at_least: 500 }))
            .expect("append advance(500) on leader");
    }
    for _ in 0..5_000 {
        for message in reader_host.tick_only() {
            cluster.network.deliver_now(message);
        }
        for message in cluster.drain_inbox(reader_id) {
            reader_host.deliver(message);
        }
        cluster.step();
        if reader_host.omnipaxos_handle().lock().get_decided_idx() > reader_recovered_decided {
            break;
        }
    }
    assert_eq!(
        reader_host.current_value(),
        100,
        "apply parked: Advance(500) decided + replicated to the follower but not yet folded",
    );

    // Phase C: parked read on the recovered follower. It mints seq = 8
    // (barrier_seq resumed to 7), appends its Barrier, and must WAIT — the
    // recovered `reader -> 7` ledger must NOT satisfy `7 >= 8`.
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut read_future = Box::pin(reader_host.current_high_water());

    // First poll mints the seq, appends the Barrier, and parks on the apply
    // notifier (applied_barrier_seq(reader) == 7 < 8).
    assert!(
        matches!(read_future.as_mut().poll(&mut cx), Poll::Pending),
        "read must park after appending its barrier",
    );
    let barrier_decided = reader_host.omnipaxos_handle().lock().get_decided_idx() + 1;

    // Drive the reader's barrier through consensus to decided WITHOUT applying
    // it. The read must stay pending throughout: the recovered ledger (7) does
    // not short-circuit the fresh seq (8).
    for _ in 0..5_000 {
        for message in reader_host.tick_only() {
            cluster.network.deliver_now(message);
        }
        for message in cluster.drain_inbox(reader_id) {
            reader_host.deliver(message);
        }
        cluster.step();
        assert!(
            matches!(read_future.as_mut().poll(&mut cx), Poll::Pending),
            "read must not return while its own barrier is decided-but-unfolded",
        );
        if reader_host.omnipaxos_handle().lock().get_decided_idx() >= barrier_decided {
            break;
        }
    }

    // Release: apply the reader. It folds Advance(500) + its own Barrier(seq 8),
    // lifting applied_barrier_seq(reader) to 8 and high-water to 500; the apply
    // notifier wakes the read, which now observes 8 >= 8 and returns 500.
    let observed = loop {
        match read_future.as_mut().poll(&mut cx) {
            Poll::Ready(result) => break result.expect("current_high_water must not error"),
            Poll::Pending => reader_host.apply_once(),
        }
    };
    assert_eq!(
        observed, 500,
        "post-restart read must wait for its own barrier (seq resumed above the recovered \
         ledger) and reflect Advance(500); the recovered ledger must not short-circuit it to \
         the stale 100",
    );

    // Drop the read future (it borrows `reader_host`) before returning the host
    // to the cluster for graceful teardown.
    drop(read_future);
    cluster.node_mut(reader_id).host = Some(reader_host);
    cluster.stop_all().await;
}
