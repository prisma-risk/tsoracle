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

//! Regression: a `StandaloneHost::stop` that fires while no apply task is
//! waiting must not poison the apply task spawned by a later `start`.
//!
//! `stop` signals shutdown with `Notify::notify_one`, which stores a permit
//! when no task is currently parked on `notified()`. If that permit lives on
//! a single host-lifetime `Notify`, a `stop` with no running apply task
//! (stop-before-start, double-stop, or stop after the task already exited)
//! leaves a stale permit behind. The next `start` then spawns an apply task
//! that consumes the permit on its first `select!` turn and breaks out of the
//! loop immediately — so decided entries are never drained into the in-memory
//! high-water, and reads that wait on the apply notifier hang forever.

use tsoracle_driver_paxos::AdvancePayload;
use tsoracle_driver_paxos::HighWaterCommand;

#[path = "common/mod.rs"]
mod common;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stop_before_start_does_not_poison_apply_task() {
    let mut cluster = common::build_mem_cluster(3);

    // Poison every host: call stop() before it has ever started. No apply
    // task is waiting, so under the bug each host's reused shutdown Notify
    // now holds a stored permit. The runner's stop() is a no-op here (it has
    // not been started), so this only exercises the apply-shutdown path.
    for node in &mut cluster.nodes {
        node.host.as_mut().expect("host present").stop().await;
    }

    cluster.start_all();
    cluster
        .drive_until(common::some_leader_elected(), 1_000)
        .await;
    let leader_id = cluster.leader();

    // Append directly on the leader's handle; the apply task is the only
    // thing that folds the decided entry into the in-memory high-water.
    cluster
        .node(leader_id)
        .omnipaxos()
        .lock()
        .append(HighWaterCommand::Advance(AdvancePayload { at_least: 25 }))
        .expect("append succeeds on leader");

    cluster
        .drive_until(common::all_decided_at_least(1), 1_000)
        .await;

    // Consensus decides the Advance regardless of the apply task, so the
    // poison is invisible until here: a live apply task must drain the
    // decided entry into every node's high-water. Under the bug the apply
    // tasks exited on the stale permit, this never becomes true, and
    // drive_until panics.
    cluster
        .drive_until(
            |state| {
                state
                    .nodes
                    .iter()
                    .all(|node| state.high_water_on(node.node_id) >= 25)
            },
            1_000,
        )
        .await;

    for node in &cluster.nodes {
        assert_eq!(
            cluster.high_water_on(node.node_id),
            25,
            "node {} converged to high-water 25 after a pre-start stop()",
            node.node_id
        );
    }

    cluster.stop_all().await;
}
