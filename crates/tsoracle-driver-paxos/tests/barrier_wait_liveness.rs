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

//! `StandaloneHost` barrier waits must not park forever (#354).
//!
//! `current_high_water` / `submit_advance` append a Barrier and wait for the
//! apply path to fold it. Two failure modes used to hang indefinitely: the
//! barrier never decides (quorum loss / lost leadership), and the apply task
//! dies (panic). Both must now end in a classified error instead of parking.
//!
//! Both tests run under tokio virtual time (`start_paused`): the deadline test
//! relies on the runtime auto-advancing the virtual clock to the barrier
//! timeout once no task can make progress, so it is instant and deterministic.

use std::time::Duration;

use tsoracle_consensus::ConsensusError;
use tsoracle_driver_paxos::host::PaxosHighWaterHost;

#[path = "common/mod.rs"]
mod common;

use common::{build_mem_cluster, build_mem_cluster_with_barrier_timeout, some_leader_elected};

/// A barrier that never decides or applies must end at the deadline as a
/// retryable `TransientDriver`, not park forever.
#[tokio::test(start_paused = true)]
async fn submit_advance_times_out_when_barrier_never_applies() {
    // Elect a leader by deterministic stepping so the Advance append succeeds,
    // then stop stepping. With no further steps the barrier is never decided
    // or applied, and a stepped cluster has no async apply task to fold it, so
    // the only way the wait can end is the deadline.
    let mut cluster = build_mem_cluster_with_barrier_timeout(3, Duration::from_millis(50));
    cluster.step_until(some_leader_elected(), 10_000);
    let leader = cluster.leader();
    let host = cluster
        .node(leader)
        .host
        .as_ref()
        .expect("leader host present");

    let err = host
        .submit_advance(42)
        .await
        .expect_err("a barrier that never applies must end at the deadline");
    assert!(
        matches!(err, ConsensusError::TransientDriver(_)),
        "the deadline must classify as TransientDriver, got {err:?}",
    );
    assert!(
        err.to_string().contains("timed out"),
        "the error must be the barrier-wait timeout, not an append failure: {err}",
    );
}

/// Once the apply task is gone, a barrier read can never be folded, so it must
/// fail fast as `PermanentDriver` rather than wait out the whole deadline.
#[tokio::test(start_paused = true)]
async fn current_high_water_fails_fast_when_apply_task_dies() {
    let mut cluster = build_mem_cluster(3);
    cluster.start_all();
    cluster.drive_until(some_leader_elected(), 10_000).await;
    let leader = cluster.leader();

    // Take the leader's host out and stop it: ApplyTask shutdown drops the
    // apply task, whose death-guard marks the apply path dead and wakes any
    // parked barrier readers.
    let mut host = cluster
        .node_mut(leader)
        .host
        .take()
        .expect("leader host present");
    host.stop().await;

    let err = host
        .current_high_water()
        .await
        .expect_err("a barrier read after the apply task is gone must fail");
    assert!(
        matches!(err, ConsensusError::PermanentDriver(_)),
        "apply-task death must classify as PermanentDriver, got {err:?}",
    );
    assert!(
        err.to_string().contains("apply task"),
        "the error must name the gone apply task: {err}",
    );

    cluster.stop_all().await;
}
