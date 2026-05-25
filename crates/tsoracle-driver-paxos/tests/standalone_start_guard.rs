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

//! Regression (#355): `StandaloneHost::start` must enforce its
//! "not already running" lifecycle invariant in *every* build profile.
//!
//! The original guard was a `debug_assert!` that compiled away in
//! release, so a double-`start` there silently overwrote `self.task` —
//! orphaning the prior apply task (duplicate apply loops folding into
//! one high-water) and leaking the runner's tick task. The guard now
//! returns `Err(AlreadyRunning)` before spawning anything, so the
//! invariant holds regardless of profile and nothing is orphaned.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use omnipaxos::messages::Message;
use omnipaxos::{ClusterConfig, OmniPaxosConfig, ServerConfig};
use parking_lot::Mutex;
use tsoracle_driver_paxos::{AlreadyRunning, HighWaterCommand, StandaloneHost};
use tsoracle_paxos_toolkit::lifecycle::MessageSink;
use tsoracle_paxos_toolkit::test_fakes::mem_storage::MemStorage;

struct NoopSink;

#[async_trait]
impl MessageSink<HighWaterCommand> for NoopSink {
    async fn send(&self, _: Message<HighWaterCommand>) {}
}

fn build_host() -> StandaloneHost<MemStorage<HighWaterCommand>> {
    let cluster_config = ClusterConfig {
        configuration_id: 1,
        nodes: vec![1, 2, 3],
        flexible_quorum: None,
    };
    let server_config = ServerConfig {
        pid: 1,
        ..Default::default()
    };
    let config = OmniPaxosConfig {
        cluster_config,
        server_config,
    };
    let omnipaxos = config
        .build(MemStorage::<HighWaterCommand>::new())
        .expect("build omnipaxos handle");
    StandaloneHost::builder()
        .omnipaxos(Arc::new(Mutex::new(omnipaxos)))
        .my_node_id(1)
        .tick_interval(Duration::from_millis(2))
        .build()
        .expect("build standalone host")
}

// A second `start` on a live host must be rejected — not orphan the prior
// apply/tick tasks — and `stop` must clear the guard so the host restarts.
#[tokio::test(start_paused = true)]
async fn double_start_is_rejected_and_stop_restores_startability() {
    let mut host = build_host();

    host.start(Arc::new(NoopSink))
        .expect("first start on a fresh host succeeds");

    // Already running: the second start spawns nothing and reports the misuse.
    assert!(
        matches!(host.start(Arc::new(NoopSink)), Err(AlreadyRunning)),
        "start while already running must return Err(AlreadyRunning)",
    );

    host.stop().await;

    // stop() took the apply-task handle, so the guard clears and a fresh start
    // is accepted again.
    host.start(Arc::new(NoopSink))
        .expect("start after stop succeeds (guard cleared)");

    host.stop().await;
}
