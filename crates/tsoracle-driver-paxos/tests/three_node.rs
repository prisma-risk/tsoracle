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

//! Three-node quorum advance + fence-check rejection coverage.
//!
//! Boots a 3-node cluster and runs two scenarios:
//! 1. Drive multiple `Advance` proposals through the leader's
//!    [`PaxosDriver`]; assert `current_value` converges across all three
//!    replicas.
//! 2. Wrap a non-leader's host in a [`PaxosDriver`] and call
//!    `persist_high_water` with the current (leader-derived) epoch — the
//!    fence passes and the proposal commits. Call it again with a
//!    fabricated stale epoch — the fence rejects with
//!    [`ConsensusError::Fenced`].

use std::future::Future;
use std::task::{Context, Poll};

use tsoracle_consensus::{ConsensusDriver, ConsensusError};
use tsoracle_core::Epoch;
use tsoracle_driver_paxos::{HighWaterCommand, PaxosDriver, encode_epoch};

#[path = "common/mod.rs"]
mod common;

// Driven by the deterministic step-driver (`step_until`).
#[tokio::test]
async fn three_node_quorum_advances_converge_across_replicas() {
    let mut cluster = common::build_mem_cluster(3);

    cluster.step_until(common::some_leader_elected(), 1_000);
    let leader_id = cluster.leader();

    cluster
        .node(leader_id)
        .omnipaxos()
        .lock()
        .append(HighWaterCommand::Advance { at_least: 10 })
        .expect("first append succeeds on leader");
    cluster
        .node(leader_id)
        .omnipaxos()
        .lock()
        .append(HighWaterCommand::Advance { at_least: 50 })
        .expect("second append succeeds on leader");

    cluster.step_until(common::all_decided_at_least(2), 1_000);
    cluster.step_until(
        |state| {
            state
                .nodes
                .iter()
                .all(|node| state.high_water_on(node.node_id) == 50)
        },
        1_000,
    );

    for node in &cluster.nodes {
        assert_eq!(
            cluster.high_water_on(node.node_id),
            50,
            "node {} converged to 50 (max of 10 and 50)",
            node.node_id
        );
    }

    cluster.stop_all().await;
}

// Driven by the deterministic step-driver. Path A (stale epoch) is rejected by
// the fence check before any cluster progress, so it completes immediately;
// Path B's blocking `persist_high_water` is polled by hand while `step()` drives
// the cluster (the proxy shares the follower's OmniPaxos by Arc, so there is no
// borrow conflict with stepping).
#[tokio::test]
async fn fence_check_rejects_stale_epoch_and_accepts_current() {
    let mut cluster = common::build_mem_cluster(3);

    cluster.step_until(common::some_leader_elected(), 1_000);
    let leader_id = cluster.leader();

    // Pick a follower deterministically.
    let follower_id = cluster
        .nodes
        .iter()
        .map(|node| node.node_id)
        .find(|id| *id != leader_id)
        .expect("at least one follower");

    // The fence epoch the driver compares against is derived from the
    // host's current promise ballot — every node that has acknowledged
    // the leader's prepare sees the same Ballot. `some_leader_elected`
    // only proves the elected node observes itself as leader; on slow
    // runners the follower may not have processed the prepare yet, so
    // its promise is still `Ballot::default()` (encodes to `Epoch(0)`).
    // Wait for the follower's encoded promise to match the leader's
    // before sampling so the assertion against the driver's fence-check
    // observation is race-free.
    cluster.step_until(
        |c| {
            let leader_epoch = encode_epoch(c.node(leader_id).omnipaxos().lock().get_promise());
            let follower_epoch = encode_epoch(c.node(follower_id).omnipaxos().lock().get_promise());
            leader_epoch != Epoch(0) && leader_epoch == follower_epoch
        },
        1_000,
    );
    let current_epoch = {
        let handle = cluster.node(follower_id).omnipaxos();
        let promise = handle.lock().get_promise();
        encode_epoch(promise)
    };

    // We cannot move the cluster's StandaloneHost into a PaxosDriver
    // (the cluster needs it for graceful teardown), and PaxosDriver<H>
    // takes the host by value. Wrap the follower's shared OmniPaxos
    // handle in a thin `PaxosHighWaterHost` proxy that delegates
    // omnipaxos() to the same Arc — fence checks read `get_promise()` off
    // that handle, which is the contract under test.
    let follower_handle = cluster.node(follower_id).omnipaxos();
    let follower_apply = FollowerProxyHost::new(follower_handle.clone());

    // The PaxosDriver doesn't actually use leader_stream for
    // persist_high_water; it only consumes it via leadership_events. Use
    // an empty channel.
    let (_sender, stream) = tsoracle_paxos_toolkit::lifecycle::leader_event_channel();
    let driver = PaxosDriver::new(follower_apply, stream);

    // Path A: stale epoch is rejected.
    let stale_epoch = Epoch(0xDEAD_BEEF);
    let result = driver.persist_high_water(75, stale_epoch).await;
    match result {
        Err(ConsensusError::Fenced { expected, current }) => {
            assert_eq!(expected, stale_epoch);
            assert_eq!(current, current_epoch);
        }
        other => panic!("expected Fenced, got {other:?}"),
    }

    // Path B: current epoch passes the fence and the proposal commits. The proxy
    // appends to the shared OmniPaxos and waits for the cluster to decide; poll
    // it by hand (noop waker) while stepping the cluster forward.
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut persist = Box::pin(driver.persist_high_water(75, current_epoch));
    let returned = loop {
        match persist.as_mut().poll(&mut cx) {
            Poll::Ready(result) => break result.expect("current epoch passes the fence"),
            Poll::Pending => cluster.step(),
        }
    };
    drop(persist);
    assert_eq!(
        returned, 75,
        "the apply state reflects the proposed high-water"
    );

    // Wait for every node's in-memory high-water to converge — submit_advance
    // returned the proxy's view, but the cluster's other nodes may still be
    // catching up.
    cluster.step_until(
        |state| {
            state
                .nodes
                .iter()
                .all(|node| state.high_water_on(node.node_id) >= 75)
        },
        1_000,
    );

    cluster.stop_all().await;
}

// ----------------------------------------------------------------------
// FollowerProxyHost: minimal PaxosHighWaterHost impl that shares a
// cluster node's OmniPaxos handle. Used by the fence-check test to wrap a
// non-leader in a PaxosDriver without moving the StandaloneHost out of
// the harness.
// ----------------------------------------------------------------------

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use omnipaxos::OmniPaxos;
use parking_lot::Mutex;
use tsoracle_driver_paxos::host::PaxosHighWaterHost;
use tsoracle_paxos_toolkit::test_fakes::mem_storage::MemStorage;

/// Minimal [`PaxosHighWaterHost`] that shares a cluster node's OmniPaxos
/// handle without taking ownership of its [`StandaloneHost`]. The fence
/// path reads only `omnipaxos()`; `submit_advance` appends directly to
/// the shared handle and polls `decided_idx` until the cluster's runners
/// + pump tasks replicate the entry.
struct FollowerProxyHost {
    omnipaxos: Arc<Mutex<OmniPaxos<HighWaterCommand, MemStorage<HighWaterCommand>>>>,
    last_known_high_water: AtomicU64,
}

impl FollowerProxyHost {
    fn new(
        omnipaxos: Arc<Mutex<OmniPaxos<HighWaterCommand, MemStorage<HighWaterCommand>>>>,
    ) -> Self {
        Self {
            omnipaxos,
            last_known_high_water: AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl PaxosHighWaterHost for FollowerProxyHost {
    type Entry = HighWaterCommand;
    type Storage = MemStorage<HighWaterCommand>;

    fn omnipaxos(&self) -> Arc<Mutex<OmniPaxos<HighWaterCommand, MemStorage<HighWaterCommand>>>> {
        self.omnipaxos.clone()
    }

    async fn current_high_water(&self) -> Result<u64, ConsensusError> {
        Ok(self.last_known_high_water.load(Ordering::SeqCst))
    }

    async fn submit_advance(&self, at_least: u64) -> Result<u64, ConsensusError> {
        // Capture the pre-append decided_idx so we can detect "this
        // proposal (or a higher one) was decided by the cluster."
        let snapshot_decided = self.omnipaxos.lock().get_decided_idx();
        self.omnipaxos
            .lock()
            .append(HighWaterCommand::Advance { at_least })
            .map_err(|err| {
                ConsensusError::TransientDriver(Box::new(ProxyAppendError(format!("{err:?}"))))
            })?;

        for _ in 0..2_000 {
            // yield_now (not a real-time sleep) so a hand-polled caller can
            // interleave cluster steps between re-polls without wall-clock waits.
            tokio::task::yield_now().await;
            let new_decided = self.omnipaxos.lock().get_decided_idx();
            if new_decided > snapshot_decided {
                self.last_known_high_water.store(at_least, Ordering::SeqCst);
                return Ok(at_least);
            }
        }
        Err(ConsensusError::TransientDriver(Box::new(ProxyAppendError(
            "submit_advance did not decide within timeout".to_string(),
        ))))
    }
}

#[derive(Debug)]
struct ProxyAppendError(String);

impl fmt::Display for ProxyAppendError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "proxy append failed: {}", self.0)
    }
}

impl std::error::Error for ProxyAppendError {}
