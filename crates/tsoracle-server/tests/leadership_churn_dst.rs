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

//! Deterministic-simulation (turmoil) variants of the timing-fragile fence /
//! leadership-churn tests from `leadership_churn.rs`.
//!
//! Those tests wait up to 10–15 seconds of *real* time for the fence to retry
//! transient persist failures with backoff, and one races the fence's retry
//! loop against a leadership-stream transition. Under turmoil the
//! `tokio::time::sleep` backoff advances *simulated* time, so the same coverage
//! runs deterministically and instantly from a fixed seed — no wall-clock waits,
//! no flakiness. The shared transport glue lives in
//! `tsoracle_server::test_support::dst`.
//!
//! The four `extend_window_maps_*` error-mapping tests stay in
//! `leadership_churn.rs`: they use `MockClock` and assert error *codes*, so
//! they are not timing-fragile and gain nothing from simulation.
//!
//! Opt in: `cargo test -p tsoracle-server --features dst --test leadership_churn_dst`.

use core::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use futures::Stream;
use tsoracle_consensus::{ConsensusDriver, ConsensusError, LeaderState};
use tsoracle_core::{Epoch, testing::MockClock};
use tsoracle_proto::v1::GetTsRequest;
use tsoracle_server::test_fakes::{FaultKind, FaultyDriver, InMemoryDriver};
use tsoracle_server::test_support::dst::{client, into_sim_parts, serve};
use tsoracle_server::test_support::{wait_until, wait_until_not_serving, wait_until_serving};
use tsoracle_server::{Server, ServingState};

const PORT: u16 = 9_999;

/// Generous virtual-time budget. Sim time is instant in wall-clock terms, so a
/// large ceiling just guarantees the backoff retries fit without a real wait.
fn sim() -> turmoil::Sim<'static> {
    turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(300))
        .build()
}

#[test]
fn fence_recovers_after_transient_burst_exceeding_old_budget() {
    // 10 transient persist faults is past the historical bound of 8. The fence
    // must keep retrying — on a node that never stops being leader — until the
    // 11th persist succeeds and the server reaches Serving. Under turmoil the
    // backoff sleeps collapse to simulated time, so this is instant + deterministic.
    let driver = Arc::new(FaultyDriver::new());
    driver.fail_next_persists(10, FaultKind::Transient);

    let driver_in_host = driver.clone();
    let mut sim = sim();
    sim.client("node", async move {
        let server = Server::builder()
            .consensus_driver(driver_in_host.clone())
            .clock(Arc::new(MockClock::new(1_000)))
            .build()
            .unwrap();
        // into_sim_parts spawns the leader-watch task on this host's executor,
        // so the fence runs under simulated time.
        let mut parts = into_sim_parts(server)?;
        driver_in_host.become_leader(Epoch(1));
        wait_until_serving(&mut parts.state_rx).await;
        Ok(())
    });

    sim.run().unwrap();

    assert_eq!(
        driver.persist_call_count(),
        11,
        "expected 10 failed persists + 1 success"
    );
}

#[test]
fn fence_observes_follower_transition_during_unbounded_transient_retry() {
    let inner = Arc::new(InMemoryDriver::new());
    let driver = Arc::new(LatchingTransientDriver::new(inner.clone()));

    let inner_in_host = inner.clone();
    let driver_in_host = driver.clone();
    let mut sim = sim();
    sim.client("node", async move {
        let server = Server::builder()
            .consensus_driver(driver_in_host.clone())
            .clock(Arc::new(MockClock::new(1_000)))
            .build()
            .unwrap();
        let mut parts = into_sim_parts(server)?;

        // Fence enters an effectively infinite transient-retry loop.
        inner_in_host.become_leader(Epoch(1));

        // Retries must climb well past the historical bound of 8, proving the
        // loop no longer gives up while still leader. Under turmoil the backoff
        // sleeps advance simulated time, so this poll is instant.
        while driver_in_host.persist_call_count() < 12 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // A real leadership change arrives while the fence is mid-retry. The
        // Follower handler publishes NotServing carrying the hint; the fence's
        // own NotServing uses leader_endpoint: None, so observing Some(hint)
        // proves the event was dispatched rather than spun past.
        inner_in_host.become_follower(Some("10.1.2.3:50551".into()));

        wait_until(&mut parts.state_rx, |s| {
            matches!(
                s,
                ServingState::NotServing {
                    leader_endpoint: Some(_),
                    ..
                }
            )
        })
        .await;

        // Once the Follower is dispatched the loop is back on stream.next(), so
        // no further persist attempts occur.
        let settled = driver_in_host.persist_call_count();
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(
            driver_in_host.persist_call_count(),
            settled,
            "no more fence persist attempts after stepping down to Follower"
        );

        // Recovery: clear the fault and re-win leadership at a new epoch.
        driver_in_host.clear_faults();
        inner_in_host.become_leader(Epoch(2));
        wait_until_serving(&mut parts.state_rx).await;
        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn sustained_leader_churn_never_exits_watch_loop() {
    // #204: under paxos's frequent leader churn, every new election opens a
    // volatile window where the first fence persist can fail as a transient
    // append error. The earlier behavior exhausted the fence's retry budget and
    // let the watch loop return Err, poisoning serving. This pins *sustained*
    // oscillation: several Leader -> Follower -> Leader cycles, each opening with
    // a transient fence burst, asserting the watch task survives every cycle.
    const BURST: usize = 3;
    const CYCLES: u128 = 4;

    let driver = Arc::new(FaultyDriver::new());
    let driver_in_host = driver.clone();
    let mut sim = sim();

    // Server host: drive the churn cycles (it owns state_rx + the watch handle),
    // then serve the final term so the client host can issue a real GetTs.
    sim.host("server", move || {
        let driver = driver_in_host.clone();
        async move {
            let server = Server::builder()
                .consensus_driver(driver.clone())
                .clock(Arc::new(MockClock::new(1_000)))
                .window_ahead(Duration::from_millis(50))
                .failover_advance(Duration::from_millis(50))
                .build()
                .unwrap();
            let parts = into_sim_parts(server)?;
            let mut state_rx = parts.state_rx;
            let watch = parts.watch;

            for epoch in 1..=CYCLES {
                driver.fail_next_persists(BURST, FaultKind::Transient);
                driver.become_leader(Epoch(epoch));
                // Reaching Serving proves the fence rode through the transient
                // burst without giving up — the watch loop is still alive.
                wait_until_serving(&mut state_rx).await;
                assert!(
                    !watch.is_finished(),
                    "watch/serve task must stay alive while Serving on epoch {epoch}",
                );

                driver.become_follower(None);
                wait_until_not_serving(&mut state_rx).await;
                assert!(
                    !watch.is_finished(),
                    "watch/serve task must stay alive after stepping down on epoch {epoch}",
                );
            }

            // Win one more term and serve it for the client.
            driver.fail_next_persists(BURST, FaultKind::Transient);
            driver.become_leader(Epoch(CYCLES + 1));
            wait_until_serving(&mut state_rx).await;
            assert!(
                !watch.is_finished(),
                "watch must survive into the final term"
            );

            serve(parts.routes, PORT).await?;
            Ok(())
        }
    });

    // Client host: poll GetTs until the server reaches its final serving term,
    // proving the server is alive end-to-end after sustained churn.
    sim.client("client", async move {
        let mut grpc = client("server", PORT)?;
        for _ in 0..200 {
            match grpc.get_ts(GetTsRequest { count: 1 }).await {
                Ok(resp) => {
                    assert_eq!(resp.into_inner().count, 1);
                    return Ok(());
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        }
        Err("GetTs never succeeded after sustained churn".into())
    });

    sim.run().unwrap();

    // The transient-fault path was actually exercised, not vacuously skipped.
    assert!(
        driver.persist_call_count() > CYCLES as u64,
        "transient fence faults must have been exercised across cycles",
    );
}

/// Returns `TransientDriver` from every `persist_high_water` while latched,
/// modelling a driver (e.g. the paxos host) that reports leadership loss as a
/// transient append failure rather than `NotLeader`. Leadership events and
/// `load_high_water` delegate to the inner driver. Copied from
/// `leadership_churn.rs` (test binaries can't share a non-`common` module).
struct LatchingTransientDriver {
    inner: Arc<InMemoryDriver>,
    fail_persists: AtomicBool,
    persist_calls: AtomicUsize,
}

impl LatchingTransientDriver {
    fn new(inner: Arc<InMemoryDriver>) -> Self {
        Self {
            inner,
            fail_persists: AtomicBool::new(true),
            persist_calls: AtomicUsize::new(0),
        }
    }

    fn clear_faults(&self) {
        self.fail_persists.store(false, Ordering::SeqCst);
    }

    fn persist_call_count(&self) -> usize {
        self.persist_calls.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl ConsensusDriver for LatchingTransientDriver {
    fn leadership_events(&self) -> Pin<Box<dyn Stream<Item = LeaderState> + Send>> {
        self.inner.leadership_events()
    }

    async fn load_high_water(&self) -> Result<u64, ConsensusError> {
        self.inner.load_high_water().await
    }

    async fn persist_high_water(&self, at_least: u64, epoch: Epoch) -> Result<u64, ConsensusError> {
        self.persist_calls.fetch_add(1, Ordering::SeqCst);
        if self.fail_persists.load(Ordering::SeqCst) {
            return Err(ConsensusError::TransientDriver(Box::new(
                std::io::Error::other("latched transient persist fault"),
            )));
        }
        self.inner.persist_high_water(at_least, epoch).await
    }
}
