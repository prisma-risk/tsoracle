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

//! Coverage for the leader-watch lifecycle plumbing on the serve path: a
//! clean `Ok` when the caller's shutdown fires, `WatchStreamClosed` on a
//! leadership-stream EOF, and `WatchPanic` when the watch task panics.
//!
//! These drive the server through `serve_with_listener` with a pre-bound
//! listener (as `boot_server` in `test_support` does). Binding the listener
//! up front and handing it over avoids the bind/drop/rebind race that an
//! `addr`-binding `serve*` call would reintroduce (#248).

use core::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::Stream;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tsoracle_consensus::{ConsensusDriver, ConsensusError, LeaderState};
use tsoracle_core::{Epoch, SystemClock};
use tsoracle_server::{
    Server, ServerError,
    test_fakes::{InMemoryDriver, StallableDriver},
};

/// Spin until the leader-watch fence has entered `persist_high_water` (and, with
/// the driver stalled from call index 0, is now parked there). Polling the
/// driver's call counter makes the "watch task is wedged mid-fence" precondition
/// deterministic rather than timing-dependent.
async fn wait_until_persist_started(driver: &StallableDriver) {
    for _ in 0..2_000 {
        if driver.persist_call_count() >= 1 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("fence never reached persist_high_water within the polling window");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serve_with_listener_returns_ok_when_user_shutdown_fires() {
    // Bind a listener, hand it to `serve_with_listener`, then trigger the
    // user-shutdown future. Asserts the function returns Ok and the spawned
    // task drops the watch handle cleanly.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let driver = Arc::new(InMemoryDriver::new());
    driver.become_leader(Epoch(1));

    let server = Server::builder()
        .consensus_driver(driver.clone())
        .clock(Arc::new(SystemClock))
        .build()
        .unwrap();

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let serve_task = tokio::spawn(async move {
        server
            .serve_with_listener(listener, async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    // Give the server a moment to start accepting, then trigger user shutdown.
    tokio::time::sleep(Duration::from_millis(100)).await;
    shutdown_tx.send(()).unwrap();

    let outcome = tokio::time::timeout(Duration::from_secs(5), serve_task)
        .await
        .expect("serve_with_listener must return after user shutdown")
        .expect("spawned task panicked");
    assert!(outcome.is_ok(), "expected Ok, got {outcome:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serve_with_listener_resolves_when_watch_task_terminates() {
    // With a never-resolving user-shutdown future, the only exit path is the
    // watch task — the same configuration `Server::serve` sets up internally.
    // Use a driver whose leadership stream closes immediately (no leader ever
    // published) to drive that exit deterministically.
    //
    // Per #72, an EOF on the leadership stream is anomalous: the watch task
    // poisons serving state and returns `ServerError::WatchStreamClosed`,
    // which the serve loop forwards verbatim.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let driver: Arc<dyn ConsensusDriver> = Arc::new(ClosedStreamDriver);

    let server = Server::builder()
        .consensus_driver(driver)
        .clock(Arc::new(SystemClock))
        .build()
        .unwrap();

    let serve_task = tokio::spawn(async move {
        server
            .serve_with_listener(listener, futures::future::pending())
            .await
    });
    let outcome = tokio::time::timeout(Duration::from_secs(5), serve_task)
        .await
        .expect("serve must return after watch stream closes")
        .expect("spawned task panicked");
    match outcome {
        Err(ServerError::WatchStreamClosed) => {}
        other => panic!("expected WatchStreamClosed, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serve_binds_addr_and_resolves_when_watch_task_terminates() {
    // `Server::serve(addr)` is the addr-binding convenience over
    // `serve_with_shutdown(addr, pending())`: it owns the bind and wires a
    // never-resolving shutdown future, so the watch task is its only exit path.
    // A leadership stream that closes immediately drives that exit
    // deterministically, proving `serve` forwards the watch outcome verbatim
    // like the listener variant above.
    //
    // Reserve a loopback port and release it so `serve` can bind it itself; the
    // watch stream closes regardless of the bind, so the race window is benign.
    let addr = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap();

    let driver: Arc<dyn ConsensusDriver> = Arc::new(ClosedStreamDriver);

    let server = Server::builder()
        .consensus_driver(driver)
        .clock(Arc::new(SystemClock))
        .build()
        .unwrap();

    let serve_task = tokio::spawn(async move { server.serve(addr).await });
    let outcome = tokio::time::timeout(Duration::from_secs(5), serve_task)
        .await
        .expect("serve must return after the watch stream closes")
        .expect("spawned task panicked");
    match outcome {
        Err(ServerError::WatchStreamClosed) => {}
        other => panic!("expected WatchStreamClosed, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serve_with_listener_translates_watch_panic_to_server_error() {
    // A driver whose `leadership_events()` panics on first poll triggers
    // `catch_unwind` in `into_router`, which republishes NotServing and
    // re-raises. The outer serve loop then surfaces it as
    // `ServerError::WatchPanic`. This path exercises the catch_unwind
    // branch (server.rs:200-210), `panic_payload_to_string`, and the
    // join-handle error mapping in `join_to_server_result`.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let driver: Arc<dyn ConsensusDriver> = Arc::new(PanickingDriver);

    let server = Server::builder()
        .consensus_driver(driver)
        .clock(Arc::new(SystemClock))
        .build()
        .unwrap();

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let serve_task = tokio::spawn(async move {
        server
            .serve_with_listener(listener, async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    let outcome = tokio::time::timeout(Duration::from_secs(5), serve_task)
        .await
        .expect("serve_with_listener must return after watch panic")
        .expect("spawned task panicked");
    let _ = shutdown_tx; // unused; the watch arm exits first

    match outcome {
        Err(ServerError::WatchPanic { payload, .. }) => {
            assert!(payload.contains("watch boom"), "got {payload}");
        }
        other => panic!("expected WatchPanic, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serve_returns_within_grace_when_a_driver_call_hangs() {
    // Regression for the shutdown-stall hazard: the leader-watch fence observes
    // its cooperative-cancel signal only at `select!` boundaries, never inside a
    // fence attempt. A `persist_high_water` that never returns (the driver trait
    // places no latency bound) therefore parks the watch task upstream of any
    // cancel-observing await, so dropping the cancel sender cannot stop it. Left
    // unbounded, `serve_inner` would block process exit until the kubelet
    // escalates to SIGKILL. With a configured `shutdown_grace`, the serve path
    // must abort the wedged task once the grace elapses and return promptly.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let driver = Arc::new(StallableDriver::new());
    // Stall every persist from call index 0: the fence's own persist wedges.
    driver.stall_from(0);
    driver.become_leader(Epoch(1));

    let server = Server::builder()
        .consensus_driver(driver.clone())
        .clock(Arc::new(SystemClock))
        .shutdown_grace(Duration::from_millis(200))
        .build()
        .unwrap();

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let serve_task = tokio::spawn(async move {
        server
            .serve_with_listener(listener, async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    // The fence must be parked in the stalled persist before we ask to stop, so
    // the cancel genuinely arrives while a driver call is in flight.
    wait_until_persist_started(&driver).await;
    shutdown_tx.send(()).unwrap();

    let outcome = tokio::time::timeout(Duration::from_secs(3), serve_task)
        .await
        .expect("serve must return after the grace elapses even though the driver is wedged")
        .expect("spawned task panicked");
    assert!(
        outcome.is_ok(),
        "a shutdown that forcibly aborts a wedged watch task reports Ok, got {outcome:?}"
    );

    // Release the held persist so the (now-aborted) future tears down cleanly.
    driver.release();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watch_guard_shutdown_returns_within_grace_when_a_driver_call_hangs() {
    // The embedder-facing analogue: `WatchGuard::shutdown` awaits the same watch
    // task. A wedged `persist_high_water` must not block an embedder's shutdown
    // either — the grace bounds the cooperative wait, then the task is aborted.
    let driver = Arc::new(StallableDriver::new());
    driver.stall_from(0);
    driver.become_leader(Epoch(1));

    let server = Server::builder()
        .consensus_driver(driver.clone())
        .clock(Arc::new(SystemClock))
        .shutdown_grace(Duration::from_millis(200))
        .build()
        .unwrap();

    let (_routes, guard) = server.into_router().expect("into_router must succeed");

    wait_until_persist_started(&driver).await;

    let outcome = tokio::time::timeout(Duration::from_secs(3), guard.shutdown())
        .await
        .expect("WatchGuard::shutdown must return after the grace elapses even when wedged");
    assert!(
        outcome.is_ok(),
        "a forced abort on shutdown reports Ok (the stop was requested), got {outcome:?}"
    );

    driver.release();
}

/// Driver whose `leadership_events()` stream resolves to `None` immediately,
/// modelling a driver that shut down before publishing any state.
struct ClosedStreamDriver;

#[async_trait::async_trait]
impl ConsensusDriver for ClosedStreamDriver {
    fn leadership_events(&self) -> Pin<Box<dyn Stream<Item = LeaderState> + Send>> {
        Box::pin(futures::stream::empty())
    }
    async fn load_high_water(&self) -> Result<u64, ConsensusError> {
        Ok(0)
    }
    async fn persist_high_water(
        &self,
        _at_least: u64,
        _epoch: Epoch,
    ) -> Result<u64, ConsensusError> {
        Ok(0)
    }
}

/// Driver whose `leadership_events()` panics on construction — exercises the
/// `catch_unwind` arm in the leader-watch task. The panic message is the
/// `&'static str` form, hitting the matching branch in
/// `panic_payload_to_string`.
struct PanickingDriver;

#[async_trait::async_trait]
impl ConsensusDriver for PanickingDriver {
    fn leadership_events(&self) -> Pin<Box<dyn Stream<Item = LeaderState> + Send>> {
        panic!("watch boom");
    }
    async fn load_high_water(&self) -> Result<u64, ConsensusError> {
        Ok(0)
    }
    async fn persist_high_water(
        &self,
        _at_least: u64,
        _epoch: Epoch,
    ) -> Result<u64, ConsensusError> {
        Ok(0)
    }
}
