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
use tsoracle_server::{Server, ServerError, test_fakes::InMemoryDriver};

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
