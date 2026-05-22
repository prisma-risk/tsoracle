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

#![cfg(feature = "failpoints")]

use std::sync::Arc;
use std::time::{Duration, Instant};
use tsoracle_core::Epoch;
use tsoracle_server::test_fakes::InMemoryDriver;
use tsoracle_server::test_support::{
    boot_router, wait_for_grpc_handshake, wait_until, wait_until_serving,
};
use tsoracle_server::{Server, ServerError, ServingState};

static FAILPOINT_TEST_SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// `server::fence::after_load_before_persist` fires inside `run_leader_watch`,
/// between `consensus.load_high_water()` and `consensus.persist_high_water()`.
/// A `return(transient)` action produces `Err(ServerError::Consensus(...))`,
/// which causes the leader-watch task to terminate. The server calls
/// `step_down_due_to_consensus_rejection` before the join handle resolves,
/// so the test can observe the error via the JoinHandle.
#[tokio::test]
async fn fence_aborted_after_load_does_not_advance_to_serving() {
    let _serial = FAILPOINT_TEST_SERIAL.lock().await;
    let _scenario = fail::FailScenario::setup();

    let driver = Arc::new(InMemoryDriver::new());
    let server = Server::builder()
        .consensus_driver(driver.clone())
        .build()
        .unwrap();
    let (_routes, watch_handle) = server.into_router();

    fail::cfg(
        "server::fence::after_load_before_persist",
        "return(transient)",
    )
    .unwrap();

    // Trigger a leadership transition; the fence will hit the failpoint.
    driver.become_leader(Epoch(1));

    // The watch handle resolves with the ServerError.
    let result = tokio::time::timeout(Duration::from_secs(2), watch_handle)
        .await
        .expect("watch task did not terminate within 2s")
        .expect("watch task panicked");

    assert!(
        matches!(result, Err(ServerError::Consensus(_))),
        "expected ServerError::Consensus, got {result:?}"
    );

    // The driver's stored high-water must not have advanced: the failpoint
    // fires before `persist_high_water` is called.
    assert_eq!(driver.current_high_water(), 0);
}

/// `server::fence::after_persist_before_publish` fires after
/// `persist_high_water` returns, before `try_on_leadership_gained` and
/// the `state_tx.send(Serving)`. A `panic` action terminates the
/// leader-watch task. The durable high-water has already advanced
/// (verifiable via `driver.current_high_water()`), but serving state
/// stays NotServing because the publish step never ran.
#[tokio::test]
async fn fence_panic_after_persist_advances_durable_but_not_serving() {
    let _serial = FAILPOINT_TEST_SERIAL.lock().await;
    let _scenario = fail::FailScenario::setup();

    let driver = Arc::new(InMemoryDriver::new());
    let server = Server::builder()
        .consensus_driver(driver.clone())
        .build()
        .unwrap();
    let (_routes, watch_handle) = server.into_router();

    fail::cfg("server::fence::after_persist_before_publish", "panic").unwrap();

    driver.become_leader(Epoch(1));

    // The wrapper in into_router catches the panic, calls step_down to poison
    // serving state, then resumes_unwind — so the panic still propagates out
    // of the spawned task and JoinHandle resolves Err(JoinError::is_panic()).
    // Handle observers (serve_with_shutdown / serve_with_listener) translate
    // that into ServerError::WatchPanic via join_to_server_result; raw
    // observers see the JoinError directly, as we do here. The poisoning
    // guarantee for handle droppers is covered separately by
    // panic_after_serving_published_poisons_state_when_handle_dropped.
    let result = tokio::time::timeout(Duration::from_secs(2), watch_handle)
        .await
        .expect("watch task did not terminate within 2s");
    assert!(
        result.is_err(),
        "expected the panic to surface as a JoinError, got {result:?}"
    );

    // The persist happened before the panic, so the driver's stored
    // high-water has advanced past zero.
    assert!(
        driver.current_high_water() > 0,
        "persist should have advanced the driver's stored value before the panic"
    );
}

/// `server::fence::after_serving_published` fires immediately after the
/// fence publishes `ServingState::Serving` and releases the `extension_gate`
/// drain guard. A `panic` action verifies the fail-safe documented on
/// `Server::into_router`: when the leader-watch task panics from a `Serving`
/// state, the `catch_unwind` wrapper in `into_router` calls
/// `step_down_due_to_consensus_rejection` before resuming the unwind, so
/// embedders who mount `into_router` directly and drop the `JoinHandle`
/// still see serving state transition to `NotServing` and subsequent RPCs
/// fail fast with `FAILED_PRECONDITION`. Without the wrapper, state would
/// remain published as `Serving` and `GetTs` would succeed against the
/// allocator seeded just before the panic — the regression this test pins.
#[tokio::test]
async fn panic_after_serving_published_poisons_state_when_handle_dropped() {
    let _serial = FAILPOINT_TEST_SERIAL.lock().await;
    let _scenario = fail::FailScenario::setup();

    let driver = Arc::new(InMemoryDriver::new());
    let server = Server::builder()
        .consensus_driver(driver.clone())
        .build()
        .unwrap();
    let (routes, watch_handle) = server.into_router();

    // Drop (detach) the JoinHandle: the embedder shape #27 names. Drop, not
    // abort — aborting would cancel the task before it ever runs.
    drop(watch_handle);

    let booted = boot_router(routes).await;

    fail::cfg("server::fence::after_serving_published", "panic").unwrap();
    driver.become_leader(Epoch(1));

    wait_for_grpc_handshake(booted.addr, Duration::from_secs(5))
        .await
        .expect("tonic never accepted gRPC handshake");

    let mut client = tsoracle_proto::v1::tso_service_client::TsoServiceClient::connect(format!(
        "http://{}",
        booted.addr
    ))
    .await
    .unwrap();

    // After the panic, the catch_unwind branch calls step_down, which
    // publishes NotServing. Without the fix, state would stay Serving and
    // GetTs would succeed against the allocator (seeded by
    // try_on_leadership_gained before the failpoint fires). Polling rather
    // than observing watch::Receiver transitions because rapid Serving →
    // NotServing transitions can collapse on a slow receiver — see
    // tokio::sync::watch's "only latest value retained" semantics.
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let result = client
            .get_ts(tsoracle_proto::v1::GetTsRequest { count: 1 })
            .await;
        match result {
            Ok(_) => {
                assert!(
                    Instant::now() < deadline,
                    "GetTs continued to succeed after watch-task panic — \
                     serving state was never poisoned (the regression #27 pins)"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(status) if status.code() == tonic::Code::FailedPrecondition => break,
            Err(status) => panic!("unexpected gRPC status: {status:?}"),
        }
    }

    booted.abort();
}

/// `server::service::before_allocate` fires at the top of `get_ts`,
/// before the allocator lock. A `sleep(ms)` action delays the request by
/// that many milliseconds; the client observes the delay. This site is
/// used for timing-shape tests only — its closure-form return would be
/// `Result<Response<GetTsResponse>, Status>` and would bypass the
/// production `ConsensusError -> Status` classification path.
#[tokio::test]
async fn before_allocate_sleep_delays_get_ts() {
    let _serial = FAILPOINT_TEST_SERIAL.lock().await;
    let _scenario = fail::FailScenario::setup();

    let driver = Arc::new(InMemoryDriver::new());
    let server = Server::builder()
        .consensus_driver(driver.clone())
        .build()
        .unwrap();
    let mut state_rx = server.state_rx.clone();
    let (routes, _watch_handle) = server.into_router();

    let booted = boot_router(routes).await;

    driver.become_leader(Epoch(1));
    wait_until_serving(&mut state_rx).await;
    wait_for_grpc_handshake(booted.addr, Duration::from_secs(5))
        .await
        .expect("tonic never accepted gRPC handshake");

    let endpoint = format!("http://{}", booted.addr);
    let client = tsoracle_client::Client::connect(vec![endpoint])
        .await
        .unwrap();

    fail::cfg("server::service::before_allocate", "sleep(150)").unwrap();
    let start = Instant::now();
    let result = client.get_ts().await;
    let elapsed = start.elapsed();
    fail::cfg("server::service::before_allocate", "off").unwrap();

    let err = result.err();
    assert!(
        err.is_none(),
        "get_ts failed under sleep(150) failpoint: {err:?} (elapsed {elapsed:?})"
    );
    assert!(
        elapsed >= Duration::from_millis(120),
        "expected at least 120ms delay (sleep was 150ms), saw {elapsed:?}"
    );

    drop(client);
    booted.abort();
}

/// `server::service::extension_gate_held` fires at the top of the
/// extension-gate read branch in `get_ts`, after the read guard is bound.
/// A `sleep(ms)` action delays the request while holding the gate read;
/// the client observes the delay. This wiring test proves the failpoint
/// is reachable from the gate path. The deeper invariant (held-gate
/// request must not observe state from after a concurrent fence) is
/// covered by `crates/tsoracle-client/tests/freshness.rs`.
#[tokio::test]
async fn extension_gate_held_sleep_delays_get_ts() {
    let _serial = FAILPOINT_TEST_SERIAL.lock().await;
    let _scenario = fail::FailScenario::setup();

    // A 1ms failover_advance ensures the initial fence window expires almost
    // immediately, so the first get_ts hits WindowExhausted and calls
    // extend_window — which acquires the extension_gate read lock and hits
    // the failpoint.
    let driver = Arc::new(InMemoryDriver::new());
    let server = Server::builder()
        .consensus_driver(driver.clone())
        .failover_advance(Duration::from_millis(1))
        .build()
        .unwrap();
    let mut state_rx = server.state_rx.clone();
    let (routes, _watch_handle) = server.into_router();

    let booted = boot_router(routes).await;

    driver.become_leader(Epoch(1));
    wait_until(&mut state_rx, |s| matches!(s, ServingState::Serving)).await;
    wait_for_grpc_handshake(booted.addr, Duration::from_secs(5))
        .await
        .expect("tonic never accepted gRPC handshake");

    let endpoint = format!("http://{}", booted.addr);
    let client = tsoracle_client::Client::connect(vec![endpoint])
        .await
        .unwrap();

    fail::cfg("server::service::extension_gate_held", "sleep(150)").unwrap();
    let start = Instant::now();
    let result = client.get_ts().await;
    let elapsed = start.elapsed();
    fail::cfg("server::service::extension_gate_held", "off").unwrap();

    let err = result.err();
    assert!(
        err.is_none(),
        "get_ts failed under sleep(150) failpoint: {err:?} (elapsed {elapsed:?})"
    );
    assert!(
        elapsed >= Duration::from_millis(120),
        "expected at least 120ms delay (sleep was 150ms), saw {elapsed:?}"
    );

    drop(client);
    booted.abort();
}
