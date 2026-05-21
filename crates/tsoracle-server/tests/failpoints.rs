#![cfg(all(feature = "failpoints", feature = "test-fakes"))]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tonic::transport::Endpoint;
use tsoracle_core::Epoch;
use tsoracle_server::test_fakes::InMemoryDriver;
use tsoracle_server::{Server, ServerError, ServingState};

static FAILPOINT_TEST_SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Block until `state_rx` reports the expected `ServingState`.
async fn wait_until<F>(rx: &mut watch::Receiver<ServingState>, predicate: F)
where
    F: Fn(&ServingState) -> bool,
{
    loop {
        if predicate(&rx.borrow_and_update()) {
            return;
        }
        rx.changed()
            .await
            .expect("state stream closed before reaching expected state");
    }
}

/// Bridge the residual race between "state_rx published Serving" and tonic's
/// accept future having been polled. Probes by opening a real gRPC channel
/// until one succeeds.
async fn wait_for_grpc_handshake(addr: SocketAddr, budget: Duration) {
    let deadline = Instant::now() + budget;
    let endpoint: Endpoint = format!("http://{addr}").parse().unwrap();
    loop {
        if endpoint.connect().await.is_ok() {
            return;
        }
        if Instant::now() >= deadline {
            panic!("tonic never accepted gRPC handshake within {budget:?}");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

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

    // The panic surfaces as a JoinError (task panicked); the wrapper in
    // into_router catches the panic via `ServerError::WatchPanic { payload }`.
    // tokio::spawn's JoinHandle resolves Err when the task panics. But the
    // wrapper we have catches the inner result, returning it. Inspect what
    // we actually get:
    let result = tokio::time::timeout(Duration::from_secs(2), watch_handle)
        .await
        .expect("watch task did not terminate within 2s");
    // A panic inside the task: JoinHandle resolves to Err(JoinError).
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

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let serve = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_routes(routes)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
    });

    driver.become_leader(Epoch(1));
    wait_until(&mut state_rx, |s| matches!(s, ServingState::Serving)).await;
    wait_for_grpc_handshake(addr, Duration::from_secs(5)).await;

    let endpoint = format!("http://{addr}");
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
    serve.abort();
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

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let serve = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_routes(routes)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
    });

    driver.become_leader(Epoch(1));
    wait_until(&mut state_rx, |s| matches!(s, ServingState::Serving)).await;
    wait_for_grpc_handshake(addr, Duration::from_secs(5)).await;

    let endpoint = format!("http://{addr}");
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
    serve.abort();
}
