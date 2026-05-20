//! Regression tests for retry semantics during leadership churn.
//!
//! Two failure modes are exercised:
//!
//! 1. `persist_high_water` returns `NotLeader` / `Fenced` mid-extension —
//!    the service must surface `FAILED_PRECONDITION` with a `LeaderHint`,
//!    poison server state (allocator cleared, `ServingState::NotServing`
//!    published), and remain in that state for subsequent calls. Transient
//!    and permanent driver errors must map to `UNAVAILABLE` and `INTERNAL`
//!    respectively, without poisoning state.
//!
//! 2. `run_leader_watch` returns `Err` — `serve_with_shutdown` must exit
//!    with the watch's error (not silently keep tonic running), and the
//!    poisoned `NotServing` state must be visible to in-flight RPCs.

use core::pin::Pin;
use futures::Stream;
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tsoracle_consensus::{ConsensusDriver, ConsensusError, LeaderState};
use tsoracle_core::{Epoch, testing::MockClock};
use tsoracle_proto::v1::{GetTsRequest, tso_service_client::TsoServiceClient};
use tsoracle_server::{Server, ServerError, ServingState, test_fakes::InMemoryDriver};

/// Wraps `InMemoryDriver` and lets a test inject one specific error from the
/// next `persist_high_water` call after the leader-transition fence has been
/// persisted. The fence persist itself succeeds (so the server reaches
/// `Serving`); only the *next* persist — the one triggered by an extension —
/// returns the injected error.
struct FaultyPersistDriver {
    inner: Arc<InMemoryDriver>,
    persists_observed: AtomicUsize,
    inject_after_first: Mutex<Option<ConsensusError>>,
}

impl FaultyPersistDriver {
    fn new(inner: Arc<InMemoryDriver>, inject: ConsensusError) -> Self {
        Self {
            inner,
            persists_observed: AtomicUsize::new(0),
            inject_after_first: Mutex::new(Some(inject)),
        }
    }
}

#[async_trait::async_trait]
impl ConsensusDriver for FaultyPersistDriver {
    fn leadership_events(&self) -> Pin<Box<dyn Stream<Item = LeaderState> + Send>> {
        self.inner.leadership_events()
    }

    async fn load_high_water(&self) -> Result<u64, ConsensusError> {
        self.inner.load_high_water().await
    }

    async fn persist_high_water(&self, at_least: u64, epoch: Epoch) -> Result<u64, ConsensusError> {
        let n = self.persists_observed.fetch_add(1, Ordering::SeqCst);
        if n == 0 {
            // Let the leader-transition fence persist succeed so the server
            // reaches Serving and the allocator gets a valid epoch.
            return self.inner.persist_high_water(at_least, epoch).await;
        }
        if let Some(err) = self.inject_after_first.lock().take() {
            return Err(err);
        }
        self.inner.persist_high_water(at_least, epoch).await
    }
}

/// Wraps `InMemoryDriver` and makes `load_high_water` fail the first time it
/// is called — exercising the path where `run_leader_watch` returns `Err`
/// during a leader transition.
struct FaultyLoadDriver {
    inner: Arc<InMemoryDriver>,
    fail_load: AtomicBool,
}

impl FaultyLoadDriver {
    fn new(inner: Arc<InMemoryDriver>) -> Self {
        Self {
            inner,
            fail_load: AtomicBool::new(true),
        }
    }
}

#[async_trait::async_trait]
impl ConsensusDriver for FaultyLoadDriver {
    fn leadership_events(&self) -> Pin<Box<dyn Stream<Item = LeaderState> + Send>> {
        self.inner.leadership_events()
    }

    async fn load_high_water(&self) -> Result<u64, ConsensusError> {
        if self.fail_load.swap(false, Ordering::SeqCst) {
            return Err(ConsensusError::PermanentDriver(Box::new(
                std::io::Error::other("synthetic load failure"),
            )));
        }
        self.inner.load_high_water().await
    }

    async fn persist_high_water(&self, at_least: u64, epoch: Epoch) -> Result<u64, ConsensusError> {
        self.inner.persist_high_water(at_least, epoch).await
    }
}

async fn bind_unused() -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

async fn wait_until_serving(rx: &mut tokio::sync::watch::Receiver<ServingState>) {
    loop {
        if matches!(*rx.borrow_and_update(), ServingState::Serving) {
            return;
        }
        rx.changed().await.unwrap();
    }
}

async fn wait_until_not_serving(rx: &mut tokio::sync::watch::Receiver<ServingState>) {
    loop {
        if matches!(*rx.borrow_and_update(), ServingState::NotServing { .. }) {
            return;
        }
        rx.changed().await.unwrap();
    }
}

/// Boot a server with the given driver, drive it to `Serving`, and force the
/// clock past the seeded high-water so the next `GetTs` triggers an extension.
async fn boot_serving<D: ConsensusDriver + 'static>(
    driver: Arc<D>,
    in_memory_for_leader: &InMemoryDriver,
) -> (
    std::net::SocketAddr,
    Arc<MockClock>,
    tokio::sync::watch::Receiver<ServingState>,
    tokio::task::JoinHandle<Result<(), ServerError>>,
    tokio::sync::oneshot::Sender<()>,
) {
    let addr = bind_unused().await;
    let clock = Arc::new(MockClock::new(1_000));
    let server = Server::builder()
        .consensus_driver(driver)
        .clock(clock.clone())
        .window_ahead(Duration::from_millis(50))
        .failover_advance(Duration::from_millis(50))
        .build()
        .unwrap();
    let state_rx = server.state_rx.clone();
    let mut state_rx_for_wait = state_rx.clone();

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let serve_handle = tokio::spawn(async move {
        server
            .serve_with_shutdown(addr, async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    // Let the watch task subscribe to leadership_events before we publish.
    tokio::time::sleep(Duration::from_millis(50)).await;
    in_memory_for_leader.become_leader(Epoch(1));
    wait_until_serving(&mut state_rx_for_wait).await;

    // Push clock past the failover-fence ceiling so the next request hits
    // WindowExhausted and triggers extend_window.
    clock.set(1_000_000);

    (addr, clock, state_rx, serve_handle, shutdown_tx)
}

#[tokio::test]
async fn extend_window_maps_not_leader_to_failed_precondition_with_hint() {
    let in_memory = Arc::new(InMemoryDriver::new());
    let faulty = Arc::new(FaultyPersistDriver::new(
        in_memory.clone(),
        ConsensusError::NotLeader {
            observed: Some(Epoch(7)),
        },
    ));

    let (addr, _clock, mut state_rx, serve_handle, shutdown_tx) =
        boot_serving(faulty.clone(), &in_memory).await;

    let mut client = TsoServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    let status = client
        .get_ts(GetTsRequest { count: 1 })
        .await
        .expect_err("should fail with not-leader during extension");

    assert_eq!(
        status.code(),
        tonic::Code::FailedPrecondition,
        "got status: {status:?}"
    );
    let hint = tsoracle_server::__priv_decode_leader_hint(&status)
        .expect("leader hint metadata must be present");
    // We did not set an endpoint, so the hint endpoint is None — but the
    // metadata itself MUST exist so clients know this is a redirectable error.
    assert!(hint.leader_endpoint.is_none());

    // Allocator must have been cleared (no current epoch).
    wait_until_not_serving(&mut state_rx).await;

    // A second request should now hit the fast NOT_LEADER gate (no second
    // consensus call), so persist count stays at the same value as after the
    // injected failure (fence + injected = 2).
    let before = faulty.persists_observed.load(Ordering::SeqCst);
    let _ = client.get_ts(GetTsRequest { count: 1 }).await;
    let after = faulty.persists_observed.load(Ordering::SeqCst);
    assert_eq!(
        before, after,
        "post-step_down requests must not contact consensus"
    );

    let _ = shutdown_tx.send(());
    let _ = serve_handle.await;
}

#[tokio::test]
async fn extend_window_maps_fenced_to_failed_precondition_with_hint() {
    let in_memory = Arc::new(InMemoryDriver::new());
    let faulty = Arc::new(FaultyPersistDriver::new(
        in_memory.clone(),
        ConsensusError::Fenced {
            expected: Epoch(1),
            current: Epoch(2),
        },
    ));

    let (addr, _clock, mut state_rx, serve_handle, shutdown_tx) =
        boot_serving(faulty.clone(), &in_memory).await;

    let mut client = TsoServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    let status = client
        .get_ts(GetTsRequest { count: 1 })
        .await
        .expect_err("should fail with fenced during extension");

    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    let _hint = tsoracle_server::__priv_decode_leader_hint(&status)
        .expect("leader hint metadata must be present for Fenced");

    wait_until_not_serving(&mut state_rx).await;

    let _ = shutdown_tx.send(());
    let _ = serve_handle.await;
}

#[tokio::test]
async fn extend_window_maps_transient_driver_to_unavailable() {
    let in_memory = Arc::new(InMemoryDriver::new());
    let faulty = Arc::new(FaultyPersistDriver::new(
        in_memory.clone(),
        ConsensusError::TransientDriver(Box::new(std::io::Error::other("flap"))),
    ));

    let (addr, _clock, state_rx, serve_handle, shutdown_tx) =
        boot_serving(faulty.clone(), &in_memory).await;

    let mut client = TsoServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    let status = client
        .get_ts(GetTsRequest { count: 1 })
        .await
        .expect_err("should fail with unavailable on transient driver error");

    assert_eq!(
        status.code(),
        tonic::Code::Unavailable,
        "transient driver errors must map to UNAVAILABLE; got: {status:?}"
    );
    // Crucially: transient driver errors must NOT step the server down —
    // there is no evidence the epoch is stale.
    assert!(matches!(*state_rx.borrow(), ServingState::Serving));

    let _ = shutdown_tx.send(());
    let _ = serve_handle.await;
}

#[tokio::test]
async fn extend_window_maps_permanent_driver_to_internal() {
    let in_memory = Arc::new(InMemoryDriver::new());
    let faulty = Arc::new(FaultyPersistDriver::new(
        in_memory.clone(),
        ConsensusError::PermanentDriver(Box::new(std::io::Error::other("corrupted"))),
    ));

    let (addr, _clock, state_rx, serve_handle, shutdown_tx) =
        boot_serving(faulty.clone(), &in_memory).await;

    let mut client = TsoServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    let status = client
        .get_ts(GetTsRequest { count: 1 })
        .await
        .expect_err("should fail with internal on permanent driver error");

    assert_eq!(
        status.code(),
        tonic::Code::Internal,
        "permanent driver errors must map to INTERNAL; got: {status:?}"
    );
    // Permanent driver errors do not step the server down either: the
    // server has no proof the epoch is stale, only that the driver is sick.
    assert!(matches!(*state_rx.borrow(), ServingState::Serving));

    let _ = shutdown_tx.send(());
    let _ = serve_handle.await;
}

#[tokio::test]
async fn serve_with_shutdown_exits_when_watch_returns_error() {
    let addr = bind_unused().await;
    let in_memory = Arc::new(InMemoryDriver::new());
    let faulty = Arc::new(FaultyLoadDriver::new(in_memory.clone()));

    let server = Server::builder()
        .consensus_driver(faulty)
        .clock(Arc::new(MockClock::new(1_000)))
        .build()
        .unwrap();
    let state_rx = server.state_rx.clone();

    // No user shutdown — pending forever. The only way out is for the watch
    // task to die, which should trigger our tonic-cancel and propagate the
    // error.
    let serve_handle = tokio::spawn(async move {
        server
            .serve_with_shutdown(addr, futures::future::pending::<()>())
            .await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    in_memory.become_leader(Epoch(1));

    // serve_with_shutdown should complete with Err — load_high_water fails
    // on the leader transition, run_leader_watch returns Err, the spawn
    // wrapper poisons state and returns the error.
    let outcome = tokio::time::timeout(Duration::from_secs(5), serve_handle)
        .await
        .expect("serve_with_shutdown must return when watch dies")
        .expect("join")
        .expect_err("expected watch failure to surface as Err");

    match outcome {
        ServerError::Consensus(ConsensusError::PermanentDriver(_)) => {}
        other => panic!("expected Consensus(PermanentDriver), got: {other:?}"),
    }

    // Poisoned state must have been published before the spawn task returned.
    assert!(
        matches!(*state_rx.borrow(), ServingState::NotServing { .. }),
        "state must be poisoned to NotServing when watch terminates"
    );
}
