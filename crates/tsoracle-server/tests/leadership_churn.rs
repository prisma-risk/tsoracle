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
use tsoracle_server::test_fakes::{FaultKind, FaultyDriver, InMemoryDriver};
use tsoracle_server::test_support::{
    BootedServer, boot_server, wait_for_grpc_handshake, wait_until, wait_until_not_serving,
    wait_until_serving,
};
use tsoracle_server::{Server, ServerError, ServingState};

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
        let prior_count = self.persists_observed.fetch_add(1, Ordering::SeqCst);
        if prior_count == 0 {
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

/// Returns `TransientDriver` from every `persist_high_water` call while the
/// latch is set, modelling a driver (e.g. the paxos host) that reports
/// leadership loss as a transient append failure rather than `NotLeader`.
/// Leadership events and `load_high_water` delegate to the inner driver.
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

/// Boot a server with the given driver, drive it to `Serving`, and force the
/// clock past the seeded high-water so the next `GetTs` triggers an extension.
async fn boot_serving<D: ConsensusDriver + 'static>(
    driver: Arc<D>,
    in_memory_for_leader: &InMemoryDriver,
) -> (BootedServer, Arc<MockClock>) {
    let clock = Arc::new(MockClock::new(1_000));
    let server = Server::builder()
        .consensus_driver(driver)
        .clock(clock.clone())
        .window_ahead(Duration::from_millis(50))
        .failover_advance(Duration::from_millis(50))
        .build()
        .unwrap();

    let mut booted = boot_server(server).await;

    in_memory_for_leader.become_leader(Epoch(1));
    wait_until_serving(&mut booted.state_rx).await;
    wait_for_grpc_handshake(booted.addr, Duration::from_secs(5))
        .await
        .expect("tonic never accepted gRPC handshake");

    // Push clock past the failover-fence ceiling so the next request hits
    // WindowExhausted and triggers extend_window.
    clock.set(1_000_000);

    (booted, clock)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extend_window_maps_not_leader_to_failed_precondition_with_hint() {
    let in_memory = Arc::new(InMemoryDriver::new());
    let faulty = Arc::new(FaultyPersistDriver::new(
        in_memory.clone(),
        ConsensusError::NotLeader {
            observed: Some(Epoch(7)),
        },
    ));

    let (mut booted, _clock) = boot_serving(faulty.clone(), &in_memory).await;

    let mut client = TsoServiceClient::connect(format!("http://{}", booted.addr))
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
    // The service-layer step-down does not propagate NotLeader's `observed`
    // epoch into the hint: the persist path that fences a stale leader returns
    // Fenced (which does carry it), so #244 scoped epoch propagation there.
    assert!(hint.leader_epoch.is_none());

    // Allocator must have been cleared (no current epoch).
    wait_until_not_serving(&mut booted.state_rx).await;

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

    booted.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extend_window_maps_fenced_to_failed_precondition_with_hint() {
    let in_memory = Arc::new(InMemoryDriver::new());
    let faulty = Arc::new(FaultyPersistDriver::new(
        in_memory.clone(),
        ConsensusError::Fenced {
            expected: Epoch(1),
            current: Epoch(2),
        },
    ));

    let (mut booted, _clock) = boot_serving(faulty.clone(), &in_memory).await;

    let mut client = TsoServiceClient::connect(format!("http://{}", booted.addr))
        .await
        .unwrap();
    let status = client
        .get_ts(GetTsRequest { count: 1 })
        .await
        .expect_err("should fail with fenced during extension");

    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    let hint = tsoracle_server::__priv_decode_leader_hint(&status)
        .expect("leader hint metadata must be present for Fenced");

    // Fenced { current: Epoch(2) } names the epoch that fenced us. The hint
    // must carry it so the client can validate its next leader against it,
    // rather than dropping it as a bare FAILED_PRECONDITION redirect.
    let (hi, lo) = Epoch(2).to_wire();
    assert_eq!(
        hint.leader_epoch,
        Some(tsoracle_proto::v1::EpochWire { hi, lo }),
        "Fenced hint must propagate the current (new-leader) epoch"
    );

    wait_until_not_serving(&mut booted.state_rx).await;

    booted.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extend_window_maps_transient_driver_to_unavailable() {
    let in_memory = Arc::new(InMemoryDriver::new());
    let faulty = Arc::new(FaultyPersistDriver::new(
        in_memory.clone(),
        ConsensusError::TransientDriver(Box::new(std::io::Error::other("flap"))),
    ));

    let (booted, _clock) = boot_serving(faulty.clone(), &in_memory).await;

    let mut client = TsoServiceClient::connect(format!("http://{}", booted.addr))
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
    assert!(matches!(*booted.state_rx.borrow(), ServingState::Serving));

    booted.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extend_window_maps_permanent_driver_to_internal() {
    let in_memory = Arc::new(InMemoryDriver::new());
    let faulty = Arc::new(FaultyPersistDriver::new(
        in_memory.clone(),
        ConsensusError::PermanentDriver(Box::new(std::io::Error::other("corrupted"))),
    ));

    let (booted, _clock) = boot_serving(faulty.clone(), &in_memory).await;

    let mut client = TsoServiceClient::connect(format!("http://{}", booted.addr))
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
    assert!(matches!(*booted.state_rx.borrow(), ServingState::Serving));

    booted.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serve_with_shutdown_exits_when_watch_returns_error() {
    let in_memory = Arc::new(InMemoryDriver::new());
    let faulty = Arc::new(FaultyLoadDriver::new(in_memory.clone()));

    let server = Server::builder()
        .consensus_driver(faulty)
        .clock(Arc::new(MockClock::new(1_000)))
        .build()
        .unwrap();

    // No user shutdown — the only way out is for the watch task to die,
    // triggering tonic-cancel inside `serve_with_listener`. We move
    // `serve_handle` out and observe it directly; the rest of `booted`
    // (including its still-armed shutdown oneshot) stays in scope so the
    // sender isn't dropped — dropping it would close `shutdown_rx` and
    // resolve the user-shutdown future, masking the watch-task-death exit
    // path this test pins.
    let booted = boot_server(server).await;
    let state_rx = booted.state_rx.clone();

    in_memory.become_leader(Epoch(1));

    let outcome = tokio::time::timeout(Duration::from_secs(5), booted.serve_handle)
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fence_recovers_after_transient_burst_exceeding_old_budget() {
    // 10 transient persist faults is past the historical bound of 8. The fence
    // must keep retrying — on a node that never stops being leader — until the
    // 11th persist succeeds and the server reaches Serving. No leadership
    // transition is ever emitted, so recovery proves the retry loop does not
    // give up while still leader.
    let driver = Arc::new(FaultyDriver::new());
    driver.fail_next_persists(10, FaultKind::Transient);

    let server = Server::builder()
        .consensus_driver(driver.clone())
        .clock(Arc::new(MockClock::new(1_000)))
        .build()
        .unwrap();
    let mut booted = boot_server(server).await;

    driver.become_leader(Epoch(1));

    tokio::time::timeout(
        Duration::from_secs(15),
        wait_until_serving(&mut booted.state_rx),
    )
    .await
    .expect("fence must reach Serving despite a transient burst past the old budget");

    assert_eq!(
        driver.persist_call_count(),
        11,
        "expected 10 failed persists + 1 success"
    );

    booted.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sustained_leader_churn_never_exits_watch_loop() {
    // #204: under paxos's frequent leader churn, every new election opens a
    // volatile window where the first fence persist can fail as a transient
    // append error (paxos reports a step-down that way, not as NotLeader). The
    // earlier behavior exhausted the fence's retry budget and let the watch loop
    // return Err, poisoning serving and taking the server task down with it — so
    // the stress harness recorded zero successful client calls.
    //
    // `fence_observes_follower_transition_during_unbounded_transient_retry`
    // already pins a *single* step-down observed mid-retry plus one recovery.
    // This test pins the part #204 actually names: *sustained* oscillation. We
    // drive several Leader -> Follower -> Leader cycles, each opening with a
    // transient fence burst, and assert the watch task survives every cycle and
    // re-serves each time leadership returns.
    let driver = Arc::new(FaultyDriver::new());

    let server = Server::builder()
        .consensus_driver(driver.clone())
        .clock(Arc::new(MockClock::new(1_000)))
        .window_ahead(Duration::from_millis(50))
        .failover_advance(Duration::from_millis(50))
        .build()
        .unwrap();
    let mut booted = boot_server(server).await;

    // A transient burst per leader window: load_high_water succeeds, then the
    // first `BURST` persists fail transiently before one succeeds. Three is
    // already past nothing pathological but exercises the backoff path several
    // times per cycle.
    const BURST: usize = 3;
    const CYCLES: u128 = 4;

    for epoch in 1..=CYCLES {
        driver.fail_next_persists(BURST, FaultKind::Transient);
        driver.become_leader(Epoch(epoch));

        // Reaching Serving proves the fence rode through the transient burst
        // without giving up — i.e. the watch loop is still alive and re-fenced
        // at this epoch rather than having exited.
        tokio::time::timeout(
            Duration::from_secs(10),
            wait_until_serving(&mut booted.state_rx),
        )
        .await
        .unwrap_or_else(|_| panic!("fence must reach Serving on epoch {epoch} despite churn"));
        assert!(
            !booted.serve_handle.is_finished(),
            "watch/serve task must stay alive while Serving on epoch {epoch}",
        );

        // Lose leadership again. The watch loop publishes NotServing and loops
        // back to await the next event — it must not exit.
        driver.become_follower(None);
        tokio::time::timeout(
            Duration::from_secs(10),
            wait_until_not_serving(&mut booted.state_rx),
        )
        .await
        .unwrap_or_else(|_| panic!("must step down to NotServing on epoch {epoch}"));
        assert!(
            !booted.serve_handle.is_finished(),
            "watch/serve task must stay alive after stepping down on epoch {epoch}",
        );
    }

    // After sustained churn the server is still alive end-to-end: win one more
    // term and serve a real GetTs over gRPC.
    driver.fail_next_persists(BURST, FaultKind::Transient);
    driver.become_leader(Epoch(CYCLES + 1));
    tokio::time::timeout(
        Duration::from_secs(10),
        wait_until_serving(&mut booted.state_rx),
    )
    .await
    .expect("fence must reach Serving on the final term");
    wait_for_grpc_handshake(booted.addr, Duration::from_secs(5))
        .await
        .expect("tonic never accepted gRPC handshake");

    let mut client = TsoServiceClient::connect(format!("http://{}", booted.addr))
        .await
        .unwrap();
    let granted = client
        .get_ts(GetTsRequest { count: 1 })
        .await
        .expect("GetTs must succeed after sustained churn");
    assert_eq!(granted.into_inner().count, 1);

    // The transient-fault path was actually exercised, not vacuously skipped:
    // every cycle (plus the final term) attempted at least BURST + 1 persists.
    assert!(
        driver.persist_call_count() > CYCLES as u64,
        "transient fence faults must have been exercised across cycles",
    );

    booted.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fence_observes_follower_transition_during_unbounded_transient_retry() {
    let inner = Arc::new(InMemoryDriver::new());
    let driver = Arc::new(LatchingTransientDriver::new(inner.clone()));

    let server = Server::builder()
        .consensus_driver(driver.clone())
        .clock(Arc::new(MockClock::new(1_000)))
        .build()
        .unwrap();
    let mut booted = boot_server(server).await;

    // Fence enters an effectively infinite transient-retry loop.
    inner.become_leader(Epoch(1));

    // Retries must climb well past the historical bound of 8 — proving the loop
    // no longer gives up while still leader.
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if driver.persist_call_count() >= 12 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("fence must keep retrying past the old budget while latched transient");

    // A real leadership change arrives while the fence is mid-retry. The race
    // against the leadership stream must observe it. The Follower handler
    // publishes NotServing carrying the hint; the fence's own NotServing uses
    // leader_endpoint: None, so observing Some(hint) proves the event was
    // dispatched rather than spun past.
    inner.become_follower(Some("10.1.2.3:50551".into()));

    tokio::time::timeout(
        Duration::from_secs(10),
        wait_until(&mut booted.state_rx, |s| {
            matches!(
                s,
                ServingState::NotServing {
                    leader_endpoint: Some(_),
                    ..
                }
            )
        }),
    )
    .await
    .expect("Follower transition during retry must be observed and published with its hint");

    // Once the Follower is dispatched the loop is back on stream.next(), so no
    // further persist attempts occur.
    let settled = driver.persist_call_count();
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(
        driver.persist_call_count(),
        settled,
        "no more fence persist attempts after stepping down to Follower"
    );

    // Recovery: clear the fault and re-win leadership at a new epoch.
    driver.clear_faults();
    inner.become_leader(Epoch(2));
    tokio::time::timeout(
        Duration::from_secs(10),
        wait_until_serving(&mut booted.state_rx),
    )
    .await
    .expect("fence must reach Serving after re-winning leadership");

    booted.shutdown().await.unwrap();
}
