//
//  ░▀█▀░█▀▀░█▀█░█▀▄░█▀█░█▀▀░█░░░█▀▀
//  ░░█░░▀▀█░█░█░█▀▄░█▀█░█░░░█░░░█▀▀
//  ░░▀░░▀▀▀░▀▀▀░▀░▀░▀░▀░▀▀▀░▀▀▀░▀▀▀
//
//  tsoracle — Distributed Timestamp Oracle
//  https://www.tsoracle.rs
//
//  Copyright (c) 2026 Prisma Risk
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

//! Regression tests for retry semantics during leadership churn.
//!
//! `persist_high_water` returns `NotLeader` / `Fenced` mid-extension — the
//! service must surface `FAILED_PRECONDITION` with a `LeaderHint`, poison server
//! state (allocator cleared, `ServingState::NotServing` published), and remain
//! in that state for subsequent calls. Transient and permanent driver errors
//! must map to `UNAVAILABLE` and `INTERNAL` respectively, without poisoning
//! state. A separate test pins that `serve_with_shutdown` exits with the watch's
//! error when `run_leader_watch` returns `Err`.
//!
//! The timing-fragile fence retry / sustained-churn tests live in
//! `leadership_churn_dst.rs`, where they run deterministically under turmoil's
//! simulated clock instead of waiting on real backoff.

use core::pin::Pin;
use futures::Stream;
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tsoracle_consensus::{ConsensusDriver, ConsensusError, LeaderState};
use tsoracle_core::Epoch;
use tsoracle_proto::v1::{GetTsRequest, tso_service_client::TsoServiceClient};
use tsoracle_server::test_fakes::{InMemoryDriver, MockClock};
use tsoracle_server::test_support::{
    BootedServer, boot_server, wait_for_grpc_handshake, wait_until_not_serving, wait_until_serving,
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

/// Wraps `InMemoryDriver`: the leader-transition fence persist succeeds (so the
/// server reaches `Serving` and the allocator seats a ceiling), but every
/// *later* persist durably returns a stale value (`0`) that sits below that
/// ceiling. The server's `commit_extension` then drops the extension as
/// `NotAdvanced` — the benign "persisted but cannot advance" outcome (a leading
/// indicator of persist reordering) that `extend_window` logs and counts
/// without stepping the leader down.
struct StalePersistDriver {
    inner: Arc<InMemoryDriver>,
    persists_observed: AtomicUsize,
}

impl StalePersistDriver {
    fn new(inner: Arc<InMemoryDriver>) -> Self {
        Self {
            inner,
            persists_observed: AtomicUsize::new(0),
        }
    }
}

#[async_trait::async_trait]
impl ConsensusDriver for StalePersistDriver {
    fn leadership_events(&self) -> Pin<Box<dyn Stream<Item = LeaderState> + Send>> {
        self.inner.leadership_events()
    }

    async fn load_high_water(&self) -> Result<u64, ConsensusError> {
        self.inner.load_high_water().await
    }

    async fn persist_high_water(&self, at_least: u64, epoch: Epoch) -> Result<u64, ConsensusError> {
        if self.persists_observed.fetch_add(1, Ordering::SeqCst) == 0 {
            // Fence persist: let it through so the allocator seats a ceiling.
            return self.inner.persist_high_water(at_least, epoch).await;
        }
        // A durable success that returns a value below the seated ceiling: the
        // commit that follows cannot advance and is dropped as NotAdvanced.
        Ok(0)
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

/// A persist that durably succeeds but returns a value below the committed
/// ceiling makes `commit_extension` drop the extension as `NotAdvanced` — a
/// benign outcome (persist reordering) that `extend_window` logs and counts but
/// must NOT step the leader down. Drives the `CommitOutcome::Ignored` arm of
/// `extend_window`, which the error-injection tests above never reach: persist
/// returns `Ok`, so the loud `NotLeader` / `Fenced` / driver-error arms are all
/// skipped and only the post-persist ignored-commit branch runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extend_window_ignored_commit_is_benign_and_keeps_serving() {
    // A TRACE subscriber so the "window extension commit ignored after persist"
    // debug line formats its fields under test rather than short-circuiting.
    use tracing_subscriber::filter::LevelFilter;
    let _ = tracing_subscriber::fmt()
        .with_max_level(LevelFilter::TRACE)
        .with_test_writer()
        .try_init();

    let in_memory = Arc::new(InMemoryDriver::new());
    let stale = Arc::new(StalePersistDriver::new(in_memory.clone()));

    let (booted, _clock) = boot_serving(stale, &in_memory).await;

    let mut client = TsoServiceClient::connect(format!("http://{}", booted.addr))
        .await
        .unwrap();
    // The extension persists, but its returned value cannot advance the ceiling,
    // so the window stays exhausted and the retried grant surfaces Internal.
    let status = client
        .get_ts(GetTsRequest { count: 1 })
        .await
        .expect_err("a dropped (NotAdvanced) extension leaves the window exhausted");
    assert_eq!(
        status.code(),
        tonic::Code::Internal,
        "got status: {status:?}"
    );
    assert!(
        status.message().contains("window exhausted"),
        "got message: {}",
        status.message(),
    );

    // NotAdvanced is benign: unlike NotLeader / Fenced it must leave the leader
    // serving, since there is no evidence the epoch is stale.
    assert!(
        matches!(*booted.state_rx.borrow(), ServingState::Serving),
        "an ignored (NotAdvanced) commit must not step the leader down",
    );

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
