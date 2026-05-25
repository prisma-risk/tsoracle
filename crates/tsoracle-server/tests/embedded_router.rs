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

use core::pin::Pin;
use futures::{Stream, StreamExt, stream};
use std::{sync::Arc, time::Duration};
use tokio::sync::Notify;
use tsoracle_consensus::{ConsensusDriver, ConsensusError, LeaderState};
use tsoracle_core::Epoch;
use tsoracle_proto::v1::{GetTsRequest, tso_service_client::TsoServiceClient};
use tsoracle_server::Server;
use tsoracle_server::test_fakes::InMemoryDriver;
use tsoracle_server::test_support::{
    boot_router, wait_for_grpc_handshake, wait_until_not_serving, wait_until_serving,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn embedded_router_serves_via_caller_owned_listener() {
    let driver = Arc::new(InMemoryDriver::new());

    let tsoracle = Server::builder()
        .consensus_driver(driver.clone())
        .build()
        .unwrap();

    // Clone state_rx before into_router consumes the Server.
    let mut state_rx = tsoracle.subscribe();
    let (router, _leader_watch) = tsoracle
        .into_router()
        .expect("into_router is infallible without the reflection feature");

    let booted = boot_router(router).await;

    driver.become_leader(Epoch(1));
    wait_until_serving(&mut state_rx).await;
    wait_for_grpc_handshake(booted.addr, Duration::from_secs(5))
        .await
        .expect("tonic never accepted gRPC handshake");

    let mut client = TsoServiceClient::connect(format!("http://{}", booted.addr))
        .await
        .unwrap();
    let resp = client
        .get_ts(GetTsRequest { count: 1 })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.count, 1);
    // Epoch(1).to_wire() == (hi: 0, lo: 1).
    assert_eq!((resp.epoch_hi, resp.epoch_lo), (0, 1));

    booted.shutdown().await.unwrap();
}

/// Regression for #72: a clean EOF on the consensus driver's leadership
/// stream must trigger the same fail-safe poisoning as an error return,
/// even when the embedder never observes the watch task's outcome directly.
///
/// Embedders who mount `into_router` directly rely on the documented
/// out-of-band guarantee that "any termination of the watch task publishes
/// `ServingState::NotServing`". Before the fix, the stream-end branch of
/// `run_leader_watch` returned `Ok(())`, the conditional poisoning in
/// `into_router` was skipped, and `Serving` remained published — leaving
/// the allocator able to issue timestamps from a stale epoch.
///
/// The `WatchGuard` is held alive for the whole test: dropping it now
/// cooperatively cancels the task (see
/// `dropping_watch_guard_cancels_leader_watch_and_poisons`), which would
/// pre-empt the stream-EOF path this test pins.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn embedded_router_poisons_when_leadership_stream_closes() {
    // EOF is held back by `eof_gate` so the test can deterministically
    // observe the Serving → NotServing transition pair, instead of racing
    // the watch task through both transitions and only catching the final
    // state.
    let eof_gate = Arc::new(Notify::new());
    let driver: Arc<dyn ConsensusDriver> = Arc::new(LeaderThenGatedEofDriver {
        epoch: Epoch(1),
        eof_gate: eof_gate.clone(),
    });

    let tsoracle = Server::builder().consensus_driver(driver).build().unwrap();

    let mut state_rx = tsoracle.subscribe();
    let (router, _leader_watch) = tsoracle
        .into_router()
        .expect("into_router is infallible without the reflection feature");

    // Keep `_leader_watch` alive: this test exercises the stream-EOF
    // poisoning path, not the drop-cancels path, and the embedder shape
    // under test never inspects the guard's outcome.

    let booted = boot_router(router).await;

    // Phase 1: the driver emits Leader and the fence reaches Serving.
    tokio::time::timeout(Duration::from_secs(5), wait_until_serving(&mut state_rx))
        .await
        .expect("fence never reached Serving after Leader event");

    // Phase 2: release the gate; the leadership stream ends and the watch
    // task must publish NotServing before terminating.
    eof_gate.notify_one();
    tokio::time::timeout(
        Duration::from_secs(5),
        wait_until_not_serving(&mut state_rx),
    )
    .await
    .expect("watch task did not publish NotServing after leadership stream ended");

    wait_for_grpc_handshake(booted.addr, Duration::from_secs(5))
        .await
        .expect("tonic never accepted gRPC handshake");

    let mut client = TsoServiceClient::connect(format!("http://{}", booted.addr))
        .await
        .unwrap();
    let status = client
        .get_ts(GetTsRequest { count: 1 })
        .await
        .expect_err("GetTs must fail fast once the watch task has terminated");
    assert_eq!(
        status.code(),
        tonic::Code::FailedPrecondition,
        "post-EOF GetTs must surface FAILED_PRECONDITION; got {status:?}",
    );

    booted.shutdown().await.unwrap();
}

/// #334: dropping the `WatchGuard` must cooperatively stop the leader-watch
/// task even when its leadership stream stays open forever, and the stop must
/// poison serving state so a still-mounted `Routes` fails fast.
///
/// `InMemoryDriver`'s leadership stream never EOFs while the driver is alive,
/// so before the fix the task would stay `Serving` indefinitely after the
/// guard was dropped — keeping `Arc<Server>` (and the driver) alive with no
/// way to stop it. The guard's cancel-on-drop is the only thing that ends the
/// task here.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_watch_guard_cancels_leader_watch_and_poisons() {
    let driver = Arc::new(InMemoryDriver::new());

    let tsoracle = Server::builder()
        .consensus_driver(driver.clone())
        .build()
        .unwrap();

    let mut state_rx = tsoracle.subscribe();
    let (router, leader_watch) = tsoracle
        .into_router()
        .expect("into_router is infallible without the reflection feature");

    let booted = boot_router(router).await;

    driver.become_leader(Epoch(1));
    wait_until_serving(&mut state_rx).await;
    wait_for_grpc_handshake(booted.addr, Duration::from_secs(5))
        .await
        .expect("tonic never accepted gRPC handshake");

    // Drop the guard. The leadership stream is still open, so only the
    // guard's cancel-on-drop can stop the task.
    drop(leader_watch);

    tokio::time::timeout(
        Duration::from_secs(5),
        wait_until_not_serving(&mut state_rx),
    )
    .await
    .expect("dropping the WatchGuard did not stop the leader-watch task");

    let mut client = TsoServiceClient::connect(format!("http://{}", booted.addr))
        .await
        .unwrap();
    let status = client
        .get_ts(GetTsRequest { count: 1 })
        .await
        .expect_err("GetTs must fail fast once the watch task has been cancelled");
    assert_eq!(
        status.code(),
        tonic::Code::FailedPrecondition,
        "post-cancel GetTs must surface FAILED_PRECONDITION; got {status:?}",
    );

    booted.shutdown().await.unwrap();
}

/// #334: `WatchGuard::shutdown().await` cooperatively stops the task and
/// reports its outcome. A cancelled task returns `Ok(())` (we asked for it),
/// and serving state is poisoned to `NotServing` before the future resolves.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watch_guard_shutdown_returns_ok_and_poisons() {
    let driver = Arc::new(InMemoryDriver::new());

    let tsoracle = Server::builder()
        .consensus_driver(driver.clone())
        .build()
        .unwrap();

    let mut state_rx = tsoracle.subscribe();
    let (router, leader_watch) = tsoracle
        .into_router()
        .expect("into_router is infallible without the reflection feature");

    let _booted = boot_router(router).await;

    driver.become_leader(Epoch(1));
    wait_until_serving(&mut state_rx).await;

    let outcome = tokio::time::timeout(Duration::from_secs(5), leader_watch.shutdown())
        .await
        .expect("WatchGuard::shutdown did not complete");
    assert!(
        outcome.is_ok(),
        "cooperative shutdown of a healthy watch task must return Ok(()); got {outcome:?}",
    );

    // shutdown() awaited the task to completion, which poisons serving state.
    assert!(
        matches!(
            &*state_rx.borrow(),
            tsoracle_server::ServingState::NotServing { .. }
        ),
        "shutdown() must leave serving state NotServing",
    );
}

/// #334: `WatchGuard::abort()` is the escape hatch for embedders that cannot
/// await a cooperative `shutdown()` — it hard-aborts the leader-watch task.
/// Unlike drop/`shutdown()`, abort bypasses the cooperative cancel arm, so it
/// makes no promise to poison serving state (the task "may be torn down
/// mid-fence"). What it *does* guarantee is that the task is gone: a live task
/// would publish `Serving` on a later leadership gain, so after abort we prove
/// the task is dead by showing that a subsequent `become_leader` never drives
/// the Serving transition.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watch_guard_abort_stops_the_task() {
    let driver = Arc::new(InMemoryDriver::new());

    let tsoracle = Server::builder()
        .consensus_driver(driver.clone())
        .build()
        .unwrap();

    let mut state_rx = tsoracle.subscribe();
    let (router, leader_watch) = tsoracle
        .into_router()
        .expect("into_router is infallible without the reflection feature");

    let _booted = boot_router(router).await;

    // Abort while still NotServing (no leader yet), then let the runtime tear
    // the task down before we probe it.
    leader_watch.abort();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // A live watch task would react to this leadership gain by publishing
    // Serving within milliseconds; the aborted task cannot, so the transition
    // must never arrive. The 2 s ceiling is far above a healthy transition.
    driver.become_leader(Epoch(1));
    let serving =
        tokio::time::timeout(Duration::from_secs(2), wait_until_serving(&mut state_rx)).await;
    assert!(
        serving.is_err(),
        "an aborted watch task must not transition to Serving on a later leadership gain",
    );
}

/// Driver whose `leadership_events()` stream yields one `Leader` event and
/// then ends after the test signals `eof_gate`. The two-phase shape lets the
/// test pin the Serving → NotServing transition deterministically; modelling
/// a real consensus backend whose watch can terminate on driver shutdown,
/// restart, or partition recovery.
struct LeaderThenGatedEofDriver {
    epoch: Epoch,
    eof_gate: Arc<Notify>,
}

#[async_trait::async_trait]
impl ConsensusDriver for LeaderThenGatedEofDriver {
    fn leadership_events(&self) -> Pin<Box<dyn Stream<Item = LeaderState> + Send>> {
        let epoch = self.epoch;
        let gate = self.eof_gate.clone();
        let leader = stream::iter(vec![LeaderState::Leader { epoch }]);
        // Yield once after the gate fires, then flat-map that unit to an
        // empty stream so the overall stream ends without a spurious event.
        let wait_then_end = stream::once(async move {
            gate.notified().await;
        })
        .flat_map(|()| stream::empty::<LeaderState>());
        Box::pin(leader.chain(wait_then_end))
    }
    async fn load_high_water(&self) -> Result<u64, ConsensusError> {
        Ok(0)
    }
    async fn persist_high_water(
        &self,
        at_least: u64,
        _epoch: Epoch,
    ) -> Result<u64, ConsensusError> {
        Ok(at_least)
    }
}
