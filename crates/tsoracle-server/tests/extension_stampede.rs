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

//! Regression test for the window-extension stampede.
//!
//! When `committed_high_water` is exceeded, every concurrent `get_ts` that
//! hits `WindowExhausted` enters `extend_window`. Without single-flight, each
//! caller independently runs `persist_high_water`, amplifying one cliff into
//! N consensus round-trips. With the `extension_lock` mutex plus a
//! recheck-after-acquire, exactly one caller in any concurrent burst contacts
//! consensus; the rest find the window already extended and return.

use core::pin::Pin;
use futures::Stream;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Barrier;
use tsoracle_consensus::{ConsensusDriver, ConsensusError, LeaderState};
use tsoracle_core::{Epoch, testing::MockClock};
use tsoracle_proto::v1::{GetTsRequest, tso_service_client::TsoServiceClient};
use tsoracle_server::Server;
use tsoracle_server::test_fakes::InMemoryDriver;
use tsoracle_server::test_support::{boot_server, wait_for_grpc_handshake, wait_until_serving};

/// Wraps `InMemoryDriver` to count `persist_high_water` calls and inject a
/// configurable delay, so concurrent stampeders pile up inside the extension
/// path during the test window rather than serializing naturally.
struct StampedeProbeDriver {
    inner: Arc<InMemoryDriver>,
    persist_calls: AtomicUsize,
    persist_delay: Duration,
}

impl StampedeProbeDriver {
    fn new(inner: Arc<InMemoryDriver>, persist_delay: Duration) -> Self {
        Self {
            inner,
            persist_calls: AtomicUsize::new(0),
            persist_delay,
        }
    }

    fn persist_calls(&self) -> usize {
        self.persist_calls.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl ConsensusDriver for StampedeProbeDriver {
    fn leadership_events(&self) -> Pin<Box<dyn Stream<Item = LeaderState> + Send>> {
        self.inner.leadership_events()
    }

    async fn load_high_water(&self) -> Result<u64, ConsensusError> {
        self.inner.load_high_water().await
    }

    async fn persist_high_water(&self, at_least: u64, epoch: Epoch) -> Result<u64, ConsensusError> {
        self.persist_calls.fetch_add(1, Ordering::SeqCst);
        if !self.persist_delay.is_zero() {
            tokio::time::sleep(self.persist_delay).await;
        }
        self.inner.persist_high_water(at_least, epoch).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extension_stampede_coalesces_to_single_persist() {
    const STAMPEDE_SIZE: usize = 50;

    let in_memory = Arc::new(InMemoryDriver::new());
    let probe = Arc::new(StampedeProbeDriver::new(
        in_memory.clone(),
        Duration::from_millis(50),
    ));
    let clock = Arc::new(MockClock::new(1_000));

    let server = Server::builder()
        .consensus_driver(probe.clone())
        .clock(clock.clone())
        .window_ahead(Duration::from_millis(50))
        .failover_advance(Duration::from_millis(50))
        .build()
        .unwrap();

    let mut booted = boot_server(server).await;

    in_memory.become_leader(Epoch(1));
    wait_until_serving(&mut booted.state_rx).await;
    wait_for_grpc_handshake(booted.addr, Duration::from_secs(5))
        .await
        .expect("tonic never accepted gRPC handshake");

    // Baseline accounts for the seed persist done by run_leader_watch.
    let baseline = probe.persist_calls();

    // Push the clock past the seeded committed_high_water so every grant
    // attempt hits WindowExhausted.
    clock.set(10_000);

    let client = TsoServiceClient::connect(format!("http://{}", booted.addr))
        .await
        .unwrap();
    let barrier = Arc::new(Barrier::new(STAMPEDE_SIZE));
    let mut tasks = Vec::with_capacity(STAMPEDE_SIZE);
    for _ in 0..STAMPEDE_SIZE {
        let barrier = barrier.clone();
        let mut client = client.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            client.get_ts(GetTsRequest { count: 1 }).await
        }));
    }

    let mut ok = 0;
    for task in tasks {
        if task.await.unwrap().is_ok() {
            ok += 1;
        }
    }
    assert_eq!(ok, STAMPEDE_SIZE, "all stampeding requests should succeed");

    let extensions = probe.persist_calls() - baseline;
    assert_eq!(
        extensions, 1,
        "expected exactly one persist_high_water call for {STAMPEDE_SIZE} concurrent \
         stampeders, observed {extensions} — single-flight is not engaged"
    );

    booted.shutdown().await.unwrap();
}
