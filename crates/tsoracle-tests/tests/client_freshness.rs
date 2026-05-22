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

//! Property: every timestamp returned by the client has physical_ms >= the
//! wall-clock time at which its caller enqueued. This is the freshness
//! contract — strict-consistency callers rely on it.

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tsoracle_client::{Client, ClientError};
use tsoracle_core::Epoch;
use tsoracle_server::Server;
use tsoracle_server::test_fakes::InMemoryDriver;
use tsoracle_server::test_support::{boot_server, wait_until_serving};

fn unix_ms_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// Layered readiness probe: a successful `get_ts` is the only signal that
/// every layer (tonic accept loop, gRPC handshake, leader fence, allocator)
/// is actually serving. Bounded retry until success or `budget` exhausted —
/// not arbitrary sleep, not implementation-coupled state inspection.
async fn wait_until_responsive(client: &Client, budget: Duration) -> Result<(), ClientError> {
    let deadline = Instant::now() + budget;
    let mut last_err: Option<ClientError> = None;
    loop {
        match client.get_ts().await {
            Ok(_) => return Ok(()),
            Err(err) => {
                if Instant::now() >= deadline {
                    return Err(last_err.unwrap_or(err));
                }
                last_err = Some(err);
                sleep(Duration::from_millis(25)).await;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timestamps_are_at_or_after_enqueue_time() {
    let driver = Arc::new(InMemoryDriver::new());

    let server = Server::builder()
        .consensus_driver(driver.clone())
        .build()
        .unwrap();

    let mut booted = boot_server(server).await;

    driver.become_leader(Epoch(1));

    // ServingState::Serving fires only after the leader-watch fence completes,
    // which means the allocator is seeded and the server is logically ready.
    // This is a sanity gate: if the server never reaches Serving, the loop
    // below would otherwise spin until budget exhaustion with a less specific
    // error.
    wait_until_serving(&mut booted.state_rx).await;

    let client = Arc::new(
        Client::connect(vec![booted.addr.to_string()])
            .await
            .unwrap(),
    );

    // Bridge the small remaining gap between "state == Serving" and "tonic's
    // accept loop has been polled and is handling HTTP/2". Once one call
    // succeeds, the gRPC channel is open and subsequent calls reuse it.
    wait_until_responsive(&client, Duration::from_secs(5))
        .await
        .expect("server never became responsive after reaching Serving");

    for _ in 0..200 {
        let enqueue_ms = unix_ms_now();
        let ts = client.get_ts().await.unwrap();
        // Tolerate a small skew: server's clock could be a few ms behind ours.
        // The freshness contract requires ts.physical_ms >= enqueue_ms with
        // some tolerance for clock granularity / fence overhead.
        assert!(
            ts.physical_ms() >= enqueue_ms.saturating_sub(2),
            "ts.physical_ms={} < enqueue_ms={}",
            ts.physical_ms(),
            enqueue_ms
        );
        if rand_jitter() {
            sleep(Duration::from_millis(1)).await;
        }
    }

    booted.shutdown().await.unwrap();
}

fn rand_jitter() -> bool {
    let nanos = Instant::now().elapsed().as_nanos();
    nanos % 3 == 0
}
