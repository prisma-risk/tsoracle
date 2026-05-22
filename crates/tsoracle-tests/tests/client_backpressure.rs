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

//! End-to-end coverage for the coalescing driver's backpressure bound and
//! per-chunk delivery. Unlike the unit tests in `tsoracle-client`, these
//! exercise the contract through the real gRPC stack — server, transport,
//! channel pool, retry, response decode — so a regression in any layer
//! between `Client::get_ts_batch` and the driver's `select!` arms surfaces
//! here as well.

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::{sleep, timeout};
use tsoracle_client::Client;
use tsoracle_core::{Epoch, LOGICAL_MAX};
use tsoracle_server::Server;
use tsoracle_server::test_fakes::StallableDriver;
use tsoracle_server::test_support::{boot_server, wait_until_serving};

/// One outbound `GetTs` triggers one `persist_high_water` call when
/// `window_ahead == 0`; every chunk dispatched by the client driver maps
/// to exactly one persist at the server, which gives tests a reliable
/// per-chunk injection point.
const NO_WINDOW_AHEAD: Duration = Duration::ZERO;

/// Layered readiness probe: the only signal that every layer (tonic
/// accept loop, gRPC handshake, leader fence, allocator) is actually
/// serving is a successful `get_ts`. Bounded retry until success or the
/// budget is exhausted.
async fn wait_until_responsive(client: &Client, budget: Duration) {
    let deadline = Instant::now() + budget;
    loop {
        if client.get_ts().await.is_ok() {
            return;
        }
        if Instant::now() >= deadline {
            panic!("server never became responsive within {budget:?}");
        }
        sleep(Duration::from_millis(25)).await;
    }
}

/// High-concurrency smoke test: `Driver::request`'s send-side gate kicks in
/// repeatedly under genuine wire conditions, but every request must still
/// resolve correctly. This is the realistic, non-stalled load path that
/// callers actually run, and it must not regress with the new gates in the
/// driver's `tokio::select!` arms.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_concurrent_requests_all_complete() {
    let driver = Arc::new(StallableDriver::new());
    let server = Server::builder()
        .consensus_driver(driver.clone())
        .build()
        .unwrap();
    let mut booted = boot_server(server).await;
    driver.become_leader(Epoch(1));
    wait_until_serving(&mut booted.state_rx).await;

    let client = Arc::new(
        Client::connect(vec![booted.addr.to_string()])
            .await
            .unwrap(),
    );
    wait_until_responsive(&client, Duration::from_secs(5)).await;

    // 8192 = 2 * QUEUE_CAPACITY; well above the driver's documented bound,
    // so the new gates exercise both the in-flight branch and the cold-start
    // flush window during this run.
    let concurrent = 8_192_usize;
    let mut handles = Vec::with_capacity(concurrent);
    for _ in 0..concurrent {
        let client = client.clone();
        handles.push(tokio::spawn(async move { client.get_ts().await }));
    }
    let results = futures::future::join_all(handles).await;
    let mut succeeded = 0_usize;
    for result in results {
        if result.expect("join must succeed").is_ok() {
            succeeded += 1;
        }
    }
    assert_eq!(
        succeeded, concurrent,
        "all {concurrent} concurrent requests must succeed under steady-state load",
    );

    booted.shutdown().await.unwrap();
}

/// Per-chunk delivery, end-to-end through the gRPC stack. Two cap-sized
/// `get_ts_batch` calls coalesce into one window then split into two
/// chunks at the client; the server stalls the second chunk's `persist`.
/// The first caller's response must arrive without waiting for the
/// second chunk to unblock — otherwise the driver is still accumulating
/// chunk results before delivering.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_chunk_delivers_before_slow_second_chunk_e2e() {
    let driver = Arc::new(StallableDriver::new());
    let server = Server::builder()
        .consensus_driver(driver.clone())
        // Force a `persist_high_water` call on every GetTs RPC: setting
        // both `window_ahead` and `failover_advance` to ZERO disables any
        // server-side pre-extension of the committed window, so each
        // outgoing chunk requires a persist round-trip — which gives
        // `StallableDriver` a reliable per-chunk hook.
        .window_ahead(NO_WINDOW_AHEAD)
        .failover_advance(Duration::ZERO)
        .build()
        .unwrap();
    let mut booted = boot_server(server).await;
    driver.become_leader(Epoch(1));
    wait_until_serving(&mut booted.state_rx).await;

    let client = Arc::new(
        Client::connect(vec![booted.addr.to_string()])
            .await
            .unwrap(),
    );
    // The warmup call(s) take an opaque number of persist invocations
    // (allocator seed + first GetTs). Snapshot the counter *after* the
    // server is responsive so the stall threshold targets only the
    // chunked batch below.
    wait_until_responsive(&client, Duration::from_secs(5)).await;
    let baseline_persist_calls = driver.persist_call_count();
    // Allow the first chunk's persist (`baseline`) to proceed; stall the
    // second chunk's persist (`baseline + 1`) and beyond.
    driver.stall_from(baseline_persist_calls + 1);

    // Each request is at the per-RPC cap, so `chunk_queue` produces one
    // chunk per waiter — exactly the shape this test needs.
    let first = {
        let client = client.clone();
        tokio::spawn(async move { client.get_ts_batch(LOGICAL_MAX + 1).await })
    };
    let second = {
        let client = client.clone();
        tokio::spawn(async move { client.get_ts_batch(LOGICAL_MAX + 1).await })
    };

    // `first` must complete promptly. If the driver accumulated both
    // chunks' responses before delivering, `first` would block on the
    // stalled second chunk and this timeout would fire.
    let first_timestamps = timeout(Duration::from_secs(5), first)
        .await
        .expect("first chunk must deliver before the stalled second chunk")
        .expect("join")
        .expect("first batch must succeed");
    assert_eq!(first_timestamps.len(), (LOGICAL_MAX + 1) as usize);

    // The second caller is still waiting on the stalled persist call —
    // give the runtime a beat to be sure no spurious wake landed and then
    // observe the JoinHandle is not finished.
    sleep(Duration::from_millis(100)).await;
    assert!(
        !second.is_finished(),
        "second caller must remain pending while its chunk's persist is stalled",
    );

    driver.release();
    let second_timestamps = timeout(Duration::from_secs(5), second)
        .await
        .expect("second chunk must complete after release")
        .expect("join")
        .expect("second batch must succeed");
    assert_eq!(second_timestamps.len(), (LOGICAL_MAX + 1) as usize);

    booted.shutdown().await.unwrap();
}

/// Soak test: sustained high-concurrency load against a fast server for a
/// bounded budget. Marked `#[ignore]` so CI can opt in via
/// `cargo test -p tsoracle-tests --release -- --ignored backpressure_soak`.
///
/// Asserts: no panics, no errors, every request completes. The shape is
/// "many clients, many requests each, mixed batch sizes" — covers the
/// realistic application pattern (one shared `Arc<Client>`, application
/// tasks each doing one batch at a time).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "long-running soak; run with --ignored"]
async fn backpressure_soak() {
    let driver = Arc::new(StallableDriver::new());
    let server = Server::builder()
        .consensus_driver(driver.clone())
        .build()
        .unwrap();
    let mut booted = boot_server(server).await;
    driver.become_leader(Epoch(1));
    wait_until_serving(&mut booted.state_rx).await;

    let client = Arc::new(
        Client::connect(vec![booted.addr.to_string()])
            .await
            .unwrap(),
    );
    wait_until_responsive(&client, Duration::from_secs(5)).await;

    let concurrent = 512_usize;
    let iterations_per_task = 200_usize;
    let started_at = Instant::now();
    let mut handles = Vec::with_capacity(concurrent);
    for task_idx in 0..concurrent {
        let client = client.clone();
        handles.push(tokio::spawn(async move {
            // Mix single and batched requests so coalescing meets every
            // chunk-shape: single waiter, small batch, near-cap batch.
            let mut local_errors: u32 = 0;
            let mut local_timestamps: u64 = 0;
            for iter in 0..iterations_per_task {
                let count = match (task_idx + iter) % 4 {
                    0 => 1,
                    1 => 16,
                    2 => 4_096,
                    _ => LOGICAL_MAX + 1,
                };
                match client.get_ts_batch(count).await {
                    Ok(timestamps) => {
                        assert_eq!(timestamps.len(), count as usize);
                        local_timestamps += timestamps.len() as u64;
                    }
                    Err(_) => local_errors += 1,
                }
            }
            (local_errors, local_timestamps)
        }));
    }
    let mut total_errors: u32 = 0;
    let mut total_timestamps: u64 = 0;
    for handle in handles {
        let (errs, ts) = handle.await.expect("soak task must not panic");
        total_errors += errs;
        total_timestamps += ts;
    }
    let elapsed = started_at.elapsed();
    eprintln!(
        "soak: {concurrent} tasks × {iterations_per_task} iters → {total_timestamps} timestamps \
         in {elapsed:.2?} ({:.0} ts/s); {total_errors} errors",
        total_timestamps as f64 / elapsed.as_secs_f64(),
    );
    assert_eq!(total_errors, 0, "soak must complete with no client errors",);

    booted.shutdown().await.unwrap();
}
