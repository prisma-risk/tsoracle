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

//! In-process pedagogy demo of the failover fence in tsoracle-server.
//!
//! Runs a tsoracle [`Server`] against the [`InMemoryDriver`], connects a
//! gRPC client, and scripts a leader → follower → new-leader sequence,
//! asserting that every timestamp issued is strictly greater than the
//! previous one — including across the failover fence.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tonic::transport::Server as TonicServer;
use tsoracle_core::{Epoch, Timestamp};
use tsoracle_proto::v1::{GetTsRequest, tso_service_client::TsoServiceClient};
use tsoracle_server::test_fakes::InMemoryDriver;
use tsoracle_server::{Server, ServingState};

async fn bind_unused() -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

async fn wait_for_serving(rx: &mut tokio::sync::watch::Receiver<ServingState>, want_serving: bool) {
    loop {
        let matches = if want_serving {
            matches!(&*rx.borrow_and_update(), ServingState::Serving)
        } else {
            matches!(&*rx.borrow_and_update(), ServingState::NotServing { .. })
        };
        if matches {
            return;
        }
        if rx.changed().await.is_err() {
            return;
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter("warn").init();

    let driver = Arc::new(InMemoryDriver::new());

    let server = Server::builder().consensus_driver(driver.clone()).build()?;

    // Capture state_rx BEFORE consuming server via into_router.
    let mut state_rx = server.subscribe();
    // Hold the WatchGuard for the lifetime of the served router: dropping it
    // would cooperatively stop the leader-watch task.
    let (router, _watch_guard) = server.into_router()?;

    let addr = bind_unused().await;
    let (sd_tx, sd_rx) = oneshot::channel::<()>();
    let serve_task = tokio::spawn(async move {
        TonicServer::builder()
            .add_routes(router)
            .serve_with_shutdown(addr, async {
                let _ = sd_rx.await;
            })
            .await
            .expect("tonic serve failed");
    });

    // Give tonic time to bind.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut client = TsoServiceClient::connect(format!("http://{addr}")).await?;

    // ── Phase 1: become leader at epoch 1 ─────────────────────────────────────
    driver.become_leader(Epoch(1));
    wait_for_serving(&mut state_rx, true).await;
    println!("[serving] became leader at epoch=1");

    let mut last: Option<Timestamp> = None;
    for _ in 0..5 {
        let resp = client.get_ts(GetTsRequest { count: 1 }).await?.into_inner();
        let ts = Timestamp::pack(resp.physical_ms, resp.logical_start);
        println!(
            "  ts = {}.{} (epoch={})",
            resp.physical_ms,
            resp.logical_start,
            Epoch::from_wire(resp.epoch_hi, resp.epoch_lo).0
        );
        if let Some(prev) = last {
            assert!(ts > prev, "monotonicity violated within epoch 1");
        }
        last = Some(ts);
    }

    // ── Phase 2: become follower (fence) ──────────────────────────────────────
    driver.become_follower(None);
    wait_for_serving(&mut state_rx, false).await;
    let fenced = client.get_ts(GetTsRequest { count: 1 }).await;
    assert!(fenced.is_err(), "GetTs should fail while NotServing");
    println!(
        "[fenced] leadership lost, GetTs => {:?}",
        fenced.err().map(|e| e.code())
    );

    // ── Phase 3: become leader at epoch 2 ─────────────────────────────────────
    driver.become_leader(Epoch(2));
    wait_for_serving(&mut state_rx, true).await;
    println!("[serving] became leader at epoch=2");

    for _ in 0..5 {
        let resp = client.get_ts(GetTsRequest { count: 1 }).await?.into_inner();
        let ts = Timestamp::pack(resp.physical_ms, resp.logical_start);
        println!(
            "  ts = {}.{} (epoch={})",
            resp.physical_ms,
            resp.logical_start,
            Epoch::from_wire(resp.epoch_hi, resp.epoch_lo).0
        );
        let prev = last.expect("phase 1 issued at least one timestamp");
        assert!(ts > prev, "fence failed: ts <= last pre-fence ts");
        last = Some(ts);
    }

    println!("OK: 10 timestamps, all strictly monotonic across the fence.");

    let _ = sd_tx.send(());
    let _ = serve_task.await;
    Ok(())
}
