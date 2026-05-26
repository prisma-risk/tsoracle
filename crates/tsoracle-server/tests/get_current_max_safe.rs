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

use std::{sync::Arc, time::Duration};
use tsoracle_core::Epoch;
use tsoracle_proto::v1::{
    GetCurrentMaxSafeRequest, GetTsRequest, tso_service_client::TsoServiceClient,
};
use tsoracle_server::Server;
use tsoracle_server::test_fakes::InMemoryDriver;
use tsoracle_server::test_support::{boot_server, wait_for_grpc_handshake, wait_until_serving};

const WINDOW_AHEAD: Duration = Duration::from_millis(500);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_current_max_safe_returns_zero_on_follower() {
    let driver = Arc::new(InMemoryDriver::new());

    let server = Server::builder()
        .consensus_driver(driver.clone())
        .window_ahead(WINDOW_AHEAD)
        .failover_advance(Duration::from_millis(200))
        .build()
        .unwrap();

    let booted = boot_server(server).await;

    driver.become_follower(Some("10.9.8.7:50551".into()));
    wait_for_grpc_handshake(booted.addr, Duration::from_secs(5))
        .await
        .expect("tonic never accepted gRPC handshake");

    let mut client = TsoServiceClient::connect(format!("http://{}", booted.addr))
        .await
        .unwrap();

    let resp = client
        .get_current_max_safe(GetCurrentMaxSafeRequest {})
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        resp.max_safe_physical_ms, 0,
        "follower must report safe-point 0",
    );
    assert_eq!(
        (resp.epoch_hi, resp.epoch_lo),
        (0, 0),
        "follower must report epoch zero",
    );

    booted.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_current_max_safe_advances_and_is_bounded() {
    let driver = Arc::new(InMemoryDriver::new());

    let server = Server::builder()
        .consensus_driver(driver.clone())
        .window_ahead(WINDOW_AHEAD)
        .failover_advance(Duration::from_millis(200))
        .build()
        .unwrap();

    let mut booted = boot_server(server).await;

    driver.become_leader(Epoch(1));
    wait_until_serving(&mut booted.state_rx).await;
    wait_for_grpc_handshake(booted.addr, Duration::from_secs(5))
        .await
        .expect("tonic never accepted gRPC handshake");

    let mut client = TsoServiceClient::connect(format!("http://{}", booted.addr))
        .await
        .unwrap();

    // Issue a few timestamps so the allocator advances its committed high-water
    // through window extensions; otherwise the safe-point only reflects the
    // initial fence-seeded ceiling.
    for _ in 0..4 {
        client.get_ts(GetTsRequest { count: 64 }).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let safe_a = client
        .get_current_max_safe(GetCurrentMaxSafeRequest {})
        .await
        .unwrap()
        .into_inner();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let safe_b = client
        .get_current_max_safe(GetCurrentMaxSafeRequest {})
        .await
        .unwrap()
        .into_inner();

    assert!(
        safe_a.max_safe_physical_ms > 0,
        "safe-point did not advance after issued timestamps",
    );
    assert!(
        safe_b.max_safe_physical_ms >= safe_a.max_safe_physical_ms,
        "safe-point regressed: {} -> {}",
        safe_a.max_safe_physical_ms,
        safe_b.max_safe_physical_ms,
    );
    // Epoch(1).to_wire() == (hi: 0, lo: 1) — same encoding GetTs uses.
    assert_eq!((safe_a.epoch_hi, safe_a.epoch_lo), (0, 1));

    booted.shutdown().await.unwrap();
}
