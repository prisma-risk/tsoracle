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

use std::sync::Arc;
use std::time::Duration;

use tonic::Code;
use tsoracle_client::{Client, ClientError};
use tsoracle_core::Epoch;
use tsoracle_server::Server;
use tsoracle_server::test_fakes::{InMemoryDriver, MockClock};
use tsoracle_server::test_support::{boot_server, wait_for_grpc_handshake, wait_until_serving};

const START_MS: u64 = 1_000_000;

async fn boot_client() -> (
    tsoracle_server::test_support::BootedServer,
    Client,
    Arc<InMemoryDriver>,
) {
    let driver = Arc::new(InMemoryDriver::new());
    let clock = Arc::new(MockClock::new(START_MS));
    let server = Server::builder()
        .consensus_driver(driver.clone())
        .clock(clock)
        .window_ahead(Duration::from_millis(500))
        .failover_advance(Duration::from_millis(200))
        .build()
        .unwrap();
    let mut booted = boot_server(server).await;
    driver.become_leader(Epoch(1));
    wait_until_serving(&mut booted.state_rx).await;
    wait_for_grpc_handshake(booted.addr, Duration::from_secs(5))
        .await
        .unwrap();
    let client = Client::connect(vec![format!("http://{}", booted.addr)])
        .await
        .unwrap();
    (booted, client, driver)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_lease_roundtrip() {
    let (booted, client, _driver) = boot_client().await;

    let lease = client
        .acquire_lease(b"group-a", 1, Duration::from_secs(10))
        .await
        .unwrap();
    assert_eq!(lease.expires_at_ms, START_MS + 10_000);
    assert_eq!(lease.epoch, Epoch(1));

    let frontier = client.get_safe_frontier().await.unwrap();
    assert_eq!(frontier.frontier_physical_ms, lease.ts_upper_bound);
    assert_eq!(frontier.epoch, Epoch(1));

    let renewal = client.renew_lease(lease.lease_id).await.unwrap();
    assert!(renewal.ts_upper_bound > lease.ts_upper_bound);
    assert_eq!(renewal.epoch, Epoch(1));

    client.release_lease(lease.lease_id).await.unwrap();
    client.release_lease(lease.lease_id).await.unwrap();

    booted.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_lease_ttl_rejection_surfaces_invalid_argument() {
    let (booted, client, _driver) = boot_client().await;

    match client
        .acquire_lease(b"group-a", 1, Duration::from_millis(1))
        .await
    {
        Err(ClientError::Rpc(status)) => assert_eq!(status.code(), Code::InvalidArgument),
        other => panic!("expected invalid argument RPC error, got {other:?}"),
    }

    booted.shutdown().await.unwrap();
}
