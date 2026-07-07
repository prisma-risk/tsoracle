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

use tsoracle_core::Epoch;
use tsoracle_proto::v1::{
    AcquireLeaseRequest, GetSafeFrontierRequest, ReleaseLeaseRequest,
    tso_service_client::TsoServiceClient,
};
use tsoracle_server::Server;
use tsoracle_server::test_fakes::{InMemoryDriver, MockClock};
use tsoracle_server::test_support::{boot_server, wait_for_grpc_handshake, wait_until_serving};

const START_MS: u64 = 1_000_000;

async fn boot_leader(
    driver: Arc<InMemoryDriver>,
    clock: Arc<MockClock>,
) -> (
    tsoracle_server::test_support::BootedServer,
    TsoServiceClient<tonic::transport::Channel>,
) {
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
    let client = TsoServiceClient::connect(format!("http://{}", booted.addr))
        .await
        .unwrap();
    (booted, client)
}

fn acquire_req(holder: &[u8], holder_epoch: u64, ttl_ms: u64) -> AcquireLeaseRequest {
    AcquireLeaseRequest {
        holder: holder.to_vec(),
        holder_epoch,
        ttl_ms,
    }
}

async fn frontier(client: &mut TsoServiceClient<tonic::transport::Channel>) -> u64 {
    client
        .get_safe_frontier(GetSafeFrontierRequest {})
        .await
        .unwrap()
        .into_inner()
        .frontier_physical_ms
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn follower_returns_zero_frontier_and_zero_epoch() {
    let driver = Arc::new(InMemoryDriver::new());
    let server = Server::builder()
        .consensus_driver(driver)
        .window_ahead(Duration::from_millis(500))
        .failover_advance(Duration::from_millis(200))
        .build()
        .unwrap();
    let booted = boot_server(server).await;
    wait_for_grpc_handshake(booted.addr, Duration::from_secs(5))
        .await
        .unwrap();
    let mut client = TsoServiceClient::connect(format!("http://{}", booted.addr))
        .await
        .unwrap();

    let resp = client
        .get_safe_frontier(GetSafeFrontierRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.frontier_physical_ms, 0);
    assert_eq!((resp.epoch_hi, resp.epoch_lo), (0, 0));

    booted.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leader_without_leases_reports_high_water() {
    let driver = Arc::new(InMemoryDriver::new());
    let clock = Arc::new(MockClock::new(START_MS));
    let (booted, mut client) = boot_leader(driver.clone(), clock).await;

    let resp = client
        .get_safe_frontier(GetSafeFrontierRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.frontier_physical_ms, driver.current_high_water());
    assert_eq!((resp.epoch_hi, resp.epoch_lo), (0, 1));

    booted.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dead_lease_stalls_frontier_until_expiry_then_advances() {
    let driver = Arc::new(InMemoryDriver::new());
    let clock = Arc::new(MockClock::new(START_MS));
    let (booted, mut client) = boot_leader(driver, clock.clone()).await;

    let lease_a = client
        .acquire_lease(acquire_req(b"group-a", 1, 5_000))
        .await
        .unwrap()
        .into_inner();
    let lease_b = client
        .acquire_lease(acquire_req(b"group-b", 1, 60_000))
        .await
        .unwrap()
        .into_inner();
    assert!(lease_b.ts_upper_bound > lease_a.ts_upper_bound);
    assert_eq!(frontier(&mut client).await, lease_a.ts_upper_bound);

    clock.advance(4_999);
    assert_eq!(frontier(&mut client).await, lease_a.ts_upper_bound);
    clock.advance(1);
    assert_eq!(frontier(&mut client).await, lease_b.ts_upper_bound);

    booted.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn superseded_lease_binds_frontier_until_expiry() {
    let driver = Arc::new(InMemoryDriver::new());
    let clock = Arc::new(MockClock::new(START_MS));
    let (booted, mut client) = boot_leader(driver, clock.clone()).await;

    let first = client
        .acquire_lease(acquire_req(b"group-a", 1, 5_000))
        .await
        .unwrap()
        .into_inner();
    let second = client
        .acquire_lease(acquire_req(b"group-a", 2, 60_000))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(frontier(&mut client).await, first.ts_upper_bound);
    clock.advance(5_000);
    assert_eq!(frontier(&mut client).await, second.ts_upper_bound);

    booted.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn released_lease_stops_binding_immediately() {
    let driver = Arc::new(InMemoryDriver::new());
    let clock = Arc::new(MockClock::new(START_MS));
    let (booted, mut client) = boot_leader(driver, clock).await;

    let first = client
        .acquire_lease(acquire_req(b"group-a", 1, 60_000))
        .await
        .unwrap()
        .into_inner();
    let second = client
        .acquire_lease(acquire_req(b"group-b", 1, 60_000))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(frontier(&mut client).await, first.ts_upper_bound);
    client
        .release_lease(ReleaseLeaseRequest {
            lease_id: first.lease_id,
        })
        .await
        .unwrap();
    assert_eq!(frontier(&mut client).await, second.ts_upper_bound);

    booted.shutdown().await.unwrap();
}
