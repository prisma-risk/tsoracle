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
use tsoracle_core::{Epoch, PeerEndpoint};
use tsoracle_server::test_fakes::{InMemoryDriver, MockClock};
use tsoracle_server::test_support::{
    boot_leader_server, boot_server, wait_for_grpc_handshake, wait_until, wait_until_serving,
};
use tsoracle_server::{Server, ServingState};

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
    let (booted, _proto_client) =
        boot_leader_server(server, || driver.become_leader(Epoch(1))).await;
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

/// A lease client configured with one load-balanced address keeps its channel on whichever member it first reached. When that member follows, its NOT_LEADER hint must steer the next call to the leader; otherwise every retry lands on the follower again and the lease is never acquired.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lease_not_leader_hint_steers_the_next_call_to_the_leader() {
    let follower_driver = Arc::new(InMemoryDriver::new());
    let leader_driver = Arc::new(InMemoryDriver::new());
    let build = |driver: Arc<InMemoryDriver>| {
        Server::builder()
            .consensus_driver(driver)
            .clock(Arc::new(MockClock::new(START_MS)))
            .window_ahead(Duration::from_millis(500))
            .failover_advance(Duration::from_millis(200))
            .build()
            .unwrap()
    };
    let mut follower = boot_server(build(follower_driver.clone())).await;
    let mut leader = boot_server(build(leader_driver.clone())).await;
    leader_driver.become_leader(Epoch(1));
    follower_driver.become_follower(Some(
        PeerEndpoint::try_from(leader.addr.to_string()).unwrap(),
    ));
    wait_until_serving(&mut leader.state_rx).await;
    wait_until(&mut follower.state_rx, |state| {
        matches!(
            state,
            ServingState::NotServing {
                leader_endpoint: Some(_),
                ..
            }
        )
    })
    .await;
    for addr in [follower.addr, leader.addr] {
        wait_for_grpc_handshake(addr, Duration::from_secs(5))
            .await
            .expect("server handshake");
    }

    let client = Client::connect(vec![format!("http://{}", follower.addr)])
        .await
        .unwrap();
    match client
        .acquire_lease(b"group-a", 1, Duration::from_secs(10))
        .await
    {
        Err(ClientError::Rpc(status)) => assert_eq!(status.code(), Code::FailedPrecondition),
        other => panic!("the follower must refuse the acquire, got {other:?}"),
    }
    let cached = client
        .cached_leader()
        .expect("the refusal's leader hint is seated");
    assert!(
        cached.ends_with(&leader.addr.to_string()),
        "cached leader {cached} names the hinted leader"
    );

    let lease = client
        .acquire_lease(b"group-a", 1, Duration::from_secs(10))
        .await
        .expect("the retry reaches the leader");
    assert_eq!(lease.epoch, Epoch(1));
    assert_eq!(leader_driver.current_leases().len(), 1);
    assert!(follower_driver.current_leases().is_empty());

    follower.shutdown().await.unwrap();
    leader.shutdown().await.unwrap();
}

/// A lease precondition refusal on the leader shares NOT_LEADER's status code but carries no hint, so it must not disturb the leader cache.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lease_precondition_refusal_leaves_the_leader_cache_alone() {
    let (booted, client, _driver) = boot_client().await;
    client
        .acquire_lease(b"group-a", 2, Duration::from_secs(10))
        .await
        .unwrap();
    let before = client.cached_leader();
    match client
        .acquire_lease(b"group-a", 1, Duration::from_secs(10))
        .await
    {
        Err(ClientError::Rpc(status)) => assert_eq!(status.code(), Code::FailedPrecondition),
        other => panic!("a stale holder epoch must be refused, got {other:?}"),
    }
    assert_eq!(client.cached_leader(), before);

    booted.shutdown().await.unwrap();
}
