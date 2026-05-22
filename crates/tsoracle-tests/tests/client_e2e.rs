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

use std::{sync::Arc, time::Duration};
use tsoracle_client::{Client, ClientError};
use tsoracle_core::Epoch;
use tsoracle_server::test_fakes::InMemoryDriver;
use tsoracle_server::test_support::{
    boot_server, wait_for_grpc_handshake, wait_until, wait_until_serving,
};
use tsoracle_server::{Server, ServingState};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_gets_timestamps_against_leader() {
    let driver = Arc::new(InMemoryDriver::new());

    let server = Server::builder()
        .consensus_driver(driver.clone())
        .build()
        .unwrap();

    let mut booted = boot_server(server).await;

    driver.become_leader(Epoch(1));
    wait_until_serving(&mut booted.state_rx).await;
    wait_for_grpc_handshake(booted.addr, Duration::from_secs(5))
        .await
        .expect("tonic never accepted gRPC handshake");

    let client = Client::connect(vec![booted.addr.to_string()])
        .await
        .unwrap();
    let ts = client.get_ts().await.unwrap();
    assert!(ts.physical_ms() > 1_700_000_000_000);

    booted.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_follows_leader_hint_on_first_call() {
    // Two servers: A (follower, hints at B) and B (leader). Client is configured
    // with only A's endpoint. First call hits A, gets NOT_LEADER with hint→B,
    // retries B immediately on the same call, and returns a timestamp. The hint
    // must work within a single get_ts(), not just as a side-effect for the next.
    let driver_a = Arc::new(InMemoryDriver::new());
    let driver_b = Arc::new(InMemoryDriver::new());

    let server_a = Server::builder()
        .consensus_driver(driver_a.clone())
        .build()
        .unwrap();
    let server_b = Server::builder()
        .consensus_driver(driver_b.clone())
        .build()
        .unwrap();

    let mut booted_a = boot_server(server_a).await;
    let mut booted_b = boot_server(server_b).await;

    driver_a.become_follower(Some(booted_b.addr.to_string()));
    driver_b.become_leader(Epoch(1));
    wait_until(&mut booted_a.state_rx, |s| {
        matches!(
            s,
            ServingState::NotServing {
                leader_endpoint: Some(_)
            }
        )
    })
    .await;
    wait_until_serving(&mut booted_b.state_rx).await;
    wait_for_grpc_handshake(booted_a.addr, Duration::from_secs(5))
        .await
        .expect("server A never accepted gRPC handshake");
    wait_for_grpc_handshake(booted_b.addr, Duration::from_secs(5))
        .await
        .expect("server B never accepted gRPC handshake");

    // Client only knows about A.
    let client = Client::connect(vec![booted_a.addr.to_string()])
        .await
        .unwrap();
    let ts = client
        .get_ts()
        .await
        .expect("must follow hint on this call");
    assert!(ts.physical_ms() > 1_700_000_000_000);

    booted_a.shutdown().await.unwrap();
    booted_b.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_surfaces_error_when_only_endpoint_is_a_hintless_follower() {
    // A follower with no known leader replies FailedPrecondition with an empty
    // LeaderHint. The retry loop must clear its cached leader (the cache is
    // now stale), exhaust the worklist, and surface the RPC error — not loop
    // on the same dead endpoint or swallow the status.
    let driver = Arc::new(InMemoryDriver::new());

    let server = Server::builder()
        .consensus_driver(driver.clone())
        .build()
        .unwrap();

    let mut booted = boot_server(server).await;

    driver.become_follower(None);
    wait_until(&mut booted.state_rx, |s| {
        matches!(
            s,
            ServingState::NotServing {
                leader_endpoint: None
            }
        )
    })
    .await;
    wait_for_grpc_handshake(booted.addr, Duration::from_secs(5))
        .await
        .expect("tonic never accepted gRPC handshake");

    let client = Client::connect(vec![booted.addr.to_string()])
        .await
        .unwrap();
    let err = client
        .get_ts()
        .await
        .expect_err("hintless follower must surface NOT_LEADER");
    match err {
        ClientError::Rpc(status) => {
            assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        }
        other => panic!("expected ClientError::Rpc(FailedPrecondition), got {other:?}"),
    }

    booted.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_requests_coalesce() {
    let driver = Arc::new(InMemoryDriver::new());

    let server = Server::builder()
        .consensus_driver(driver.clone())
        .build()
        .unwrap();

    let mut booted = boot_server(server).await;

    driver.become_leader(Epoch(1));
    wait_until_serving(&mut booted.state_rx).await;
    wait_for_grpc_handshake(booted.addr, Duration::from_secs(5))
        .await
        .expect("tonic never accepted gRPC handshake");

    let client = Arc::new(
        Client::connect(vec![booted.addr.to_string()])
            .await
            .unwrap(),
    );

    // Fire 32 concurrent get_ts; with a 1ms flush, many should ride the same RPC.
    let mut handles = Vec::new();
    for _ in 0..32 {
        let client = client.clone();
        handles.push(tokio::spawn(async move { client.get_ts().await.unwrap() }));
    }
    let timestamps: Vec<_> = futures::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    let mut sorted = timestamps.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(sorted.len(), 32, "all 32 timestamps must be unique");

    booted.shutdown().await.unwrap();
}
