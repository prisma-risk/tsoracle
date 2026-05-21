use std::{sync::Arc, time::Duration};
use tsoracle_core::Epoch;
use tsoracle_proto::v1::{GetTsRequest, tso_service_client::TsoServiceClient};
use tsoracle_server::test_fakes::InMemoryDriver;
use tsoracle_server::test_support::{
    boot_server, wait_for_grpc_handshake, wait_until, wait_until_serving,
};
use tsoracle_server::{Server, ServingState};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn end_to_end_get_ts() {
    let driver = Arc::new(InMemoryDriver::new());

    let server = Server::builder()
        .consensus_driver(driver.clone())
        .window_ahead(Duration::from_secs(1))
        .failover_advance(Duration::from_millis(500))
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
    let resp = client
        .get_ts(GetTsRequest { count: 10 })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.count, 10);
    assert_eq!(resp.logical_start, 0);
    assert_eq!(resp.epoch, 1);
    // physical_ms must be at least wall-clock-now (the failover fence advances above it).
    assert!(resp.physical_ms > 1_700_000_000_000);

    booted.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn returns_not_leader_with_hint() {
    use tsoracle_server::__priv_decode_leader_hint as decode;
    let driver = Arc::new(InMemoryDriver::new());

    let server = Server::builder()
        .consensus_driver(driver.clone())
        .build()
        .unwrap();

    let mut booted = boot_server(server).await;

    driver.become_follower(Some("10.9.8.7:50551".into()));
    // Wait for the follower hint to actually be visible in state — distinct
    // from the initial NotServing { leader_endpoint: None }.
    wait_until(&mut booted.state_rx, |s| {
        matches!(
            s,
            ServingState::NotServing {
                leader_endpoint: Some(_)
            }
        )
    })
    .await;
    wait_for_grpc_handshake(booted.addr, Duration::from_secs(5))
        .await
        .expect("tonic never accepted gRPC handshake");

    let mut client = TsoServiceClient::connect(format!("http://{}", booted.addr))
        .await
        .unwrap();
    let status = client.get_ts(GetTsRequest { count: 1 }).await.unwrap_err();
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    let hint = decode(&status).expect("trailer present");
    assert_eq!(hint.leader_endpoint.as_deref(), Some("10.9.8.7:50551"));

    booted.shutdown().await.unwrap();
}
