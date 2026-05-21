use std::{sync::Arc, time::Duration};
use tsoracle_core::Epoch;
use tsoracle_proto::v1::{GetTsRequest, tso_service_client::TsoServiceClient};
use tsoracle_server::Server;
use tsoracle_server::test_fakes::InMemoryDriver;
use tsoracle_server::test_support::{boot_router, wait_for_grpc_handshake, wait_until_serving};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn embedded_router_serves_via_caller_owned_listener() {
    let driver = Arc::new(InMemoryDriver::new());

    let tsoracle = Server::builder()
        .consensus_driver(driver.clone())
        .build()
        .unwrap();

    // Clone state_rx before into_router consumes the Server.
    let mut state_rx = tsoracle.state_rx.clone();
    let (router, _leader_watch) = tsoracle.into_router();

    let booted = boot_router(router).await;

    driver.become_leader(Epoch(1));
    wait_until_serving(&mut state_rx).await;
    wait_for_grpc_handshake(booted.addr, Duration::from_secs(5))
        .await
        .expect("tonic never accepted gRPC handshake");

    let mut client = TsoServiceClient::connect(format!("http://{}", booted.addr))
        .await
        .unwrap();
    let resp = client
        .get_ts(GetTsRequest { count: 1 })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.count, 1);
    assert_eq!(resp.epoch, 1);

    booted.shutdown().await.unwrap();
}
