use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::time::sleep;
use tonic::transport::Server as TonicServer;
use tsoracle_core::Epoch;
use tsoracle_proto::v1::{GetTsRequest, tso_service_client::TsoServiceClient};
use tsoracle_server::{Server, test_fakes::InMemoryDriver};

async fn bind_unused() -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

#[tokio::test]
async fn embedded_router_serves_via_caller_owned_listener() {
    let addr = bind_unused().await;
    let driver = Arc::new(InMemoryDriver::new());

    let tsoracle = Server::builder()
        .consensus_driver(driver.clone())
        .build()
        .unwrap();

    let (router, _leader_watch) = tsoracle.into_router();

    let (sd_tx, sd_rx) = tokio::sync::oneshot::channel::<()>();
    let serve = tokio::spawn(async move {
        TonicServer::builder()
            .add_routes(router)
            .serve_with_shutdown(addr, async {
                let _ = sd_rx.await;
            })
            .await
            .unwrap();
    });

    sleep(Duration::from_millis(50)).await;
    driver.become_leader(Epoch(1));
    sleep(Duration::from_millis(50)).await;

    let mut client = TsoServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    let resp = client
        .get_ts(GetTsRequest { count: 1 })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.count, 1);
    assert_eq!(resp.epoch, 1);

    let _ = sd_tx.send(());
    let _ = serve.await;
}
