use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::time::sleep;
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
async fn end_to_end_get_ts() {
    let addr = bind_unused().await;
    let driver = Arc::new(InMemoryDriver::new());

    let server = Server::builder()
        .consensus_driver(driver.clone())
        .window_ahead(Duration::from_secs(1))
        .failover_advance(Duration::from_millis(500))
        .build()
        .unwrap();

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let serve_handle = tokio::spawn(async move {
        server
            .serve_with_shutdown(addr, async {
                let _ = shutdown_rx.await;
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
        .get_ts(GetTsRequest { count: 10 })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.count, 10);
    assert_eq!(resp.logical_start, 0);
    assert_eq!(resp.epoch, 1);
    // physical_ms must be at least wall-clock-now (the failover fence advances above it).
    assert!(resp.physical_ms > 1_700_000_000_000);

    let _ = shutdown_tx.send(());
    let _ = serve_handle.await;
}

#[tokio::test]
async fn returns_not_leader_with_hint() {
    use tsoracle_server::__priv_decode_leader_hint as decode;
    let addr = bind_unused().await;
    let driver = Arc::new(InMemoryDriver::new());

    let server = Server::builder()
        .consensus_driver(driver.clone())
        .build()
        .unwrap();

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let serve_handle = tokio::spawn(async move {
        server
            .serve_with_shutdown(addr, async {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
    });

    sleep(Duration::from_millis(50)).await;
    driver.become_follower(Some("10.9.8.7:50551".into()));
    sleep(Duration::from_millis(50)).await;

    let mut client = TsoServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    let status = client.get_ts(GetTsRequest { count: 1 }).await.unwrap_err();
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    let hint = decode(&status).expect("trailer present");
    assert_eq!(hint.leader_endpoint.as_deref(), Some("10.9.8.7:50551"));

    let _ = shutdown_tx.send(());
    let _ = serve_handle.await;
}
