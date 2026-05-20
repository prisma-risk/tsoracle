use std::{net::SocketAddr, sync::Arc, time::Duration, time::Instant};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::time::sleep;
use tonic::transport::Endpoint;
use tsoracle_core::Epoch;
use tsoracle_proto::v1::{GetTsRequest, tso_service_client::TsoServiceClient};
use tsoracle_server::{Server, ServingState, test_fakes::InMemoryDriver};

/// Block until `state_rx` reports the expected `ServingState`. Replaces
/// `sleep(50ms)` — a real readiness signal rather than a hopeful delay.
async fn wait_until<F>(rx: &mut watch::Receiver<ServingState>, predicate: F)
where
    F: Fn(&ServingState) -> bool,
{
    loop {
        if predicate(&rx.borrow_and_update()) {
            return;
        }
        rx.changed()
            .await
            .expect("state stream closed before reaching expected state");
    }
}

/// Bridge the residual race between "state_rx published the expected state"
/// and "tonic's accept future has been polled and is handling HTTP/2".
/// Probes by opening a real gRPC channel; once one succeeds, the test's own
/// client connection will too.
async fn wait_for_grpc_handshake(
    addr: SocketAddr,
    budget: Duration,
) -> Result<(), tonic::transport::Error> {
    let deadline = Instant::now() + budget;
    let endpoint: Endpoint = format!("http://{addr}").parse().unwrap();
    let mut last_err: Option<tonic::transport::Error> = None;
    loop {
        match endpoint.connect().await {
            Ok(channel) => {
                drop(channel);
                return Ok(());
            }
            Err(err) => {
                if Instant::now() >= deadline {
                    return Err(last_err.unwrap_or(err));
                }
                last_err = Some(err);
                sleep(Duration::from_millis(25)).await;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn end_to_end_get_ts() {
    let driver = Arc::new(InMemoryDriver::new());

    let server = Server::builder()
        .consensus_driver(driver.clone())
        .window_ahead(Duration::from_secs(1))
        .failover_advance(Duration::from_millis(500))
        .build()
        .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();
    let mut state_rx = server.state_rx.clone();

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let serve_handle = tokio::spawn(async move {
        server
            .serve_with_listener(listener, async {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
    });

    driver.become_leader(Epoch(1));
    wait_until(&mut state_rx, |s| matches!(s, ServingState::Serving)).await;
    wait_for_grpc_handshake(local_addr, Duration::from_secs(5))
        .await
        .expect("tonic never accepted gRPC handshake");

    let mut client = TsoServiceClient::connect(format!("http://{local_addr}"))
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn returns_not_leader_with_hint() {
    use tsoracle_server::__priv_decode_leader_hint as decode;
    let driver = Arc::new(InMemoryDriver::new());

    let server = Server::builder()
        .consensus_driver(driver.clone())
        .build()
        .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();
    let mut state_rx = server.state_rx.clone();

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let serve_handle = tokio::spawn(async move {
        server
            .serve_with_listener(listener, async {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
    });

    driver.become_follower(Some("10.9.8.7:50551".into()));
    // Wait for the follower hint to actually be visible in state — distinct
    // from the initial NotServing { leader_endpoint: None }.
    wait_until(&mut state_rx, |s| {
        matches!(
            s,
            ServingState::NotServing {
                leader_endpoint: Some(_)
            }
        )
    })
    .await;
    wait_for_grpc_handshake(local_addr, Duration::from_secs(5))
        .await
        .expect("tonic never accepted gRPC handshake");

    let mut client = TsoServiceClient::connect(format!("http://{local_addr}"))
        .await
        .unwrap();
    let status = client.get_ts(GetTsRequest { count: 1 }).await.unwrap_err();
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    let hint = decode(&status).expect("trailer present");
    assert_eq!(hint.leader_endpoint.as_deref(), Some("10.9.8.7:50551"));

    let _ = shutdown_tx.send(());
    let _ = serve_handle.await;
}
