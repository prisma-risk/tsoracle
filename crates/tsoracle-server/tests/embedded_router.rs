use std::{net::SocketAddr, sync::Arc, time::Duration, time::Instant};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::time::sleep;
use tonic::transport::{Endpoint, Server as TonicServer, server::TcpIncoming};
use tsoracle_core::Epoch;
use tsoracle_proto::v1::{GetTsRequest, tso_service_client::TsoServiceClient};
use tsoracle_server::{Server, ServingState, test_fakes::InMemoryDriver};

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
async fn embedded_router_serves_via_caller_owned_listener() {
    let driver = Arc::new(InMemoryDriver::new());

    let tsoracle = Server::builder()
        .consensus_driver(driver.clone())
        .build()
        .unwrap();

    // Clone state_rx before into_router consumes the Server.
    let mut state_rx = tsoracle.state_rx.clone();
    let (router, _leader_watch) = tsoracle.into_router();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();
    let incoming = TcpIncoming::from(listener);

    let (sd_tx, sd_rx) = tokio::sync::oneshot::channel::<()>();
    let serve = tokio::spawn(async move {
        TonicServer::builder()
            .add_routes(router)
            .serve_with_incoming_shutdown(incoming, async {
                let _ = sd_rx.await;
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
        .get_ts(GetTsRequest { count: 1 })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.count, 1);
    assert_eq!(resp.epoch, 1);

    let _ = sd_tx.send(());
    let _ = serve.await;
}
