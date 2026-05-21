use std::{net::SocketAddr, sync::Arc, time::Duration, time::Instant};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::time::sleep;
use tonic::transport::Endpoint;
use tsoracle_client::{Client, ClientError};
use tsoracle_core::Epoch;
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

/// Bridge the residual race between "state_rx published the expected state"
/// and "tonic's accept future has been polled and is handling HTTP/2".
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
async fn client_gets_timestamps_against_leader() {
    let driver = Arc::new(InMemoryDriver::new());

    let server = Server::builder()
        .consensus_driver(driver.clone())
        .build()
        .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();
    let mut state_rx = server.state_rx.clone();

    let (sd_tx, sd_rx) = tokio::sync::oneshot::channel::<()>();
    let serve = tokio::spawn(async move {
        server
            .serve_with_listener(listener, async {
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

    let client = Client::connect(vec![local_addr.to_string()]).await.unwrap();
    let ts = client.get_ts().await.unwrap();
    assert!(ts.physical_ms() > 1_700_000_000_000);

    let _ = sd_tx.send(());
    let _ = serve.await;
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

    let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_a = listener_a.local_addr().unwrap();
    let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_b = listener_b.local_addr().unwrap();
    let mut state_rx_a = server_a.state_rx.clone();
    let mut state_rx_b = server_b.state_rx.clone();

    let (sda_tx, sda_rx) = tokio::sync::oneshot::channel::<()>();
    let (sdb_tx, sdb_rx) = tokio::sync::oneshot::channel::<()>();
    let server_a_task = tokio::spawn(async move {
        server_a
            .serve_with_listener(listener_a, async {
                let _ = sda_rx.await;
            })
            .await
            .unwrap();
    });
    let server_b_task = tokio::spawn(async move {
        server_b
            .serve_with_listener(listener_b, async {
                let _ = sdb_rx.await;
            })
            .await
            .unwrap();
    });

    driver_a.become_follower(Some(addr_b.to_string()));
    driver_b.become_leader(Epoch(1));
    wait_until(&mut state_rx_a, |s| {
        matches!(
            s,
            ServingState::NotServing {
                leader_endpoint: Some(_)
            }
        )
    })
    .await;
    wait_until(&mut state_rx_b, |s| matches!(s, ServingState::Serving)).await;
    wait_for_grpc_handshake(addr_a, Duration::from_secs(5))
        .await
        .expect("server A never accepted gRPC handshake");
    wait_for_grpc_handshake(addr_b, Duration::from_secs(5))
        .await
        .expect("server B never accepted gRPC handshake");

    // Client only knows about A.
    let client = Client::connect(vec![addr_a.to_string()]).await.unwrap();
    let ts = client
        .get_ts()
        .await
        .expect("must follow hint on this call");
    assert!(ts.physical_ms() > 1_700_000_000_000);

    let _ = sda_tx.send(());
    let _ = sdb_tx.send(());
    let _ = server_a_task.await;
    let _ = server_b_task.await;
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

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();
    let mut state_rx = server.state_rx.clone();

    let (sd_tx, sd_rx) = tokio::sync::oneshot::channel::<()>();
    let serve = tokio::spawn(async move {
        server
            .serve_with_listener(listener, async {
                let _ = sd_rx.await;
            })
            .await
            .unwrap();
    });

    driver.become_follower(None);
    wait_until(&mut state_rx, |s| {
        matches!(
            s,
            ServingState::NotServing {
                leader_endpoint: None
            }
        )
    })
    .await;
    wait_for_grpc_handshake(local_addr, Duration::from_secs(5))
        .await
        .expect("tonic never accepted gRPC handshake");

    let client = Client::connect(vec![local_addr.to_string()]).await.unwrap();
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

    let _ = sd_tx.send(());
    let _ = serve.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_requests_coalesce() {
    let driver = Arc::new(InMemoryDriver::new());

    let server = Server::builder()
        .consensus_driver(driver.clone())
        .build()
        .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();
    let mut state_rx = server.state_rx.clone();

    let (sd_tx, sd_rx) = tokio::sync::oneshot::channel::<()>();
    let serve = tokio::spawn(async move {
        server
            .serve_with_listener(listener, async {
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

    let client = Arc::new(Client::connect(vec![local_addr.to_string()]).await.unwrap());

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

    let _ = sd_tx.send(());
    let _ = serve.await;
}
