use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::time::sleep;
use tsoracle_client::Client;
use tsoracle_core::Epoch;
use tsoracle_server::{Server, test_fakes::InMemoryDriver};

async fn bind_unused() -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

#[tokio::test]
async fn client_gets_timestamps_against_leader() {
    let addr = bind_unused().await;
    let driver = Arc::new(InMemoryDriver::new());

    let server = Server::builder()
        .consensus_driver(driver.clone())
        .build()
        .unwrap();

    let (sd_tx, sd_rx) = tokio::sync::oneshot::channel::<()>();
    let serve = tokio::spawn(async move {
        server
            .serve_with_shutdown(addr, async {
                let _ = sd_rx.await;
            })
            .await
            .unwrap();
    });

    sleep(Duration::from_millis(50)).await;
    driver.become_leader(Epoch(1));
    sleep(Duration::from_millis(50)).await;

    let client = Client::connect(vec![addr.to_string()]).await.unwrap();
    let ts = client.get_ts().await.unwrap();
    assert!(ts.physical_ms() > 1_700_000_000_000);

    let _ = sd_tx.send(());
    let _ = serve.await;
}

#[tokio::test]
async fn client_follows_leader_hint_on_first_call() {
    // Two servers: A (follower, hints at B) and B (leader). Client is configured
    // with only A's endpoint. First call hits A, gets NOT_LEADER with hint→B,
    // retries B immediately on the same call, and returns a timestamp. The hint
    // must work within a single get_ts(), not just as a side-effect for the next.
    let addr_a = bind_unused().await;
    let addr_b = bind_unused().await;
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
    let (sda_tx, sda_rx) = tokio::sync::oneshot::channel::<()>();
    let (sdb_tx, sdb_rx) = tokio::sync::oneshot::channel::<()>();
    let server_a_task = tokio::spawn(async move {
        server_a
            .serve_with_shutdown(addr_a, async {
                let _ = sda_rx.await;
            })
            .await
            .unwrap();
    });
    let server_b_task = tokio::spawn(async move {
        server_b
            .serve_with_shutdown(addr_b, async {
                let _ = sdb_rx.await;
            })
            .await
            .unwrap();
    });

    sleep(Duration::from_millis(50)).await;
    driver_a.become_follower(Some(addr_b.to_string()));
    driver_b.become_leader(Epoch(1));
    sleep(Duration::from_millis(50)).await;

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

#[tokio::test]
async fn concurrent_requests_coalesce() {
    let addr = bind_unused().await;
    let driver = Arc::new(InMemoryDriver::new());

    let server = Server::builder()
        .consensus_driver(driver.clone())
        .build()
        .unwrap();
    let (sd_tx, sd_rx) = tokio::sync::oneshot::channel::<()>();
    let serve = tokio::spawn(async move {
        server
            .serve_with_shutdown(addr, async {
                let _ = sd_rx.await;
            })
            .await
            .unwrap();
    });

    sleep(Duration::from_millis(50)).await;
    driver.become_leader(Epoch(1));
    sleep(Duration::from_millis(50)).await;

    let client = Arc::new(Client::connect(vec![addr.to_string()]).await.unwrap());

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
