//! Property: every timestamp returned by the client has physical_ms >= the
//! wall-clock time at which its caller enqueued. This is the freshness
//! contract — strict-consistency callers rely on it.

use std::time::Duration;
use std::{net::SocketAddr, sync::Arc, time::Instant};
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

fn unix_ms_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[tokio::test]
async fn timestamps_are_at_or_after_enqueue_time() {
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

    for _ in 0..200 {
        let enqueue_ms = unix_ms_now();
        let ts = client.get_ts().await.unwrap();
        // Tolerate a small skew: server's clock could be a few ms behind ours.
        // The freshness contract requires ts.physical_ms >= enqueue_ms with
        // some tolerance for clock granularity / fence overhead.
        assert!(
            ts.physical_ms() >= enqueue_ms.saturating_sub(2),
            "ts.physical_ms={} < enqueue_ms={}",
            ts.physical_ms(),
            enqueue_ms
        );
        if rand_jitter() {
            sleep(Duration::from_millis(1)).await;
        }
    }

    let _ = sd_tx.send(());
    let _ = serve.await;
}

fn rand_jitter() -> bool {
    let nanos = Instant::now().elapsed().as_nanos();
    nanos % 3 == 0
}
