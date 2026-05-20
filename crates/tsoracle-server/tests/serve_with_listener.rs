//! Verifies `Server::serve_with_listener` binds a caller-owned `TcpListener`,
//! so callers using `127.0.0.1:0` can capture the OS-picked port before
//! clients connect.

use std::sync::Arc;
use tokio::net::TcpListener;
use tsoracle_core::Epoch;
use tsoracle_server::{Server, ServingState, test_fakes::InMemoryDriver};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serve_with_listener_uses_caller_owned_socket() {
    let driver = Arc::new(InMemoryDriver::new());
    driver.become_leader(Epoch(1));

    let server = Server::builder()
        .consensus_driver(driver.clone())
        .build()
        .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();
    assert_ne!(local_addr.port(), 0, "OS must have picked a real port");

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    // Clone state_rx before moving server into the spawn.
    let mut state_rx = server.state_rx.clone();
    let server_handle = tokio::spawn(async move {
        server
            .serve_with_listener(listener, async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    // Wait for the server to reach Serving rather than sleeping a fixed delay.
    loop {
        if matches!(*state_rx.borrow_and_update(), ServingState::Serving) {
            break;
        }
        state_rx
            .changed()
            .await
            .expect("state stream closed before Serving");
    }

    // Make a real client call against the captured port.
    let endpoint = format!("http://{local_addr}");
    let client = tsoracle_client::Client::connect(vec![endpoint])
        .await
        .expect("client connect");
    let ts = client.get_ts().await.expect("get_ts");
    // Verify the server returned a real (non-zero) timestamp.
    assert!(
        ts.physical_ms() > 1_700_000_000_000,
        "expected a real wall-clock timestamp, got physical_ms={}",
        ts.physical_ms()
    );

    drop(client);
    let _ = shutdown_tx.send(());
    let result = server_handle.await.expect("join");
    assert!(result.is_ok(), "server exited Err: {result:?}");
}
