use std::{net::SocketAddr, time::Duration};
use tempfile::tempdir;
use tokio::process::Command;
use tokio::time::sleep;
use tsoracle_client::Client;

async fn bind_unused() -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

#[tokio::test]
async fn binary_serves_timestamps() {
    let bin = env!("CARGO_BIN_EXE_tsoracle");
    let dir = tempdir().unwrap();
    let addr = bind_unused().await;

    let mut child = Command::new(bin)
        .arg("serve")
        .arg("--listen")
        .arg(addr.to_string())
        .arg("--state-dir")
        .arg(dir.path())
        .arg("--log")
        .arg("warn")
        .kill_on_drop(true)
        .spawn()
        .unwrap();

    sleep(Duration::from_millis(500)).await;

    let client = Client::connect(vec![addr.to_string()]).await.unwrap();
    let ts1 = client.get_ts().await.unwrap();
    let ts2 = client.get_ts().await.unwrap();
    assert!(ts2 > ts1, "ts2 {ts2:?} > ts1 {ts1:?}");

    child.kill().await.unwrap();
}
