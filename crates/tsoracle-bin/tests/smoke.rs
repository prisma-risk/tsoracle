//
//  ░▀█▀░█▀▀░█▀█░█▀▄░█▀█░█▀▀░█░░░█▀▀
//  ░░█░░▀▀█░█░█░█▀▄░█▀█░█░░░█░░░█▀▀
//  ░░▀░░▀▀▀░▀▀▀░▀░▀░▀░▀░▀▀▀░▀▀▀░▀▀▀
//
//  tsoracle — Distributed Timestamp Oracle
//
//  Copyright (c) 2026 Prisma Risk
//  Licensed under the Apache License, Version 2.0
//  https://github.com/prisma-risk/tsoracle
//

use std::time::Instant;
use std::{io, net::SocketAddr, time::Duration};
use tempfile::tempdir;
use tokio::net::{TcpListener, TcpStream};
use tokio::process::Command;
use tokio::time::{sleep, timeout};
use tsoracle_client::{Client, ClientError};

async fn bind_unused() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

/// Replaces the brittle `sleep(...)` startup wait with a real condition
/// signal: poll TCP connectability until the subprocess's listener accepts.
/// Cross-process readiness can't share an in-memory channel, but a successful
/// `TcpStream::connect` proves the kernel has the listener in LISTEN and the
/// gRPC accept loop is running — which is exactly what `Client::connect`
/// needs to succeed on the next call.
async fn wait_until_accepting(addr: SocketAddr, budget: Duration) -> io::Result<()> {
    timeout(budget, async move {
        loop {
            if TcpStream::connect(addr).await.is_ok() {
                return;
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .map_err(|_| {
        io::Error::new(
            io::ErrorKind::TimedOut,
            format!("server at {addr} did not accept within {budget:?}"),
        )
    })
}

#[tokio::test]
async fn binary_serves_timestamps() {
    let binary_path = env!("CARGO_BIN_EXE_tsoracle");
    let state_dir = tempdir().unwrap();
    let listen_addr = bind_unused().await;

    let mut child = Command::new(binary_path)
        .arg("serve")
        .arg("--listen")
        .arg(listen_addr.to_string())
        .arg("--state-dir")
        .arg(state_dir.path())
        .arg("--log")
        .arg("warn")
        .kill_on_drop(true)
        .spawn()
        .unwrap();

    // Race readiness against the subprocess exiting early — if the binary
    // dies in startup, we want a clear "exited before accepting" failure
    // rather than a 10s "timed out" red herring.
    let readiness = wait_until_accepting(listen_addr, Duration::from_secs(10));
    tokio::pin!(readiness);
    tokio::select! {
        result = &mut readiness => {
            result.expect("binary did not start accepting connections");
        }
        child_result = child.wait() => {
            let status = child_result.expect("wait on child failed");
            panic!("binary exited before accepting connections: status={status}");
        }
    }

    let client = Client::connect(vec![listen_addr.to_string()])
        .await
        .unwrap();

    // TCP-accept readiness above proves the listener is up but not that the
    // binary's FileDriver has finished promoting to leader. A successful
    // get_ts is the end-to-end readiness signal — once one call succeeds,
    // subsequent calls reuse the open channel.
    wait_until_responsive(&client, Duration::from_secs(5))
        .await
        .expect("server never became responsive after starting to accept");

    let ts1 = client.get_ts().await.unwrap();
    let ts2 = client.get_ts().await.unwrap();
    assert!(ts2 > ts1, "ts2 {ts2:?} > ts1 {ts1:?}");

    child.kill().await.unwrap();
}

/// Under Kubernetes / `docker stop` / systemd the supervisor sends SIGTERM,
/// not SIGINT. The server must treat SIGTERM as a graceful-shutdown trigger so
/// tonic drains in-flight requests and the process exits 0 — otherwise the
/// default SIGTERM disposition terminates it by signal and it is SIGKILLed
/// after the grace period (#245).
#[cfg(unix)]
#[tokio::test]
async fn sigterm_triggers_graceful_shutdown() {
    let binary_path = env!("CARGO_BIN_EXE_tsoracle");
    let state_dir = tempdir().unwrap();
    let listen_addr = bind_unused().await;

    let mut child = Command::new(binary_path)
        .arg("serve")
        .arg("--listen")
        .arg(listen_addr.to_string())
        .arg("--state-dir")
        .arg(state_dir.path())
        .arg("--log")
        .arg("warn")
        .kill_on_drop(true)
        .spawn()
        .unwrap();

    // Wait until the SIGTERM handler is live: it is registered when the
    // shutdown future is first polled, which happens once tonic is serving —
    // i.e. by the time the listener is accepting connections.
    let readiness = wait_until_accepting(listen_addr, Duration::from_secs(10));
    tokio::pin!(readiness);
    tokio::select! {
        result = &mut readiness => {
            result.expect("binary did not start accepting connections");
        }
        child_result = child.wait() => {
            let status = child_result.expect("wait on child failed");
            panic!("binary exited before accepting connections: status={status}");
        }
    }

    let pid = child.id().expect("child has a pid before exit");
    let kill = Command::new("kill")
        .arg("-TERM")
        .arg(pid.to_string())
        .status()
        .await
        .expect("spawn kill");
    assert!(kill.success(), "failed to deliver SIGTERM to pid {pid}");

    let status = timeout(Duration::from_secs(10), child.wait())
        .await
        .expect("server did not exit within the grace period after SIGTERM")
        .expect("wait on child failed");

    assert!(
        status.success(),
        "expected graceful exit (status 0) after SIGTERM, got {status}"
    );
}

async fn wait_until_responsive(client: &Client, budget: Duration) -> Result<(), ClientError> {
    let deadline = Instant::now() + budget;
    let mut last_err: Option<ClientError> = None;
    loop {
        match client.get_ts().await {
            Ok(_) => return Ok(()),
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
