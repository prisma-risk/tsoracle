//
//  ░▀█▀░█▀▀░█▀█░█▀▄░█▀█░█▀▀░█░░░█▀▀
//  ░░█░░▀▀█░█░█░█▀▄░█▀█░█░░░█░░░█▀▀
//  ░░▀░░▀▀▀░▀▀▀░▀░▀░▀░▀░▀▀▀░▀▀▀░▀▀▀
//
//  tsoracle — Distributed Timestamp Oracle
//  https://www.tsoracle.rs
//
//  Copyright (c) 2026 Prisma Risk
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

//! Regression test for the multi-node example honouring SIGTERM. Under
//! Kubernetes / `docker stop` / systemd the supervisor sends SIGTERM, not
//! SIGINT, so the cluster binary must treat SIGTERM as a graceful-shutdown
//! trigger — otherwise the default disposition kills it mid-flight and it is
//! SIGKILLed after the grace period. The stock `tsoracle serve` binary has
//! covered this since #245; this pins the same contract on the standalone
//! example a cluster actually runs.

#![cfg(unix)]

use std::net::SocketAddr;
use std::time::Duration;

use tempfile::tempdir;
use tokio::net::{TcpListener, TcpStream};
use tokio::process::Command;
use tokio::time::{sleep, timeout};

/// Bind an ephemeral port, learn the address, then release it so the child can
/// claim it. Same TOCTOU-tolerant trick the stock binary's smoke test uses.
async fn lease_port() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

/// Poll TCP connectability until the child's tso listener accepts — proof that
/// the gRPC accept loop (and therefore the shutdown handler it installs) is
/// live.
async fn wait_until_accepting(addr: SocketAddr, budget: Duration) {
    timeout(budget, async move {
        loop {
            if TcpStream::connect(addr).await.is_ok() {
                return;
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("tso server at {addr} did not accept within {budget:?}"));
}

#[tokio::test]
async fn sigterm_triggers_graceful_shutdown() {
    let binary = env!("CARGO_BIN_EXE_openraft-standalone");
    let raft_dir = tempdir().unwrap();
    let raft_addr = lease_port().await;
    let tso_addr = lease_port().await;

    // Single-node cluster: enough for the tso gRPC server to bind and accept,
    // which is all the SIGTERM path needs.
    let mut child = Command::new(binary)
        .arg("--id")
        .arg("1")
        .arg("--raft-addr")
        .arg(raft_addr.to_string())
        .arg("--tso-addr")
        .arg(tso_addr.to_string())
        .arg("--members")
        .arg(format!("1={raft_addr}/{tso_addr}"))
        .arg("--raft-dir")
        .arg(raft_dir.path())
        .arg("--bootstrap")
        .kill_on_drop(true)
        .spawn()
        .unwrap();

    // Race readiness against an early exit so a startup crash fails loudly
    // rather than as a readiness timeout.
    let readiness = wait_until_accepting(tso_addr, Duration::from_secs(15));
    tokio::pin!(readiness);
    tokio::select! {
        () = &mut readiness => {}
        child_result = child.wait() => {
            let status = child_result.expect("wait on child failed");
            panic!("example exited before accepting connections: status={status}");
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
        .expect("example did not exit within the grace period after SIGTERM")
        .expect("wait on child failed");

    assert!(
        status.success(),
        "expected graceful exit (status 0) after SIGTERM, got {status}"
    );
}
