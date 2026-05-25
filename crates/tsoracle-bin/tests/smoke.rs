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

use std::path::PathBuf;
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
        .arg("file")
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
        .arg("file")
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

/// A single-node openraft cluster bootstraps, elects itself leader, and serves
/// timestamps through the shipped `serve openraft` path.
#[cfg(feature = "openraft")]
#[tokio::test]
async fn serve_openraft_single_node_serves_after_bootstrap() {
    let binary_path = env!("CARGO_BIN_EXE_tsoracle");
    let raft_dir = tempdir().unwrap();
    let listen_addr = bind_unused().await;
    let raft_addr = bind_unused().await;
    let admin_addr = bind_unused().await;

    let mut child = Command::new(binary_path)
        .arg("serve")
        .arg("openraft")
        .arg("--id")
        .arg("1")
        .arg("--listen")
        .arg(listen_addr.to_string())
        .arg("--raft-addr")
        .arg(raft_addr.to_string())
        .arg("--raft-dir")
        .arg(raft_dir.path())
        .arg("--bootstrap")
        .arg("--members")
        .arg(format!("1={raft_addr}/{listen_addr}/{admin_addr}"))
        .arg("--log")
        .arg("warn")
        .kill_on_drop(true)
        .spawn()
        .unwrap();

    let readiness = wait_until_accepting(listen_addr, Duration::from_secs(15));
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
    // A single-node raft still needs one election timeout to promote to leader
    // before get_ts can succeed; give it a generous budget.
    wait_until_responsive(&client, Duration::from_secs(15))
        .await
        .expect("openraft node never became responsive after starting to accept");

    let ts1 = client.get_ts().await.unwrap();
    let ts2 = client.get_ts().await.unwrap();
    assert!(ts2 > ts1, "ts2 {ts2:?} > ts1 {ts1:?}");

    child.kill().await.unwrap();
}

struct ServerCerts {
    cert: PathBuf,
    key: PathBuf,
    ca_path: PathBuf,
    ca_pem: String,
}

/// Mint a self-signed CA and a server leaf cert (SANs: `localhost` + `127.0.0.1`),
/// write them into `dir`, and return the paths + the CA PEM string.
fn write_server_certs(dir: &std::path::Path) -> ServerCerts {
    use rcgen::{
        BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair,
        KeyUsagePurpose,
    };

    let ca_key = KeyPair::generate().expect("ca keypair");
    let mut ca_params =
        CertificateParams::new(vec!["tsoracle-smoke-ca".to_string()]).expect("ca params");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let ca_cert = ca_params.self_signed(&ca_key).expect("ca self-sign");

    let server_key = KeyPair::generate().expect("server keypair");
    let mut server_params =
        CertificateParams::new(vec!["localhost".to_string(), "127.0.0.1".to_string()])
            .expect("server params");
    server_params
        .distinguished_name
        .push(DnType::CommonName, "localhost");
    server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let server_cert = server_params
        .signed_by(&server_key, &ca_cert, &ca_key)
        .expect("server sign");

    let cert_path = dir.join("cert.pem");
    let key_path = dir.join("key.pem");
    let ca_path = dir.join("ca.pem");
    let ca_pem = ca_cert.pem();
    std::fs::write(&cert_path, server_cert.pem()).expect("write cert.pem");
    std::fs::write(&key_path, server_key.serialize_pem()).expect("write key.pem");
    std::fs::write(&ca_path, &ca_pem).expect("write ca.pem");

    ServerCerts {
        cert: cert_path,
        key: key_path,
        ca_path,
        ca_pem,
    }
}

/// The file driver starts up, serves its client gRPC API over TLS (server-auth),
/// and a TLS-configured client can issue `get_ts` successfully.
#[tokio::test]
async fn serve_file_with_client_tls_serves_over_tls() {
    let binary_path = env!("CARGO_BIN_EXE_tsoracle");
    let certdir = tempdir().unwrap();
    let certs = write_server_certs(certdir.path());
    let state_dir = tempdir().unwrap();
    let listen_addr = bind_unused().await;

    let mut child = Command::new(binary_path)
        .arg("serve")
        .arg("file")
        .arg("--listen")
        .arg(listen_addr.to_string())
        .arg("--state-dir")
        .arg(state_dir.path())
        .arg("--tls-cert")
        .arg(&certs.cert)
        .arg("--tls-key")
        .arg(&certs.key)
        .arg("--log")
        .arg("warn")
        .kill_on_drop(true)
        .spawn()
        .unwrap();

    let readiness = wait_until_accepting(listen_addr, Duration::from_secs(10));
    tokio::pin!(readiness);
    tokio::select! {
        r = &mut readiness => r.expect("binary did not start accepting connections"),
        c = child.wait() => panic!("binary exited before accepting connections: {c:?}"),
    }

    let tls = tonic::transport::ClientTlsConfig::new()
        .ca_certificate(tonic::transport::Certificate::from_pem(&certs.ca_pem))
        .domain_name("localhost");
    let client = tsoracle_client::ClientBuilder::endpoints(vec![format!(
        "localhost:{}",
        listen_addr.port()
    )])
    .tls_config(tls)
    .build()
    .await
    .expect("client build");

    wait_until_responsive(&client, Duration::from_secs(5))
        .await
        .expect("server never became responsive after TLS handshake");
    assert!(client.get_ts().await.is_ok());
    child.kill().await.unwrap();
}

/// A single-node openraft cluster with peer mTLS configured on the raft
/// transport binds and serves timestamps normally (a single node never dials
/// a peer, so this proves the peer-TLS server binds without error and the
/// node still elects itself and serves the client API).
#[cfg(feature = "openraft")]
#[tokio::test]
async fn serve_openraft_with_peer_mtls_boots_and_serves() {
    let binary_path = env!("CARGO_BIN_EXE_tsoracle");
    let certdir = tempdir().unwrap();
    let certs = write_server_certs(certdir.path());
    let raft_dir = tempdir().unwrap();
    let listen_addr = bind_unused().await;
    let raft_addr = bind_unused().await;
    let admin_addr = bind_unused().await;

    let mut child = Command::new(binary_path)
        .arg("serve")
        .arg("openraft")
        .arg("--id")
        .arg("1")
        .arg("--listen")
        .arg(listen_addr.to_string())
        .arg("--raft-addr")
        .arg(raft_addr.to_string())
        .arg("--raft-dir")
        .arg(raft_dir.path())
        .arg("--bootstrap")
        .arg("--members")
        .arg(format!("1={raft_addr}/{listen_addr}/{admin_addr}"))
        .arg("--peer-tls-cert")
        .arg(&certs.cert)
        .arg("--peer-tls-key")
        .arg(&certs.key)
        .arg("--peer-tls-ca")
        .arg(&certs.ca_path)
        .arg("--log")
        .arg("warn")
        .kill_on_drop(true)
        .spawn()
        .unwrap();

    let readiness = wait_until_accepting(listen_addr, Duration::from_secs(15));
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
    wait_until_responsive(&client, Duration::from_secs(15))
        .await
        .expect("openraft node with peer mTLS never became responsive");

    let ts1 = client.get_ts().await.unwrap();
    let ts2 = client.get_ts().await.unwrap();
    assert!(ts2 > ts1, "ts2 {ts2:?} > ts1 {ts1:?}");

    child.kill().await.unwrap();
}

/// A build without the paxos feature must reject `serve paxos` with the
/// friendly "not included in this build" message, not a clap parse error.
#[tokio::test]
async fn serve_paxos_errors_when_feature_compiled_out() {
    // CARGO_BIN_EXE_tsoracle points at the test build of the binary. The
    // workspace default includes paxos, so this test only asserts the message
    // shape when the feature is OFF; gate it accordingly.
    #[cfg(not(feature = "paxos"))]
    {
        let exe = env!("CARGO_BIN_EXE_tsoracle");
        let output = tokio::process::Command::new(exe)
            .args([
                "serve",
                "paxos",
                "--node-id",
                "1",
                "--peer-listen",
                "127.0.0.1:0",
                "--peers",
                "1=127.0.0.1:1",
                "--tso-peers",
                "1=127.0.0.1:2",
                "--data-dir",
                "/tmp/x",
            ])
            .output()
            .await
            .unwrap();
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("does not include the paxos driver"),
            "stderr: {stderr}"
        );
    }
}
