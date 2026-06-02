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
use std::process::{ExitStatus, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use std::{io, net::SocketAddr, time::Duration};
use tempfile::tempdir;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::process::{Child, Command};
use tokio::time::{sleep, timeout};
use tsoracle_client::{Client, ClientError};

/// Allocate `n` unused 127.0.0.1 ports by binding `n` listeners at once and
/// dropping them together, which keeps the probe window per-port as short as
/// possible. The kernel can still hand any one of these ports to another
/// process between `drop` and the subprocess's `.bind()` — that residual
/// race is handled by [`retry_spawn`].
async fn bind_unused_set(n: usize) -> Vec<SocketAddr> {
    let mut listeners = Vec::with_capacity(n);
    for _ in 0..n {
        listeners.push(TcpListener::bind("127.0.0.1:0").await.unwrap());
    }
    let addrs: Vec<SocketAddr> = listeners.iter().map(|l| l.local_addr().unwrap()).collect();
    drop(listeners);
    addrs
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

enum AwaitOutcome {
    Ready,
    ChildExited(ExitStatus),
    Timeout(SocketAddr),
}

#[cfg(feature = "metrics")]
fn disable_metrics_exporter_for_test(cmd: &mut Command) {
    cmd.arg("--no-metrics");
}

#[cfg(not(feature = "metrics"))]
fn disable_metrics_exporter_for_test(_cmd: &mut Command) {}

/// Wait for each address in `ready_idx` to start accepting connections,
/// racing every wait against the child exiting early. Returns
/// `Ready` only after every selected addr has accepted.
async fn await_listening(
    child: &mut Child,
    ready_idx: &[usize],
    addrs: &[SocketAddr],
    per_addr_budget: Duration,
) -> AwaitOutcome {
    for &i in ready_idx {
        let addr = addrs[i];
        let waiter = wait_until_accepting(addr, per_addr_budget);
        tokio::pin!(waiter);
        tokio::select! {
            res = &mut waiter => match res {
                Ok(()) => continue,
                Err(_) => return AwaitOutcome::Timeout(addr),
            },
            res = child.wait() => {
                let status = res.expect("wait on child failed");
                return AwaitOutcome::ChildExited(status);
            }
        }
    }
    AwaitOutcome::Ready
}

/// Spawn the binary with a retry loop that survives the
/// probe-then-drop port race: if the child exits with
/// `Address already in use` in its stderr, re-probe ports and try again
/// (up to 3 attempts).
///
/// - `n_ports` reserves that many `127.0.0.1:0` ports per attempt.
/// - `ready_idx` lists the addrs whose subprocess listener we wait for via
///   [`wait_until_accepting`]. Ports passed through as membership metadata
///   but never `.bind()`-ed by the child (e.g. `admin_addr` without
///   `--admin-listen`) MUST NOT be included — readiness would time out.
/// - `build` constructs the `Command` for each attempt and is called once
///   per retry. The closure captures stable test state (tempdirs, certs);
///   `--bootstrap` is idempotent so reusing a `raft_dir` across attempts
///   is safe.
async fn retry_spawn<F>(
    n_ports: usize,
    ready_idx: &[usize],
    per_addr_budget: Duration,
    mut build: F,
) -> (Child, Vec<SocketAddr>)
where
    F: FnMut(&[SocketAddr]) -> Command,
{
    const MAX_ATTEMPTS: usize = 3;
    let mut last_eaddrinuse: Option<String> = None;

    for attempt in 0..MAX_ATTEMPTS {
        let addrs = bind_unused_set(n_ports).await;
        let mut cmd = build(&addrs);
        disable_metrics_exporter_for_test(&mut cmd);
        cmd.stderr(Stdio::piped()).kill_on_drop(true);
        let mut child = cmd.spawn().expect("spawn tsoracle");

        // Drain stderr concurrently so a chatty child can't fill the pipe
        // buffer and back-pressure itself into a hang. The buffer is read
        // on early exit to classify the failure as EADDRINUSE vs. other.
        let stderr_buf: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
        let mut stderr_pipe = child.stderr.take().expect("stderr piped");
        let drain_buf = Arc::clone(&stderr_buf);
        let drain = tokio::spawn(async move {
            let mut chunk = [0u8; 4096];
            loop {
                match stderr_pipe.read(&mut chunk).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => drain_buf.lock().unwrap().extend_from_slice(&chunk[..n]),
                }
            }
        });

        match await_listening(&mut child, ready_idx, &addrs, per_addr_budget).await {
            AwaitOutcome::Ready => {
                // Detach the drain — it exits naturally when the child
                // closes stderr (on test-end kill).
                drop(drain);
                return (child, addrs);
            }
            AwaitOutcome::ChildExited(status) => {
                let _ = drain.await;
                let stderr = String::from_utf8_lossy(&stderr_buf.lock().unwrap()).into_owned();
                if stderr.contains("Address already in use") {
                    last_eaddrinuse = Some(format!(
                        "attempt {}/{MAX_ATTEMPTS}: EADDRINUSE (status={status})\
                         \nstderr:\n{stderr}",
                        attempt + 1,
                    ));
                    continue;
                }
                panic!(
                    "binary exited before accepting connections: status={status}\
                     \nstderr:\n{stderr}"
                );
            }
            AwaitOutcome::Timeout(addr) => {
                let _ = child.kill().await;
                let _ = drain.await;
                let stderr = String::from_utf8_lossy(&stderr_buf.lock().unwrap()).into_owned();
                panic!(
                    "binary did not start accepting on {addr} within {per_addr_budget:?}\
                     \nstderr:\n{stderr}"
                );
            }
        }
    }

    panic!(
        "EADDRINUSE on all {MAX_ATTEMPTS} port-allocation attempts; last error:\n{}",
        last_eaddrinuse.unwrap_or_else(|| "<none>".into())
    );
}

#[tokio::test]
async fn binary_serves_timestamps() {
    let binary_path = env!("CARGO_BIN_EXE_tsoracle");
    let state_dir = tempdir().unwrap();

    let (mut child, addrs) = retry_spawn(1, &[0], Duration::from_secs(10), |addrs| {
        let mut cmd = Command::new(binary_path);
        cmd.arg("serve")
            .arg("file")
            .arg("--listen")
            .arg(addrs[0].to_string())
            .arg("--state-dir")
            .arg(state_dir.path())
            .arg("--log")
            .arg("warn");
        cmd
    })
    .await;
    let listen_addr = addrs[0];

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

    // Wait until the SIGTERM handler is live: it is registered when the
    // shutdown future is first polled, which happens once tonic is serving —
    // i.e. by the time the listener is accepting connections.
    let (mut child, _addrs) = retry_spawn(1, &[0], Duration::from_secs(10), |addrs| {
        let mut cmd = Command::new(binary_path);
        cmd.arg("serve")
            .arg("file")
            .arg("--listen")
            .arg(addrs[0].to_string())
            .arg("--state-dir")
            .arg(state_dir.path())
            .arg("--log")
            .arg("warn");
        cmd
    })
    .await;

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

    // admin_addr is metadata-only here (no `--admin-listen`); only the
    // client listener is bound, so ready_idx = [0].
    let (mut child, addrs) = retry_spawn(3, &[0], Duration::from_secs(15), |addrs| {
        let listen_addr = addrs[0];
        let raft_addr = addrs[1];
        let admin_addr = addrs[2];
        let mut cmd = Command::new(binary_path);
        cmd.arg("serve")
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
            .arg("warn");
        cmd
    })
    .await;
    let listen_addr = addrs[0];

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

    let (mut child, addrs) = retry_spawn(1, &[0], Duration::from_secs(10), |addrs| {
        let mut cmd = Command::new(binary_path);
        cmd.arg("serve")
            .arg("file")
            .arg("--listen")
            .arg(addrs[0].to_string())
            .arg("--state-dir")
            .arg(state_dir.path())
            .arg("--tls-cert")
            .arg(&certs.cert)
            .arg("--tls-key")
            .arg(&certs.key)
            .arg("--log")
            .arg("warn");
        cmd
    })
    .await;
    let listen_addr = addrs[0];

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

    // admin_addr is metadata-only here (no `--admin-listen`); only the
    // client listener is bound, so ready_idx = [0].
    let (mut child, addrs) = retry_spawn(3, &[0], Duration::from_secs(15), |addrs| {
        let listen_addr = addrs[0];
        let raft_addr = addrs[1];
        let admin_addr = addrs[2];
        let mut cmd = Command::new(binary_path);
        cmd.arg("serve")
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
            .arg("warn");
        cmd
    })
    .await;
    let listen_addr = addrs[0];

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

/// Boot a single-node openraft cluster with `--admin-listen`, wait for it to
/// elect itself leader, then run `tsoracle admin members` and assert that the
/// output contains the bootstrapped node id.
#[cfg(feature = "openraft")]
#[tokio::test]
async fn admin_members_lists_the_bootstrap_node() {
    let binary_path = env!("CARGO_BIN_EXE_tsoracle");
    let raft_dir = tempdir().unwrap();

    // Both the client gRPC port and the admin gRPC port are bound by the
    // child here (`--admin-listen` is passed), so wait on both.
    let (mut server, addrs) = retry_spawn(3, &[0, 2], Duration::from_secs(15), |addrs| {
        let listen_addr = addrs[0];
        let raft_addr = addrs[1];
        let admin_addr = addrs[2];
        let mut cmd = Command::new(binary_path);
        cmd.arg("serve")
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
            .arg("--admin-listen")
            .arg(admin_addr.to_string())
            .arg("--heartbeat-ms")
            .arg("50")
            .arg("--election-min-ms")
            .arg("150")
            .arg("--election-max-ms")
            .arg("300")
            .arg("--log")
            .arg("warn");
        cmd
    })
    .await;
    let listen_addr = addrs[0];
    let admin_addr = addrs[2];

    // Wait for the node to elect itself leader so that list_members returns
    // a coherent view.
    let tso_client = Client::connect(vec![listen_addr.to_string()])
        .await
        .unwrap();
    wait_until_responsive(&tso_client, Duration::from_secs(15))
        .await
        .expect("openraft node never became responsive before admin query");

    // Run: tsoracle admin members --endpoint http://<admin_addr>
    let output = Command::new(binary_path)
        .arg("admin")
        .arg("members")
        .arg("--endpoint")
        .arg(format!("http://{admin_addr}"))
        .output()
        .await
        .expect("spawn tsoracle admin members");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "tsoracle admin members failed (status={})\nstdout: {stdout}\nstderr: {stderr}",
        output.status
    );
    assert!(
        stdout.contains("id=1"),
        "expected stdout to contain 'id=1', got:\n{stdout}"
    );
    assert!(
        stdout.contains("role=Voter"),
        "expected the role rendered as a human-readable name, got:\n{stdout}"
    );

    server.kill().await.unwrap();
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
