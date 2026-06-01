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

use std::process::{ExitStatus, Stdio};
use std::sync::{Arc, Mutex};
use std::{io, net::SocketAddr, time::Duration};
use tempfile::tempdir;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::process::{Child, Command};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};

/// Allocate `n` unused 127.0.0.1 ports by binding `n` listeners at once and
/// dropping them together, which keeps the probe window per-port as short as
/// possible. The kernel can still hand any one of these ports to another
/// process between `drop` and the subprocess's `.bind()` — that residual
/// race is handled by [`retry_spawn_with_stderr`].
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
/// racing every wait against the child exiting early.
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

/// Same as smoke::retry_spawn but ALSO returns the stdout-buffer Arc<Mutex<Vec<u8>>>
/// and the background drain `JoinHandle` so the test can `await` the drain after
/// killing the child (ensuring all buffered output is flushed before asserting).
///
/// NOTE: `tracing_subscriber::fmt()` (the default, used by the tsoracle binary) writes
/// tracing logs to **stdout**. This helper therefore pipes and drains **stdout**; stderr
/// is inherited so EADDRINUSE diagnostics still reach the test runner for debugging.
///
/// Pattern:
/// ```ignore
/// let (mut child, addrs, drain, stdout_buf) = retry_spawn_with_stderr(...).await;
/// // ... run the child ...
/// child.start_kill().unwrap();
/// let _ = child.wait().await;          // child exit closes its pipe end
/// let stdout = collect_stderr(drain, stdout_buf).await;   // drain sees EOF, exits cleanly
/// ```
///
/// - `n_ports` reserves that many `127.0.0.1:0` ports per attempt.
/// - `ready_idx` lists the addrs whose subprocess listener we wait for via
///   [`wait_until_accepting`].
/// - `build` constructs the `Command` for each attempt and is called once per retry.
async fn retry_spawn_with_stderr<F>(
    n_ports: usize,
    ready_idx: &[usize],
    per_addr_budget: Duration,
    mut build: F,
) -> (Child, Vec<SocketAddr>, JoinHandle<()>, Arc<Mutex<Vec<u8>>>)
where
    F: FnMut(&[SocketAddr]) -> Command,
{
    const MAX_ATTEMPTS: usize = 3;
    let mut last_eaddrinuse: Option<String> = None;

    for attempt in 0..MAX_ATTEMPTS {
        let addrs = bind_unused_set(n_ports).await;
        let mut cmd = build(&addrs);
        disable_metrics_exporter_for_test(&mut cmd);
        // tracing_subscriber::fmt() (the binary's default) writes to STDOUT, so
        // pipe stdout to capture heartbeat log lines. Also pipe stderr to detect
        // EADDRINUSE on early-exit retry logic.
        cmd.stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = cmd.spawn().expect("spawn tsoracle (heartbeat_subprocess)");

        // Drain stdout concurrently so a chatty child can't fill the pipe buffer
        // and back-pressure itself into a hang. The buffer is kept alive and
        // returned to the caller so it can read accumulated log output after
        // killing the child.
        let stdout_buf: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
        let mut stdout_pipe = child.stdout.take().expect("stdout piped");
        let drain_buf = Arc::clone(&stdout_buf);
        let drain = tokio::spawn(async move {
            let mut chunk = [0u8; 4096];
            loop {
                match stdout_pipe.read(&mut chunk).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => drain_buf.lock().unwrap().extend_from_slice(&chunk[..n]),
                }
            }
        });

        // Also drain stderr into a separate buffer used only for EADDRINUSE
        // classification on early exit. It is not returned to the caller.
        let stderr_diag_buf: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
        let mut stderr_pipe = child.stderr.take().expect("stderr piped");
        let stderr_drain_buf = Arc::clone(&stderr_diag_buf);
        let stderr_drain = tokio::spawn(async move {
            let mut chunk = [0u8; 4096];
            loop {
                match stderr_pipe.read(&mut chunk).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => stderr_drain_buf
                        .lock()
                        .unwrap()
                        .extend_from_slice(&chunk[..n]),
                }
            }
        });

        match await_listening(&mut child, ready_idx, &addrs, per_addr_budget).await {
            AwaitOutcome::Ready => {
                // Return the stdout drain handle so the caller can await it after
                // killing the child. The child's exit closes its pipe end, which
                // makes the drain task see EOF and exit promptly.
                // The stderr drain is detached; it exits when the child closes.
                drop(stderr_drain);
                return (child, addrs, drain, stdout_buf);
            }
            AwaitOutcome::ChildExited(status) => {
                let _ = drain.await;
                let _ = stderr_drain.await;
                let stdout = String::from_utf8_lossy(&stdout_buf.lock().unwrap()).into_owned();
                let stderr = String::from_utf8_lossy(&stderr_diag_buf.lock().unwrap()).into_owned();
                // EADDRINUSE may surface on either stdout (tracing) or stderr.
                if stdout.contains("Address already in use")
                    || stderr.contains("Address already in use")
                {
                    last_eaddrinuse = Some(format!(
                        "attempt {}/{MAX_ATTEMPTS}: EADDRINUSE (status={status})\
                         \nstdout:\n{stdout}\nstderr:\n{stderr}",
                        attempt + 1,
                    ));
                    continue;
                }
                panic!(
                    "heartbeat_subprocess: binary exited before accepting connections: \
                     status={status}\nstdout:\n{stdout}\nstderr:\n{stderr}"
                );
            }
            AwaitOutcome::Timeout(addr) => {
                let _ = child.kill().await;
                let _ = drain.await;
                let _ = stderr_drain.await;
                let stdout = String::from_utf8_lossy(&stdout_buf.lock().unwrap()).into_owned();
                let stderr = String::from_utf8_lossy(&stderr_diag_buf.lock().unwrap()).into_owned();
                panic!(
                    "heartbeat_subprocess: binary did not start accepting on {addr} \
                     within {per_addr_budget:?}\nstdout:\n{stdout}\nstderr:\n{stderr}"
                );
            }
        }
    }

    panic!(
        "heartbeat_subprocess: EADDRINUSE on all {MAX_ATTEMPTS} port-allocation attempts; \
         last error:\n{}",
        last_eaddrinuse.unwrap_or_else(|| "<none>".into())
    );
}

/// Await the stdout drain task and return the accumulated output as a `String`.
///
/// Call this after `child.start_kill() + child.wait()`: the child's exit closes
/// its stdout pipe, which causes the drain task to see EOF and exit cleanly.
async fn collect_log_output(drain: JoinHandle<()>, stdout_buf: Arc<Mutex<Vec<u8>>>) -> String {
    let _ = drain.await;
    String::from_utf8_lossy(&stdout_buf.lock().unwrap()).into_owned()
}

/// The binary emits heartbeat log lines at (approximately) the configured interval.
///
/// We run the file driver with `--heartbeat-interval 100ms` for ~400 ms and
/// assert that at least 2 lines containing `tsoracle::heartbeat` appear on
/// stderr, which proves the background heartbeat task is firing.
#[tokio::test]
async fn binary_emits_heartbeat_lines_at_configured_interval() {
    let binary_path = env!("CARGO_BIN_EXE_tsoracle");
    let state_dir = tempdir().unwrap();

    let (mut child, _addrs, drain, stdout_buf) =
        retry_spawn_with_stderr(1, &[0], Duration::from_secs(10), |addrs| {
            let mut cmd = Command::new(binary_path);
            cmd.arg("serve")
                .arg("file")
                .arg("--listen")
                .arg(addrs[0].to_string())
                .arg("--state-dir")
                .arg(state_dir.path())
                .arg("--log")
                .arg("info")
                .arg("--heartbeat-interval")
                .arg("100ms");
            cmd
        })
        .await;

    // ~400 ms gives 3-4 heartbeat ticks at 100 ms interval.
    tokio::time::sleep(Duration::from_millis(400)).await;
    child.start_kill().unwrap();
    let _ = child.wait().await;

    // Await the drain task: child exit closes its stdout pipe, so the drain
    // sees EOF and exits promptly. This guarantees all buffered log output is
    // flushed before we assert.
    let stdout = collect_log_output(drain, stdout_buf).await;
    let count = stdout
        .lines()
        .filter(|l| l.contains("tsoracle::heartbeat"))
        .count();
    assert!(
        count >= 2,
        "expected >= 2 heartbeat lines, got {count}.\nstdout:\n{stdout}"
    );
}

/// With `--heartbeat-interval 0s` the heartbeat task is disabled and the binary
/// must emit zero `tsoracle::heartbeat` lines during normal operation.
#[tokio::test]
async fn binary_with_zero_heartbeat_interval_emits_no_heartbeat_lines() {
    let binary_path = env!("CARGO_BIN_EXE_tsoracle");
    let state_dir = tempdir().unwrap();

    let (mut child, _addrs, drain, stdout_buf) =
        retry_spawn_with_stderr(1, &[0], Duration::from_secs(10), |addrs| {
            let mut cmd = Command::new(binary_path);
            cmd.arg("serve")
                .arg("file")
                .arg("--listen")
                .arg(addrs[0].to_string())
                .arg("--state-dir")
                .arg(state_dir.path())
                .arg("--log")
                .arg("info")
                .arg("--heartbeat-interval")
                .arg("0s");
            cmd
        })
        .await;

    tokio::time::sleep(Duration::from_millis(400)).await;
    child.start_kill().unwrap();
    let _ = child.wait().await;
    let stdout = collect_log_output(drain, stdout_buf).await;

    let count = stdout
        .lines()
        .filter(|l| l.contains("tsoracle::heartbeat"))
        .count();
    assert_eq!(
        count, 0,
        "expected zero heartbeat lines, got {count}.\nstdout:\n{stdout}"
    );
}
