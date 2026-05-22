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

//! Pool of client tasks issuing GetTs / GetTsBatch.
//!
//! MIRRORS `bench-minimal::is_transient` with one local addition:
//! `FailedPrecondition` is *transient* here because it is the legitimate
//! failover-fence error code under chaos (see spec § "Client RPC errors").
//! The two copies are kept in sync manually.

use tonic::Code;
use tsoracle_client::ClientError;

pub fn is_transient(err: &ClientError) -> bool {
    match err {
        ClientError::Transport(_) => true,
        ClientError::Rpc(status) => matches!(
            status.code(),
            Code::Unavailable
                | Code::DeadlineExceeded
                | Code::ResourceExhausted
                | Code::FailedPrecondition
        ),
        ClientError::NoReachableEndpoints
        | ClientError::InvalidEndpoint(_)
        | ClientError::InvalidCount(_)
        | ClientError::Connector(_)
        | ClientError::DriverGone => false,
    }
}

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tsoracle_client::Client;

use crate::event::SupervisorEvent;
use crate::sample::{IssuedSample, LivenessIncident, LivenessIncidentKind};
use crate::types::{BatchId, ClientId};

/// Per-task input. One instance per loadgen task.
pub struct ClientTaskCfg {
    pub client_id: ClientId,
    pub client: Arc<Client>,
    pub batch_size: u32,
    pub warmup_iters: u64,
    pub liveness_deadline: Duration,
    pub stop: Arc<AtomicBool>,
    pub tx: mpsc::Sender<SupervisorEvent>,
    pub transient_retries: Arc<AtomicU64>,
}

/// Run one client task: warmup (untimed, not forwarded), then issue calls
/// until `stop` is set, forwarding each timestamp to the supervisor.
///
/// Returns `Err` only on non-transient `ClientError` (programmer error per
/// spec § "Client RPC errors"); transient errors are retried, with retries
/// exceeding the deadline budget emitting a `LivenessIncident` and the call
/// being abandoned (loop continues with the next call).
pub async fn client_task(cfg: ClientTaskCfg) -> Result<u64, tsoracle_client::ClientError> {
    // Warmup: discard results.
    for _ in 0..cfg.warmup_iters {
        let _ = issue_one(&cfg.client, cfg.batch_size, &cfg.transient_retries).await?;
    }

    let mut batch_counter: BatchId = 0;
    let mut timestamps_issued: u64 = 0;
    while !cfg.stop.load(Ordering::Relaxed) {
        let started = Instant::now();
        let mut attempts: u32 = 0;
        let issued_at = started;
        let timestamps = loop {
            attempts += 1;
            let result = if cfg.batch_size == 1 {
                cfg.client.get_ts().await.map(|t| vec![t])
            } else {
                cfg.client.get_ts_batch(cfg.batch_size).await
            };
            match result {
                Ok(ts) => break ts,
                Err(e) if is_transient(&e) => {
                    cfg.transient_retries.fetch_add(1, Ordering::Relaxed);
                    if started.elapsed() >= cfg.liveness_deadline {
                        let incident = LivenessIncident {
                            kind: LivenessIncidentKind::DeadlineExceeded {
                                client_id: cfg.client_id,
                                attempts,
                                last_error: format!("{e:?}"),
                                started_at: started,
                            },
                            at: Instant::now(),
                        };
                        let _ = cfg.tx.send(SupervisorEvent::Liveness(incident)).await;
                        break Vec::new();
                    }
                    continue;
                }
                Err(e) => return Err(e),
            }
        };

        let recv_time = Instant::now();
        let n = timestamps.len() as u32;
        let this_batch = batch_counter;
        batch_counter = batch_counter.wrapping_add(1);
        for (idx, ts) in timestamps.into_iter().enumerate() {
            let sample = IssuedSample {
                client_id: cfg.client_id,
                batch_id: this_batch,
                batch_idx: idx as u32,
                is_last: (idx as u32) == n.saturating_sub(1),
                ts,
                issued_at,
                recv_time,
            };
            timestamps_issued += 1;
            if cfg.tx.send(SupervisorEvent::Issued(sample)).await.is_err() {
                return Ok(timestamps_issued);
            }
        }
    }
    Ok(timestamps_issued)
}

async fn issue_one(
    client: &Client,
    batch_size: u32,
    transient_retries: &AtomicU64,
) -> Result<u64, tsoracle_client::ClientError> {
    loop {
        let result = if batch_size == 1 {
            client.get_ts().await.map(|_| 1u64)
        } else {
            client
                .get_ts_batch(batch_size)
                .await
                .map(|b| b.len() as u64)
        };
        match result {
            Ok(n) => return Ok(n),
            Err(e) if is_transient(&e) => {
                transient_retries.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            Err(e) => return Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::{Code, Status};
    use tsoracle_client::ClientError;

    #[test]
    fn transient_classifies_known_codes() {
        let status = Status::new(Code::Unavailable, "leader changed");
        assert!(is_transient(&ClientError::Rpc(status)));
        let status = Status::new(Code::DeadlineExceeded, "timeout");
        assert!(is_transient(&ClientError::Rpc(status)));
        let status = Status::new(Code::ResourceExhausted, "backpressure");
        assert!(is_transient(&ClientError::Rpc(status)));
        let status = Status::new(Code::FailedPrecondition, "fence active");
        assert!(is_transient(&ClientError::Rpc(status)));
    }

    #[test]
    fn non_transient_rejected() {
        let status = Status::new(Code::InvalidArgument, "bad batch size");
        assert!(!is_transient(&ClientError::Rpc(status)));
        assert!(!is_transient(&ClientError::NoReachableEndpoints));
    }

    use crate::supervisor::Supervisor;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;
    use tsoracle_consensus::ConsensusDriver;
    use tsoracle_core::Epoch;
    use tsoracle_server::{Server, test_fakes::InMemoryDriver};

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn client_task_round_trip_against_real_server() {
        let driver = Arc::new(InMemoryDriver::new());
        driver.become_leader(Epoch(1));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = Server::builder()
            .consensus_driver(driver.clone() as Arc<dyn ConsensusDriver>)
            .build()
            .unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let server_handle = tokio::spawn(async move {
            server
                .serve_with_listener(listener, async {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        let endpoint = format!("http://{addr}");
        let client = Arc::new(Client::connect(vec![endpoint]).await.unwrap());

        // Insert a counting forwarder between the loadgen task and the
        // Supervisor so the test can observe progress by polling instead of
        // sleeping a fixed wall-clock budget — the latter is brittle under
        // sanitizer or emulation slowdown where first-request latency can
        // exceed any reasonable hand-picked duration.
        let (tx_from_task, mut rx_from_task) = mpsc::channel::<SupervisorEvent>(64);
        let (tx_to_supervisor, rx_to_supervisor) = mpsc::channel::<SupervisorEvent>(64);
        let supervisor_handle = tokio::spawn(Supervisor::new().run(rx_to_supervisor));

        let issued_seen = Arc::new(AtomicU64::new(0));
        let issued_seen_for_forwarder = issued_seen.clone();
        let forwarder_handle = tokio::spawn(async move {
            while let Some(event) = rx_from_task.recv().await {
                if matches!(event, SupervisorEvent::Issued(_)) {
                    issued_seen_for_forwarder.fetch_add(1, Ordering::Relaxed);
                }
                if tx_to_supervisor.send(event).await.is_err() {
                    break;
                }
            }
        });

        let stop = Arc::new(AtomicBool::new(false));
        let cfg = ClientTaskCfg {
            client_id: ClientId(0),
            client: client.clone(),
            batch_size: 4,
            warmup_iters: 4,
            liveness_deadline: Duration::from_secs(5),
            stop: stop.clone(),
            tx: tx_from_task.clone(),
            transient_retries: Arc::new(AtomicU64::new(0)),
        };
        let task = tokio::spawn(client_task(cfg));

        // Poll for the first issued sample. The 30s ceiling is well above
        // any realistic worst case; the loop exits as soon as progress
        // shows, so fast machines pay nothing extra.
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while std::time::Instant::now() < deadline {
            if issued_seen.load(Ordering::Relaxed) > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        stop.store(true, Ordering::Relaxed);
        let issued = task.await.unwrap().unwrap();

        let _ = tx_from_task.send(SupervisorEvent::End).await;
        drop(tx_from_task);
        let _ = forwarder_handle.await;
        let outcome = supervisor_handle.await.unwrap();
        assert!(issued > 0, "expected some timestamps issued, got 0");
        assert!(
            outcome.violations.is_empty(),
            "got {:?}",
            outcome.violations
        );

        let _ = shutdown_tx.send(());
        let _ = server_handle.await;
    }

    /// Helper: spawn a server with a driver in `leader`-or-`follower` state and
    /// return everything the test needs to drive a `client_task` against it.
    async fn spawn_server_with(
        driver: Arc<InMemoryDriver>,
    ) -> (
        Arc<Client>,
        tokio::sync::oneshot::Sender<()>,
        tokio::task::JoinHandle<Result<(), tsoracle_server::ServerError>>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = Server::builder()
            .consensus_driver(driver as Arc<dyn ConsensusDriver>)
            .build()
            .unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let server_handle = tokio::spawn(async move {
            server
                .serve_with_listener(listener, async {
                    let _ = shutdown_rx.await;
                })
                .await
        });
        let client = Arc::new(
            Client::connect(vec![format!("http://{addr}")])
                .await
                .unwrap(),
        );
        (client, shutdown_tx, server_handle)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn warmup_propagates_non_transient_error() {
        let driver = Arc::new(InMemoryDriver::new());
        driver.become_leader(Epoch(1));
        let (client, shutdown_tx, server_handle) = spawn_server_with(driver).await;
        let (tx, _rx) = mpsc::channel::<SupervisorEvent>(8);
        // `batch_size: 0` makes `Client::get_ts_batch` short-circuit with
        // `InvalidCount(0)`, a non-transient error. With `warmup_iters >= 1`
        // that error propagates out of `client_task` via the `?` on the
        // warmup `issue_one` call — the non-transient-error path.
        let cfg = ClientTaskCfg {
            client_id: ClientId(0),
            client: client.clone(),
            batch_size: 0,
            warmup_iters: 1,
            liveness_deadline: Duration::from_secs(5),
            stop: Arc::new(AtomicBool::new(false)),
            tx,
            transient_retries: Arc::new(AtomicU64::new(0)),
        };
        let result = client_task(cfg).await;
        match result {
            Err(ClientError::InvalidCount(0)) => {}
            other => panic!("expected InvalidCount(0), got {other:?}"),
        }
        let _ = shutdown_tx.send(());
        let _ = server_handle.await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn deadline_exceeded_emits_liveness_incident() {
        // A follower-state driver makes the server return FailedPrecondition
        // on every GetTs call. That code is transient per the stress
        // classifier, so `client_task` retries until the per-call deadline
        // budget is exhausted, then emits a `LivenessIncident::DeadlineExceeded`.
        let driver = Arc::new(InMemoryDriver::new());
        driver.become_follower(None);
        let (client, shutdown_tx, server_handle) = spawn_server_with(driver).await;
        let (tx, mut rx) = mpsc::channel::<SupervisorEvent>(64);
        let stop = Arc::new(AtomicBool::new(false));
        let cfg = ClientTaskCfg {
            client_id: ClientId(0),
            client: client.clone(),
            batch_size: 1,
            warmup_iters: 0,
            liveness_deadline: Duration::from_millis(100),
            stop: stop.clone(),
            tx,
            transient_retries: Arc::new(AtomicU64::new(0)),
        };
        let task = tokio::spawn(client_task(cfg));

        let mut saw_incident = false;
        for _ in 0..20 {
            match tokio::time::timeout(Duration::from_millis(200), rx.recv()).await {
                Ok(Some(SupervisorEvent::Liveness(incident))) => {
                    match incident.kind {
                        LivenessIncidentKind::DeadlineExceeded { client_id, .. } => {
                            assert_eq!(client_id, ClientId(0));
                            saw_incident = true;
                        }
                        other => panic!("unexpected liveness kind: {other:?}"),
                    }
                    break;
                }
                Ok(Some(_)) | Ok(None) => continue,
                Err(_) => continue,
            }
        }
        stop.store(true, Ordering::Relaxed);
        let _ = task.await.unwrap();
        let _ = shutdown_tx.send(());
        let _ = server_handle.await;
        assert!(
            saw_incident,
            "expected a DeadlineExceeded LivenessIncident from follower-state server",
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn closed_supervisor_channel_ends_task() {
        let driver = Arc::new(InMemoryDriver::new());
        driver.become_leader(Epoch(1));
        let (client, shutdown_tx, server_handle) = spawn_server_with(driver).await;
        let (tx, rx) = mpsc::channel::<SupervisorEvent>(1);
        // Drop the receiver immediately. The next `tx.send` after the bounded
        // channel fills will return `Err`, exercising the early-return branch
        // in `client_task`.
        drop(rx);
        let cfg = ClientTaskCfg {
            client_id: ClientId(0),
            client: client.clone(),
            batch_size: 1,
            warmup_iters: 0,
            liveness_deadline: Duration::from_secs(5),
            stop: Arc::new(AtomicBool::new(false)),
            tx,
            transient_retries: Arc::new(AtomicU64::new(0)),
        };
        let result = tokio::time::timeout(Duration::from_secs(2), client_task(cfg))
            .await
            .expect("client_task should exit after the channel closes")
            .unwrap();
        // No assertion on the exact count: it depends on how fast the bounded
        // channel fills, which is racy. The contract is just "returns Ok".
        let _ = result;
        let _ = shutdown_tx.send(());
        let _ = server_handle.await;
    }
}
