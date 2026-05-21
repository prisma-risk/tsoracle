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
        | ClientError::InvalidCount(_) => false,
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

        let (tx, rx) = mpsc::channel::<SupervisorEvent>(64);
        let supervisor_handle = tokio::spawn(Supervisor::new().run(rx));

        let stop = Arc::new(AtomicBool::new(false));
        let cfg = ClientTaskCfg {
            client_id: ClientId(0),
            client: client.clone(),
            batch_size: 4,
            warmup_iters: 4,
            liveness_deadline: Duration::from_secs(5),
            stop: stop.clone(),
            tx: tx.clone(),
            transient_retries: Arc::new(AtomicU64::new(0)),
        };
        let task = tokio::spawn(client_task(cfg));

        tokio::time::sleep(Duration::from_millis(200)).await;
        stop.store(true, Ordering::Relaxed);
        let issued = task.await.unwrap().unwrap();

        let _ = tx.send(SupervisorEvent::End).await;
        drop(tx);
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
}
