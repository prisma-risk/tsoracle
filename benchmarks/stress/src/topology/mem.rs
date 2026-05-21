//! In-process server with `InMemoryDriver`; failpoint and driver-promotion chaos.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tsoracle_consensus::ConsensusDriver;
use tsoracle_core::Epoch;
use tsoracle_server::{Server, test_fakes::InMemoryDriver};

use crate::chaos::{ChaosEvent, ChaosKind, ChaosOutcome};
use crate::topology::{ChaosController, NodeId, timed_event};

/// In-process mem topology: single `InMemoryDriver` + single `Server`.
pub struct MemTopology {
    pub controller: MemController,
    pub server_handle: tokio::task::JoinHandle<Result<(), tsoracle_server::ServerError>>,
}

pub struct MemController {
    driver: Arc<InMemoryDriver>,
    endpoint: String,
    /// Bumped on each leader promotion so we don't clash with previous epochs.
    /// Reserved for T17 (kill_leader promotes a fresh epoch).
    #[allow(dead_code)]
    epoch: Mutex<Epoch>,
    shutdown_tx: Mutex<Option<oneshot::Sender<()>>>,
    grace: Duration,
}

impl MemTopology {
    pub async fn spawn(grace: Duration) -> anyhow::Result<Self> {
        let driver = Arc::new(InMemoryDriver::new());
        driver.become_leader(Epoch(1));
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .context("bind loopback for mem topology")?;
        let addr: SocketAddr = listener.local_addr()?;
        let server = Server::builder()
            .consensus_driver(driver.clone() as Arc<dyn ConsensusDriver>)
            .build()
            .map_err(|e| anyhow::anyhow!("server build: {e:?}"))?;
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let server_handle = tokio::spawn(async move {
            server
                .serve_with_listener(listener, async {
                    let _ = shutdown_rx.await;
                })
                .await
        });
        let controller = MemController {
            driver,
            endpoint: format!("http://{addr}"),
            epoch: Mutex::new(Epoch(1)),
            shutdown_tx: Mutex::new(Some(shutdown_tx)),
            grace,
        };
        Ok(MemTopology { controller, server_handle })
    }
}

#[async_trait]
impl ChaosController for MemController {
    async fn kill_leader(&self) -> ChaosEvent {
        timed_event(ChaosKind::LeaderKill, self.grace, || async {
            ChaosOutcome::Skipped { reason: "kill_leader not yet implemented (T17)".into() }
        })
        .await
    }
    async fn pause_leader(&self, _dur: Duration) -> ChaosEvent {
        timed_event(ChaosKind::LeaderPause, self.grace, || async {
            ChaosOutcome::Skipped { reason: "pause_leader not yet implemented (T17)".into() }
        })
        .await
    }
    async fn arm_failpoint(&self, name: &str, _action: &str) -> ChaosEvent {
        timed_event(ChaosKind::FailpointArm { name: name.into() }, self.grace, || async {
            ChaosOutcome::Skipped { reason: "failpoints not yet implemented (T18)".into() }
        })
        .await
    }
    async fn disarm_failpoint(&self, name: &str) -> ChaosEvent {
        timed_event(ChaosKind::FailpointDisarm { name: name.into() }, self.grace, || async {
            ChaosOutcome::Skipped { reason: "failpoints not yet implemented (T18)".into() }
        })
        .await
    }
    fn endpoints(&self) -> Vec<String> {
        vec![self.endpoint.clone()]
    }
    fn current_leader(&self) -> Option<NodeId> {
        Some(NodeId(0))
    }
    async fn shutdown(self: Box<Self>) {
        if let Some(tx) = self.shutdown_tx.lock().take() {
            let _ = tx.send(());
        }
        let _ = self.driver;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn spawn_returns_reachable_endpoint() {
        let topo = MemTopology::spawn(Duration::from_millis(100)).await.unwrap();
        let endpoints = topo.controller.endpoints();
        assert_eq!(endpoints.len(), 1);
        let client = tsoracle_client::Client::connect(endpoints).await.unwrap();
        let ts = client.get_ts().await.unwrap();
        assert!(ts.0 > 0, "expected a positive timestamp, got {ts:?}");
        Box::new(topo.controller).shutdown().await;
    }
}
