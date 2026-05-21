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
        let driver = self.driver.clone();
        let next_epoch = {
            let mut guard = self.epoch.lock();
            *guard = Epoch(guard.0 + 1);
            *guard
        };
        timed_event(ChaosKind::LeaderKill, self.grace, move || async move {
            // Step down then promote at the next epoch. With a single in-memory
            // driver, "kill the leader and elect a new one" collapses to this
            // synchronous transition. The fence in tsoracle-core guarantees the
            // new epoch's first timestamp is > the old high-water.
            driver.become_follower(None);
            driver.become_leader(next_epoch);
            ChaosOutcome::Applied
        })
        .await
    }
    async fn pause_leader(&self, dur: Duration) -> ChaosEvent {
        let driver = self.driver.clone();
        let next_epoch = {
            let mut guard = self.epoch.lock();
            *guard = Epoch(guard.0 + 1);
            *guard
        };
        timed_event(ChaosKind::LeaderPause, self.grace, move || async move {
            driver.become_follower(None);
            tokio::time::sleep(dur).await;
            driver.become_leader(next_epoch);
            ChaosOutcome::Applied
        })
        .await
    }
    async fn arm_failpoint(&self, name: &str, action: &str) -> ChaosEvent {
        let kind = ChaosKind::FailpointArm { name: name.into() };
        #[cfg(feature = "stress-failpoints")]
        {
            let name = name.to_string();
            let action = action.to_string();
            return timed_event(kind, self.grace, move || async move {
                match fail::cfg(name.as_str(), action.as_str()) {
                    Ok(()) => ChaosOutcome::Applied,
                    Err(e) => ChaosOutcome::Failed { reason: format!("fail::cfg: {e}") },
                }
            })
            .await;
        }
        #[cfg(not(feature = "stress-failpoints"))]
        {
            let _ = (name, action);
            timed_event(kind, self.grace, || async {
                ChaosOutcome::Skipped {
                    reason: "stress-failpoints feature off; failpoints not linked".into(),
                }
            })
            .await
        }
    }
    async fn disarm_failpoint(&self, name: &str) -> ChaosEvent {
        let kind = ChaosKind::FailpointDisarm { name: name.into() };
        #[cfg(feature = "stress-failpoints")]
        {
            let name = name.to_string();
            return timed_event(kind, self.grace, move || async move {
                fail::remove(name.as_str());
                ChaosOutcome::Applied
            })
            .await;
        }
        #[cfg(not(feature = "stress-failpoints"))]
        {
            let _ = name;
            timed_event(kind, self.grace, || async {
                ChaosOutcome::Skipped { reason: "stress-failpoints feature off".into() }
            })
            .await
        }
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

    use crate::topology::ChaosController;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn kill_leader_bumps_epoch_and_promotes() {
        let topo = MemTopology::spawn(Duration::from_millis(50)).await.unwrap();
        let client = tsoracle_client::Client::connect(topo.controller.endpoints()).await.unwrap();
        let ts1 = client.get_ts().await.unwrap();
        let ev = topo.controller.kill_leader().await;
        assert!(ev.outcome.is_applied(), "got {:?}", ev.outcome);
        let ts2 = client.get_ts().await.unwrap();
        assert!(ts2 > ts1, "ts2={ts2:?} should be > ts1={ts1:?}");
        Box::new(topo.controller).shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pause_leader_returns_applied() {
        let topo = MemTopology::spawn(Duration::from_millis(50)).await.unwrap();
        let ev = topo.controller.pause_leader(Duration::from_millis(20)).await;
        assert!(ev.outcome.is_applied());
        Box::new(topo.controller).shutdown().await;
    }

    #[cfg(feature = "stress-failpoints")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn arm_disarm_failpoint_round_trip() {
        let topo = MemTopology::spawn(Duration::from_millis(50)).await.unwrap();
        let ev_arm = topo
            .controller
            .arm_failpoint("stress-test::fp_unused", "off")
            .await;
        assert!(ev_arm.outcome.is_applied(), "arm: {:?}", ev_arm.outcome);
        let ev_disarm = topo.controller.disarm_failpoint("stress-test::fp_unused").await;
        assert!(ev_disarm.outcome.is_applied(), "disarm: {:?}", ev_disarm.outcome);
        Box::new(topo.controller).shutdown().await;
    }

    #[cfg(not(feature = "stress-failpoints"))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn failpoints_off_returns_skipped() {
        let topo = MemTopology::spawn(Duration::from_millis(50)).await.unwrap();
        let ev = topo.controller.arm_failpoint("any", "panic").await;
        match ev.outcome {
            ChaosOutcome::Skipped { .. } => {}
            other => panic!("expected Skipped, got {other:?}"),
        }
        Box::new(topo.controller).shutdown().await;
    }
}
