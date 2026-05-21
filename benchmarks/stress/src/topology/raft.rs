//! In-process openraft cluster on `MemNetwork`.

use std::time::Duration;

use crate::topology::ChaosController;

pub struct RaftTopology;

impl RaftTopology {
    pub async fn spawn(_nodes: usize, _grace: Duration) -> anyhow::Result<Self> {
        anyhow::bail!("raft topology not yet implemented")
    }
}

pub struct RaftController;

#[async_trait::async_trait]
impl ChaosController for RaftController {
    async fn kill_leader(&self) -> crate::chaos::ChaosEvent {
        unimplemented!("raft topology not yet implemented")
    }
    async fn pause_leader(&self, _dur: Duration) -> crate::chaos::ChaosEvent {
        unimplemented!("raft topology not yet implemented")
    }
    async fn arm_failpoint(&self, _: &str, _: &str) -> crate::chaos::ChaosEvent {
        unimplemented!("raft topology not yet implemented")
    }
    async fn disarm_failpoint(&self, _: &str) -> crate::chaos::ChaosEvent {
        unimplemented!("raft topology not yet implemented")
    }
    fn endpoints(&self) -> Vec<String> {
        unimplemented!("raft topology not yet implemented")
    }
    fn current_leader(&self) -> Option<crate::topology::NodeId> {
        unimplemented!("raft topology not yet implemented")
    }
    async fn shutdown(self: Box<Self>) {
        unimplemented!("raft topology not yet implemented")
    }
}
