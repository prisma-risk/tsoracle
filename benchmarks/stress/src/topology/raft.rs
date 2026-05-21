//! In-process openraft cluster on `MemNetwork`. (Plan B.)

use std::time::Duration;

use crate::topology::ChaosController;

pub struct RaftTopology;

impl RaftTopology {
    pub async fn spawn(_nodes: usize, _grace: Duration) -> anyhow::Result<Self> {
        anyhow::bail!("raft topology not yet implemented (Plan B)")
    }
}

pub struct RaftController;

#[async_trait::async_trait]
impl ChaosController for RaftController {
    async fn kill_leader(&self) -> crate::chaos::ChaosEvent { unimplemented!("Plan B") }
    async fn pause_leader(&self, _dur: Duration) -> crate::chaos::ChaosEvent { unimplemented!("Plan B") }
    async fn arm_failpoint(&self, _: &str, _: &str) -> crate::chaos::ChaosEvent { unimplemented!("Plan B") }
    async fn disarm_failpoint(&self, _: &str) -> crate::chaos::ChaosEvent { unimplemented!("Plan B") }
    fn endpoints(&self) -> Vec<String> { unimplemented!("Plan B") }
    fn current_leader(&self) -> Option<crate::topology::NodeId> { unimplemented!("Plan B") }
    async fn shutdown(self: Box<Self>) { unimplemented!("Plan B") }
}
