//! Spawned `tsoracle` binaries; POSIX signal + `FAILPOINTS` env chaos.

use std::time::Duration;

use crate::topology::ChaosController;

pub struct ProcessTopology;

impl ProcessTopology {
    pub async fn spawn(_nodes: usize, _grace: Duration) -> anyhow::Result<Self> {
        anyhow::bail!("process topology not yet implemented")
    }
}

pub struct ProcessController;

#[async_trait::async_trait]
impl ChaosController for ProcessController {
    async fn kill_leader(&self) -> crate::chaos::ChaosEvent {
        unimplemented!("process topology not yet implemented")
    }
    async fn pause_leader(&self, _dur: Duration) -> crate::chaos::ChaosEvent {
        unimplemented!("process topology not yet implemented")
    }
    async fn arm_failpoint(&self, _: &str, _: &str) -> crate::chaos::ChaosEvent {
        unimplemented!("process topology not yet implemented")
    }
    async fn disarm_failpoint(&self, _: &str) -> crate::chaos::ChaosEvent {
        unimplemented!("process topology not yet implemented")
    }
    fn endpoints(&self) -> Vec<String> {
        unimplemented!("process topology not yet implemented")
    }
    fn current_leader(&self) -> Option<crate::topology::NodeId> {
        unimplemented!("process topology not yet implemented")
    }
    async fn shutdown(self: Box<Self>) {
        unimplemented!("process topology not yet implemented")
    }
}
