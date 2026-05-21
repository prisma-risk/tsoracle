//! Spawned `tsoracle` binaries; POSIX signal + `FAILPOINTS` env chaos. (Plan C.)

use std::time::Duration;

use crate::topology::ChaosController;

pub struct ProcessTopology;

impl ProcessTopology {
    pub async fn spawn(_nodes: usize, _grace: Duration) -> anyhow::Result<Self> {
        anyhow::bail!("process topology not yet implemented (Plan C)")
    }
}

pub struct ProcessController;

#[async_trait::async_trait]
impl ChaosController for ProcessController {
    async fn kill_leader(&self) -> crate::chaos::ChaosEvent { unimplemented!("Plan C") }
    async fn pause_leader(&self, _dur: Duration) -> crate::chaos::ChaosEvent { unimplemented!("Plan C") }
    async fn arm_failpoint(&self, _: &str, _: &str) -> crate::chaos::ChaosEvent { unimplemented!("Plan C") }
    async fn disarm_failpoint(&self, _: &str) -> crate::chaos::ChaosEvent { unimplemented!("Plan C") }
    fn endpoints(&self) -> Vec<String> { unimplemented!("Plan C") }
    fn current_leader(&self) -> Option<crate::topology::NodeId> { unimplemented!("Plan C") }
    async fn shutdown(self: Box<Self>) { unimplemented!("Plan C") }
}
