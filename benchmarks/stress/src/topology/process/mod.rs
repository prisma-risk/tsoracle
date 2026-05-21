//! Process topology: spawned `tsoracle` binaries, POSIX-signal chaos,
//! `FAILPOINTS` env propagation. Unix-only.
//!
//! This module's spawn + chaos surfaces land incrementally; see the
//! follow-up commits that introduce `ProcessTopology::spawn`,
//! `kill_leader`, `pause_leader`, and `arm_failpoint`/`disarm_failpoint`.

mod child;

use std::time::Duration;

use async_trait::async_trait;

use crate::chaos::ChaosEvent;
use crate::topology::{ChaosController, NodeId};

pub use self::child::{ChildHandle, ChildSpec, spawn_child, spawn_into, supervise_child};

pub struct ProcessTopology {
    pub controller: ProcessController,
}

pub struct ProcessController;

impl ProcessTopology {
    pub async fn spawn(_node_count: usize, _grace: Duration) -> anyhow::Result<Self> {
        anyhow::bail!("process topology not yet implemented")
    }
}

#[async_trait]
impl ChaosController for ProcessController {
    async fn kill_leader(&self) -> ChaosEvent {
        unimplemented!("process topology not yet implemented")
    }
    async fn pause_leader(&self, _dur: Duration) -> ChaosEvent {
        unimplemented!("process topology not yet implemented")
    }
    async fn arm_failpoint(&self, _: &str, _: &str) -> ChaosEvent {
        unimplemented!("process topology not yet implemented")
    }
    async fn disarm_failpoint(&self, _: &str) -> ChaosEvent {
        unimplemented!("process topology not yet implemented")
    }
    fn endpoints(&self) -> Vec<String> {
        unimplemented!("process topology not yet implemented")
    }
    fn current_leader(&self) -> Option<NodeId> {
        unimplemented!("process topology not yet implemented")
    }
    async fn shutdown(self: Box<Self>) {
        unimplemented!("process topology not yet implemented")
    }
}
