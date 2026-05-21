//! Run configuration and validation.

use std::net::SocketAddr;
use std::time::Duration;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TopologyKind {
    Mem,
    Raft,
    Process,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ScenarioKind {
    Named(String),
    Random { seed: u64 },
}

#[derive(Debug, Clone)]
pub struct StressConfig {
    pub topology: TopologyKind,
    pub scenario: ScenarioKind,
    pub duration: Option<Duration>,
    pub ops: Option<u64>,
    pub clients: usize,
    pub batch_size: u32,
    pub warmup: u64,
    pub client_threads: usize,
    pub server_threads: usize,
    pub liveness_deadline: Duration,
    pub grace_mem: Duration,
    pub grace_raft: Duration,
    pub grace_process: Duration,
    pub nodes: usize,
    pub bind: SocketAddr,
    pub json: bool,
    pub json_stream: bool,
    pub print_interval: Duration,
    pub seed: u64,
    pub schedule_out: Option<std::path::PathBuf>,
    pub ci_smoke: bool,
}

impl StressConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.duration.is_some() && self.ops.is_some() {
            return Err("--duration and --ops are mutually exclusive".into());
        }
        if self.duration.is_none() && self.ops.is_none() {
            return Err("run requires either --duration or --ops".into());
        }
        if self.clients == 0 {
            return Err("--clients must be >= 1".into());
        }
        if self.batch_size == 0 {
            return Err("--batch-size must be >= 1".into());
        }
        if self.client_threads == 0 {
            return Err("--client-threads must be >= 1".into());
        }
        if self.server_threads == 0 {
            return Err("--server-threads must be >= 1".into());
        }
        if matches!(self.topology, TopologyKind::Raft | TopologyKind::Process) && self.nodes < 1 {
            return Err("--nodes must be >= 1 for raft/process topology".into());
        }
        Ok(())
    }

    /// Effective per-topology grace window for fence-freshness / liveness gating.
    pub fn grace(&self) -> Duration {
        match self.topology {
            TopologyKind::Mem => self.grace_mem,
            TopologyKind::Raft => self.grace_raft,
            TopologyKind::Process => self.grace_process,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use std::time::Duration;

    fn ok_config() -> StressConfig {
        StressConfig {
            topology: TopologyKind::Mem,
            scenario: ScenarioKind::Named("steady".into()),
            duration: Some(Duration::from_secs(20)),
            ops: None,
            clients: 4,
            batch_size: 1,
            warmup: 100,
            client_threads: 1,
            server_threads: 1,
            liveness_deadline: Duration::from_secs(5),
            grace_mem: Duration::from_millis(100),
            grace_raft: Duration::from_millis(750),
            grace_process: Duration::from_secs(2),
            nodes: 3,
            bind: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
            json: false,
            json_stream: false,
            print_interval: Duration::from_secs(1),
            seed: 0,
            schedule_out: None,
            ci_smoke: false,
        }
    }

    #[test]
    fn known_good_validates() {
        ok_config().validate().unwrap();
    }

    #[test]
    fn duration_and_ops_mutex() {
        let mut cfg = ok_config();
        cfg.ops = Some(1000);
        assert!(cfg.validate().unwrap_err().contains("mutually exclusive"));
    }

    #[test]
    fn neither_duration_nor_ops_rejected() {
        let mut cfg = ok_config();
        cfg.duration = None;
        cfg.ops = None;
        assert!(cfg.validate().unwrap_err().contains("requires"));
    }

    #[test]
    fn zero_clients_rejected() {
        let mut cfg = ok_config();
        cfg.clients = 0;
        assert!(cfg.validate().unwrap_err().contains("--clients"));
    }

    #[test]
    fn zero_batch_size_rejected() {
        let mut cfg = ok_config();
        cfg.batch_size = 0;
        assert!(cfg.validate().unwrap_err().contains("--batch-size"));
    }

    #[test]
    fn zero_client_threads_rejected() {
        let mut cfg = ok_config();
        cfg.client_threads = 0;
        assert!(cfg.validate().unwrap_err().contains("--client-threads"));
    }

    #[test]
    fn zero_server_threads_rejected() {
        let mut cfg = ok_config();
        cfg.server_threads = 0;
        assert!(cfg.validate().unwrap_err().contains("--server-threads"));
    }

    #[test]
    fn raft_topology_requires_nodes() {
        let mut cfg = ok_config();
        cfg.topology = TopologyKind::Raft;
        cfg.nodes = 0;
        assert!(cfg.validate().unwrap_err().contains("--nodes"));
    }
}
