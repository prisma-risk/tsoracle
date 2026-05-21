//! Final report: outcome, latency, throughput, violations, chaos events.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::chaos::ChaosEvent;
use crate::config::{StressConfig, TopologyKind};
use crate::git::GitInfo;
use crate::schedule::Schedule;
use crate::violation::Violation;

#[derive(Debug, Clone)]
pub struct Report {
    pub config: StressConfig,
    pub git: GitInfo,
    pub hostname: String,
    pub topology: TopologyKind,
    pub elapsed: Duration,
    pub recorded: RecordedCounts,
    pub throughput: Throughput,
    pub latency_per_call_us: LatencyStats,
    pub transient_retries: u64,
    pub out_of_range_samples: u64,
    pub violations: Vec<Violation>,
    pub chaos_events: Vec<ChaosEvent>,
    pub schedule: Schedule,
    pub outcome: Outcome,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct RecordedCounts {
    pub client_calls: u64,
    pub timestamps: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Throughput {
    pub client_calls_per_sec: f64,
    pub timestamps_per_sec: f64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct LatencyStats {
    pub p50: u64,
    pub p90: u64,
    pub p99: u64,
    pub p999: u64,
    pub min: u64,
    pub max: u64,
    pub mean: u64,
}

#[derive(Debug, Clone)]
pub enum Outcome {
    Ok,
    InvariantViolation,
    ProgrammerError { reason: String },
    HarnessError { kind: HarnessErrorKind },
    Interrupted,
}

#[derive(Debug, Clone)]
pub enum HarnessErrorKind {
    ServerPanic { topology: TopologyKind, detail: String },
    SpawnFailure { topology: TopologyKind, detail: String },
    TokioTaskPanic { task: &'static str, detail: String },
    HostFault { detail: String },
}

impl Outcome {
    /// Map `Outcome` to the process exit code documented in the spec.
    pub fn exit_code(&self) -> i32 {
        match self {
            Outcome::Ok => 0,
            Outcome::InvariantViolation => 1,
            Outcome::ProgrammerError { .. } => 2,
            Outcome::HarnessError { .. } => 3,
            Outcome::Interrupted => 130,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exit_code_mapping() {
        assert_eq!(Outcome::Ok.exit_code(), 0);
        assert_eq!(Outcome::InvariantViolation.exit_code(), 1);
        assert_eq!(Outcome::ProgrammerError { reason: "x".into() }.exit_code(), 2);
        assert_eq!(
            Outcome::HarnessError { kind: HarnessErrorKind::HostFault { detail: "y".into() } }
                .exit_code(),
            3
        );
        assert_eq!(Outcome::Interrupted.exit_code(), 130);
    }
}
