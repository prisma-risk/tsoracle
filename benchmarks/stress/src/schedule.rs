//! Nemesis schedule: deterministic sequence of timed chaos ops.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::chaos::ChaosOp;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schedule {
    pub source: ScheduleSource,
    pub ops: Vec<ScheduledOp>,
    /// `burst` and similar load-modulation scenarios carry a loadgen-side
    /// pause. `None` for pure-nemesis scenarios.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub loadgen_pause: Option<LoadgenPause>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ScheduleSource {
    Named { scenario: String },
    Random { seed: u64, params: RandomParams },
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RandomParams {
    #[serde(with = "humantime_serde")]
    pub mean_gap: Duration,
    #[serde(with = "humantime_serde")]
    pub total: Duration,
    pub weight_kill: f64,
    pub weight_pause: f64,
    pub weight_failpoint: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduledOp {
    #[serde(with = "humantime_serde")]
    pub at: Duration,
    pub op: ChaosOp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadgenPause {
    #[serde(with = "humantime_serde")]
    pub at: Duration,
    #[serde(with = "humantime_serde")]
    pub dur: Duration,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chaos::ChaosOp;
    use std::time::Duration;

    #[test]
    fn schedule_json_round_trip() {
        let s = Schedule {
            source: ScheduleSource::Named {
                scenario: "killer-loop".into(),
            },
            ops: vec![
                ScheduledOp {
                    at: Duration::from_secs(2),
                    op: ChaosOp::KillLeader,
                },
                ScheduledOp {
                    at: Duration::from_secs(4),
                    op: ChaosOp::PauseLeader {
                        dur: Duration::from_millis(500),
                    },
                },
            ],
            loadgen_pause: None,
        };
        let json = serde_json::to_string(&s).unwrap();
        let parsed: Schedule = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.ops.len(), 2);
        match &parsed.source {
            ScheduleSource::Named { scenario } => assert_eq!(scenario, "killer-loop"),
            _ => panic!("wrong source"),
        }
    }

    #[test]
    fn schedule_with_loadgen_pause_round_trip() {
        let s = Schedule {
            source: ScheduleSource::Named {
                scenario: "burst".into(),
            },
            ops: vec![],
            loadgen_pause: Some(LoadgenPause {
                at: Duration::from_secs(30),
                dur: Duration::from_secs(5),
            }),
        };
        let json = serde_json::to_string(&s).unwrap();
        let parsed: Schedule = serde_json::from_str(&json).unwrap();
        let p = parsed.loadgen_pause.unwrap();
        assert_eq!(p.at, Duration::from_secs(30));
        assert_eq!(p.dur, Duration::from_secs(5));
    }
}
