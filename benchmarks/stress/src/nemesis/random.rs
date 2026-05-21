//! Seeded random scheduler.

use std::time::Duration;

use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;

use crate::chaos::ChaosOp;
use crate::schedule::{RandomParams, Schedule, ScheduleSource, ScheduledOp};

/// Build a schedule from `(seed, params)`. Deterministic given the seed.
pub fn build(seed: u64, params: RandomParams) -> Schedule {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut ops = Vec::new();
    let total = params.total;
    let mean_gap_ms = (params.mean_gap.as_millis().max(1)) as f64;
    let mut t = Duration::ZERO;
    let total_weight = params.weight_kill + params.weight_pause + params.weight_failpoint;
    while t < total {
        // Exponential inter-arrival: -ln(U) * mean.
        let u: f64 = rng.random::<f64>().max(1e-9);
        let gap_ms = -u.ln() * mean_gap_ms;
        t += Duration::from_millis(gap_ms as u64);
        if t > total {
            break;
        }
        // Weighted op selection.
        let pick: f64 = rng.random::<f64>() * total_weight;
        let op = if pick < params.weight_kill {
            ChaosOp::KillLeader
        } else if pick < params.weight_kill + params.weight_pause {
            let dur_ms = rng.random_range(50..500);
            ChaosOp::PauseLeader {
                dur: Duration::from_millis(dur_ms),
            }
        } else {
            let names = [
                "tsoracle::driver_file::write_record::after_fsync",
                "tsoracle::driver_file::write_record::before_fsync",
            ];
            let name = names[rng.random_range(0..names.len())];
            ChaosOp::ArmFailpoint {
                name: name.into(),
                action: "panic".into(),
            }
        };
        ops.push(ScheduledOp { at: t, op });
    }
    let total = params.total;
    Schedule {
        source: ScheduleSource::Random { seed, params },
        ops,
        total,
        loadgen_pause: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn standard_params(total: Duration) -> crate::schedule::RandomParams {
        crate::schedule::RandomParams {
            mean_gap: Duration::from_millis(500),
            total,
            weight_kill: 1.0,
            weight_pause: 1.0,
            weight_failpoint: 0.5,
        }
    }

    #[test]
    fn same_seed_produces_same_schedule() {
        let p = standard_params(Duration::from_secs(10));
        let a = build(42, p.clone());
        let b = build(42, p);
        assert_eq!(a.ops.len(), b.ops.len());
        for (oa, ob) in a.ops.iter().zip(b.ops.iter()) {
            assert_eq!(oa.at, ob.at);
            assert_eq!(
                std::mem::discriminant(&oa.op),
                std::mem::discriminant(&ob.op)
            );
        }
    }

    #[test]
    fn different_seed_produces_different_schedule() {
        let p = standard_params(Duration::from_secs(10));
        let a = build(1, p.clone());
        let b = build(2, p);
        assert!(
            a.ops.len() != b.ops.len()
                || a.ops
                    .iter()
                    .zip(b.ops.iter())
                    .any(|(oa, ob)| oa.at != ob.at),
            "seeds 1 vs 2 produced identical schedules"
        );
    }

    #[test]
    fn schedule_stays_within_total() {
        let total = Duration::from_secs(5);
        let p = standard_params(total);
        let s = build(7, p);
        for op in &s.ops {
            assert!(op.at <= total, "op at {:?} > total {:?}", op.at, total);
        }
    }
}
