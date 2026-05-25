//
//  ░▀█▀░█▀▀░█▀█░█▀▄░█▀█░█▀▀░█░░░█▀▀
//  ░░█░░▀▀█░█░█░█▀▄░█▀█░█░░░█░░░█▀▀
//  ░░▀░░▀▀▀░▀▀▀░▀░▀░▀░▀░▀▀▀░▀▀▀░▀▀▀
//
//  tsoracle — Distributed Timestamp Oracle
//  https://www.tsoracle.rs
//
//  Copyright (c) 2026 Prisma Risk
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

//! Chaos vocabulary: kinds, ops, windows, events.

use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

/// What the nemesis is asking the topology to do.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ChaosOp {
    KillLeader,
    PauseLeader {
        #[serde(with = "humantime_serde")]
        dur: Duration,
    },
    ArmFailpoint {
        name: String,
        action: String,
    },
    DisarmFailpoint {
        name: String,
    },
}

/// Categorical view of an in-progress chaos window. Mirrors `ChaosOp` but
/// drops the action body (which the supervisor doesn't need to gate
/// invariants).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ChaosKind {
    LeaderKill,
    LeaderPause,
    FailpointArm { name: String },
    FailpointDisarm { name: String },
}

/// One chaos window with three timestamps (see spec § "Chaos window timing").
#[derive(Debug, Clone)]
pub struct ChaosWindow {
    pub kind: ChaosKind,
    pub started_at: Instant,
    pub ended_at: Instant,
    pub grace: Duration,
}

impl ChaosWindow {
    /// Inclusive on `started_at`, exclusive at `ended_at + grace`.
    pub fn contains(&self, t: Instant) -> bool {
        t >= self.started_at && t < self.ended_at + self.grace
    }
}

/// Result of a `ChaosController` op. The op may complete cleanly, be skipped
/// (e.g., no current leader), or fail (the chaos primitive itself errored).
#[derive(Debug, Clone)]
pub enum ChaosOutcome {
    Applied,
    Skipped { reason: String },
    Failed { reason: String },
}

impl ChaosOutcome {
    pub fn is_applied(&self) -> bool {
        matches!(self, ChaosOutcome::Applied)
    }
}

/// What the nemesis pushes into the supervisor mpsc and into the report's
/// `chaos_events` vector.
#[derive(Debug, Clone)]
pub struct ChaosEvent {
    pub window: ChaosWindow,
    pub outcome: ChaosOutcome,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[test]
    fn chaos_window_contains_time() {
        let start = Instant::now();
        let ended = start + Duration::from_millis(100);
        let grace = Duration::from_millis(50);
        let w = ChaosWindow {
            kind: ChaosKind::LeaderKill,
            started_at: start,
            ended_at: ended,
            grace,
        };
        // Inside the window:
        assert!(w.contains(start));
        assert!(w.contains(start + Duration::from_millis(50)));
        // Inside the grace tail:
        assert!(w.contains(ended + Duration::from_millis(25)));
        // Past grace:
        assert!(!w.contains(ended + Duration::from_millis(60)));
        // Before window:
        assert!(!w.contains(start - Duration::from_millis(1)));
    }

    #[test]
    fn chaos_outcome_classification() {
        let applied = ChaosOutcome::Applied;
        let skipped = ChaosOutcome::Skipped {
            reason: "no leader".into(),
        };
        let failed = ChaosOutcome::Failed {
            reason: "spawn EBADF".into(),
        };
        assert!(applied.is_applied());
        assert!(!skipped.is_applied());
        assert!(!failed.is_applied());
    }
}
