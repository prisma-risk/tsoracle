//! Log entries replicated by the openraft cluster.
//!
//! The driver replicates a single command: advance the high-water mark to at
//! least `target`. The state machine treats this as `current = max(current,
//! target)`, which makes the operation idempotent under retries and monotone
//! under reordering — matching the [`tsoracle_consensus::ConsensusDriver`]
//! "advance to at least" contract.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Commands the state machine knows how to apply.
///
/// `Display` is implemented because openraft's `AppData` blanket requires it
/// (used in the `Entry`'s human-readable summary).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum HighWaterCommand {
    /// Advance the high-water mark to at least `target`. Idempotent.
    Bump { target: u64 },
}

impl fmt::Display for HighWaterCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HighWaterCommand::Bump { target } => write!(f, "Bump {{ target: {target} }}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_renders_bump() {
        let cmd = HighWaterCommand::Bump { target: 42 };
        assert_eq!(format!("{cmd}"), "Bump { target: 42 }");
    }

    #[test]
    fn display_renders_zero_target() {
        let cmd = HighWaterCommand::Bump { target: 0 };
        assert_eq!(format!("{cmd}"), "Bump { target: 0 }");
    }

    #[test]
    fn postcard_round_trip_bump() {
        let cmd = HighWaterCommand::Bump {
            target: 1_234_567_890,
        };
        let bytes = postcard::to_stdvec(&cmd).expect("serialize");
        let back: HighWaterCommand = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(back, cmd);
    }

    #[test]
    fn postcard_round_trip_zero() {
        let cmd = HighWaterCommand::Bump { target: 0 };
        let bytes = postcard::to_stdvec(&cmd).expect("serialize");
        let back: HighWaterCommand = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(back, cmd);
    }

    #[test]
    fn postcard_round_trip_max() {
        let cmd = HighWaterCommand::Bump { target: u64::MAX };
        let bytes = postcard::to_stdvec(&cmd).expect("serialize");
        let back: HighWaterCommand = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(back, cmd);
    }
}
