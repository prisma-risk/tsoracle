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
