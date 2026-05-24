//
//  ░▀█▀░█▀▀░█▀█░█▀▄░█▀█░█▀▀░█░░░█▀▀
//  ░░█░░▀▀█░█░█░█▀▄░█▀█░█░░░█░░░█▀▀
//  ░░▀░░▀▀▀░▀▀▀░▀░▀░▀░▀░▀▀▀░▀▀▀░▀▀▀
//
//  tsoracle — Distributed Timestamp Oracle
//
//  Copyright (c) 2026 Prisma Risk
//  Licensed under the Apache License, Version 2.0
//  https://github.com/prisma-risk/tsoracle
//

// #[PerformanceCriticalPath]
//! Log entries replicated by the openraft cluster.
//!
//! The driver replicates a single command: advance the high-water mark to at
//! least [`AdvancePayload::at_least`]. The state machine treats this as
//! `current = max(current, at_least)`, which makes the operation idempotent
//! under retries and monotone under reordering — matching the
//! [`tsoracle_consensus::ConsensusDriver`] "advance to at least" contract. The
//! payload is the cross-backend [`tsoracle_consensus::AdvancePayload`], shared
//! with the paxos driver so the "advance" command carries one name and one
//! field across backends.

use std::fmt;

use serde::{Deserialize, Serialize};
use tsoracle_consensus::AdvancePayload;

/// Commands the state machine knows how to apply.
///
/// `Display` is implemented because openraft's `AppData` blanket requires it
/// (used in the `Entry`'s human-readable summary).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum HighWaterCommand {
    /// Advance the high-water mark to at least `at_least`. Idempotent.
    Advance(AdvancePayload),
}

impl fmt::Display for HighWaterCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HighWaterCommand::Advance(AdvancePayload { at_least }) => {
                write!(f, "Advance {{ at_least: {at_least} }}")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_renders_advance() {
        let cmd = HighWaterCommand::Advance(AdvancePayload { at_least: 42 });
        assert_eq!(format!("{cmd}"), "Advance { at_least: 42 }");
    }

    #[test]
    fn display_renders_zero_at_least() {
        let cmd = HighWaterCommand::Advance(AdvancePayload { at_least: 0 });
        assert_eq!(format!("{cmd}"), "Advance { at_least: 0 }");
    }

    #[test]
    fn postcard_round_trip_advance() {
        let cmd = HighWaterCommand::Advance(AdvancePayload {
            at_least: 1_234_567_890,
        });
        let bytes = postcard::to_stdvec(&cmd).expect("serialize");
        let back: HighWaterCommand = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(back, cmd);
    }

    #[test]
    fn postcard_round_trip_zero() {
        let cmd = HighWaterCommand::Advance(AdvancePayload { at_least: 0 });
        let bytes = postcard::to_stdvec(&cmd).expect("serialize");
        let back: HighWaterCommand = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(back, cmd);
    }

    #[test]
    fn postcard_round_trip_max() {
        let cmd = HighWaterCommand::Advance(AdvancePayload { at_least: u64::MAX });
        let bytes = postcard::to_stdvec(&cmd).expect("serialize");
        let back: HighWaterCommand = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(back, cmd);
    }
}
