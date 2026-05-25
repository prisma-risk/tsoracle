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

//! Errors returned by [`ConsensusDriver`](crate::ConsensusDriver) operations.

use tsoracle_core::Epoch;

/// Errors returned by `ConsensusDriver` operations.
///
/// Driver implementations classify their internal failures into one of
/// `TransientDriver` or `PermanentDriver`. The server uses that classification
/// directly to pick a gRPC status code:
///
/// | Variant            | gRPC code            | Client expectation         |
/// |--------------------|----------------------|----------------------------|
/// | `NotLeader`        | `FAILED_PRECONDITION` + `LeaderHint` | Retry against the new leader |
/// | `Fenced`           | `FAILED_PRECONDITION` + `LeaderHint` | Retry against the new leader |
/// | `TransientDriver`  | `UNAVAILABLE`        | Safe to retry              |
/// | `PermanentDriver`  | `INTERNAL`           | Do NOT silently retry      |
#[derive(Debug, thiserror::Error)]
pub enum ConsensusError {
    #[error("not leader (current epoch: {observed:?})")]
    NotLeader { observed: Option<Epoch> },
    #[error("epoch fenced: expected {expected:?}, current {current:?}")]
    Fenced { expected: Epoch, current: Epoch },
    /// A driver-level failure the caller MAY retry. Use for errors that are
    /// reasonably expected to clear on their own: storage I/O hiccup, peer
    /// transport flap, transient quorum loss.
    #[error("transient driver error: {0}")]
    TransientDriver(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// A driver-level failure the caller MUST NOT silently retry. Use for
    /// persistent local fault: read-only filesystem, corruption, gone
    /// storage device, bad driver implementation, invariant violation.
    #[error("permanent driver error: {0}")]
    PermanentDriver(#[source] Box<dyn std::error::Error + Send + Sync>),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consensus_error_display_text() {
        let not_leader = ConsensusError::NotLeader {
            observed: Some(Epoch(3)),
        };
        assert!(not_leader.to_string().contains("not leader"));

        let fenced = ConsensusError::Fenced {
            expected: Epoch(2),
            current: Epoch(5),
        };
        let fenced_text = fenced.to_string();
        assert!(fenced_text.contains("fenced"));
        assert!(fenced_text.contains('2'));
        assert!(fenced_text.contains('5'));

        let transient = ConsensusError::TransientDriver(Box::new(std::io::Error::other("flap")));
        assert!(transient.to_string().contains("transient"));
        assert!(transient.to_string().contains("flap"));

        let permanent = ConsensusError::PermanentDriver(Box::new(std::io::Error::other("corrupt")));
        assert!(permanent.to_string().contains("permanent"));
        assert!(permanent.to_string().contains("corrupt"));
    }
}
