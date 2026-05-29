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

//! Single-attempt dense GetSeq call and outcome classification. The dense path
//! is non-idempotent, so ambiguous post-send failures are surfaced (SeqUncertain)
//! rather than retried.

use crate::error::ClientError;

/// One contiguous dense block returned to the caller.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SeqBlock {
    pub start: u64,
    pub count: u32,
    pub epoch: u128,
}

/// Classification of a single GetSeq attempt.
/// Note: on success the retry loop returns the `SeqBlock` directly;
/// this enum only describes the non-success outcomes.
#[cfg_attr(test, derive(Debug))]
pub(crate) enum SeqAttemptOutcome {
    /// Pre-commit-certain: the leader refused before any durable advance. Safe to
    /// retry / follow the hint.
    LeaderHint {
        endpoint: String,
        epoch: Option<u128>,
    },
    /// Ambiguous post-send failure: surfaced, never silently retried.
    Uncertain,
    Err(ClientError),
}

/// Classify a failed GetSeq RPC. `sent` indicates the request reached the wire,
/// making a commit possible. A NOT_LEADER hint is decoded by the caller via the
/// shared leader_hint helper before this is reached; this handles the non-hint
/// statuses.
///
/// The dense path is non-idempotent, so the post-send (`sent == true`) default
/// is **fail-uncertain**: any status that is not *explicitly* known to be
/// pre-commit-certain is surfaced as [`SeqAttemptOutcome::Uncertain`] (never a
/// definitive, retry-safe error). The server can only emit a post-send
/// `INTERNAL` after attempting the durable fetch-add — the file driver maps a
/// post-rename directory-fsync failure to a permanent fault → `INTERNAL` — so
/// the advance may already be committed; treating it as retry-safe would let
/// the caller double-spend a block. Only these codes are pre-commit-certain by
/// construction and stay definitive even post-send:
///   * `InvalidArgument` — request validation, before any state is touched.
///   * `ResourceExhausted` — the per-key cardinality cap is checked before the
///     durable write.
///   * `Unimplemented` — the driver has no dense support and never advances.
///
/// `FailedPrecondition` (overflow / NOT_LEADER) is intercepted by the caller
/// before this function and is likewise pre-commit; it only reaches here with
/// `sent == false`, where it is surfaced as a definitive error. Connect-phase
/// transport failures never reach this function — the retry loop handles them
/// in its connect arm — so there is no pre-send "ride-out" outcome here.
pub(crate) fn classify_seq_status(status: tonic::Status, sent: bool) -> SeqAttemptOutcome {
    use tonic::Code;
    match status.code() {
        // Pre-commit-certain regardless of send → definitive.
        Code::InvalidArgument => SeqAttemptOutcome::Err(ClientError::InvalidSeqKey),
        Code::ResourceExhausted | Code::Unimplemented => {
            SeqAttemptOutcome::Err(ClientError::from(status))
        }
        // Post-send and not provably pre-commit → ambiguous (incl. INTERNAL,
        // UNKNOWN, ABORTED, and transport-class Unavailable/DeadlineExceeded).
        _ if sent => SeqAttemptOutcome::Uncertain,
        // Pre-send (only the bare-FailedPrecondition path passes `sent == false`)
        // → definitive.
        _ => SeqAttemptOutcome::Err(ClientError::from(status)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_unavailable_after_send_is_uncertain() {
        let status = tonic::Status::unavailable("connection reset");
        // `sent` = the request was put on the wire (post-send ambiguity).
        let outcome = classify_seq_status(status, true);
        assert!(matches!(outcome, SeqAttemptOutcome::Uncertain));
    }

    #[test]
    fn classify_unavailable_before_send_is_definitive() {
        // Connect-phase transport failures are handled by the retry loop's
        // connect arm, never here, so a transport status reaching this function
        // with `sent == false` (an unusual case) is surfaced as a definitive
        // error rather than a ride-out signal.
        let status = tonic::Status::unavailable("no connection established");
        let outcome = classify_seq_status(status, false);
        assert!(matches!(
            outcome,
            SeqAttemptOutcome::Err(ClientError::Rpc(_))
        ));
    }

    #[test]
    fn classify_internal_after_send_is_uncertain() {
        // INTERNAL is the server's mapping for a permanent driver fault, which
        // the file driver can emit *after* the durable rename (a failed dir
        // fsync) — the advance may already be committed. Post-send it is
        // therefore ambiguous and MUST surface as Uncertain, never a definitive
        // (retry-safe) error, or the caller could double-spend a block.
        let status = tonic::Status::internal("permanent driver fault");
        let outcome = classify_seq_status(status, true);
        assert!(
            matches!(outcome, SeqAttemptOutcome::Uncertain),
            "post-send INTERNAL must be Uncertain",
        );
    }

    #[test]
    fn classify_resource_exhausted_after_send_is_definitive() {
        // The cardinality cap is checked before any durable write, so
        // ResourceExhausted is pre-commit-certain even once the request reached
        // the wire — a definitive error, not Uncertain.
        let status = tonic::Status::resource_exhausted("cardinality cap reached");
        let outcome = classify_seq_status(status, true);
        assert!(
            matches!(outcome, SeqAttemptOutcome::Err(_)),
            "post-send ResourceExhausted is pre-commit-certain → definitive Err",
        );
    }

    #[test]
    fn classify_unimplemented_after_send_is_definitive() {
        // UNIMPLEMENTED means the driver has no dense support and rejected the
        // request before any durable advance — pre-commit-certain → definitive.
        let status = tonic::Status::unimplemented("dense sequences unsupported");
        let outcome = classify_seq_status(status, true);
        assert!(
            matches!(outcome, SeqAttemptOutcome::Err(_)),
            "post-send UNIMPLEMENTED is pre-commit-certain → definitive Err",
        );
    }
}
