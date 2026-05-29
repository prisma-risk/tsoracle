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
    /// A reachable peer reported no leader yet (an absent-hint NOT_LEADER). The
    /// original server status is carried so the retry loop can surface the
    /// server's own message rather than a synthetic one.
    NoLeaderYet(tonic::Status),
    /// Ambiguous post-send failure: surfaced, never silently retried.
    Uncertain,
    Err(ClientError),
}

/// Classify a failed GetSeq RPC. `sent` indicates the request reached the wire,
/// making a commit possible (post-send ambiguity → Uncertain). A NOT_LEADER
/// hint is decoded by the caller via the shared leader_hint helper before this
/// is reached; this handles the non-hint statuses.
pub(crate) fn classify_seq_status(status: tonic::Status, sent: bool) -> SeqAttemptOutcome {
    use tonic::Code;
    match status.code() {
        Code::Unavailable | Code::DeadlineExceeded if sent => SeqAttemptOutcome::Uncertain,
        Code::Unavailable | Code::DeadlineExceeded => SeqAttemptOutcome::NoLeaderYet(status),
        Code::InvalidArgument => SeqAttemptOutcome::Err(ClientError::InvalidSeqKey),
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
    fn classify_unavailable_before_send_is_no_leader_yet() {
        let status = tonic::Status::unavailable("no connection established");
        let outcome = classify_seq_status(status, false);
        assert!(matches!(outcome, SeqAttemptOutcome::NoLeaderYet(_)));
    }
}
