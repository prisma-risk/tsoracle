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

//! Per-RPC samples emitted by loadgen.

use std::time::Instant;

use tsoracle_core::Timestamp;

use crate::types::{BatchId, ClientId};

/// One timestamp the supervisor needs to observe.
///
/// One `IssuedSample` per `Timestamp` returned by a successful RPC. For a
/// `GetTsBatch(N)` call, the loadgen task emits N samples sharing
/// `(client_id, batch_id)`, with `batch_idx` 0..N-1 and `is_last = true` on
/// the last one.
#[derive(Debug, Clone)]
pub struct IssuedSample {
    pub client_id: ClientId,
    pub batch_id: BatchId,
    pub batch_idx: u32,
    pub is_last: bool,
    pub ts: Timestamp,
    /// When loadgen began issuing the call (start of the RPC).
    pub issued_at: Instant,
    /// When loadgen received this timestamp (end of the RPC).
    pub recv_time: Instant,
}

/// A liveness-relevant event: either the client exceeded its deadline budget
/// outside any chaos window, or the topology layer reaped an unexpected
/// server exit. Both flow through the same supervisor mpsc.
#[derive(Debug, Clone)]
pub struct LivenessIncident {
    pub kind: LivenessIncidentKind,
    pub at: Instant,
}

#[derive(Debug, Clone)]
pub enum LivenessIncidentKind {
    DeadlineExceeded {
        client_id: ClientId,
        attempts: u32,
        last_error: String,
        started_at: Instant,
    },
    UnexpectedServerExit {
        pid: u32,
        last_log_lines: Vec<String>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ClientId;
    use std::time::Instant;
    use tsoracle_core::Timestamp;

    #[test]
    fn issued_sample_clone_and_field_access() {
        let now = Instant::now();
        let s = IssuedSample {
            client_id: ClientId(7),
            batch_id: 42,
            batch_idx: 3,
            is_last: false,
            ts: Timestamp(0xdead_beef),
            issued_at: now,
            recv_time: now,
        };
        let s2 = s.clone();
        assert_eq!(s.client_id.0, 7);
        assert_eq!(s2.batch_id, 42);
        assert!(!s2.is_last);
    }

    #[test]
    fn liveness_incident_kind_variants_compile() {
        let now = Instant::now();
        let _a = LivenessIncident {
            kind: LivenessIncidentKind::DeadlineExceeded {
                client_id: ClientId(0),
                attempts: 5,
                last_error: "Unavailable".into(),
                started_at: now,
            },
            at: now,
        };
        let _b = LivenessIncident {
            kind: LivenessIncidentKind::UnexpectedServerExit {
                pid: 12345,
                last_log_lines: vec!["panic".into()],
            },
            at: now,
        };
    }
}
