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

//! Invariant violation records.

use std::time::Instant;

use tsoracle_core::Timestamp;

use crate::sample::{IssuedSample, LivenessIncident};
use crate::types::{BatchId, ClientId};

#[derive(Debug, Clone)]
pub struct Violation {
    pub kind: ViolationKind,
    pub at: Instant,
}

#[derive(Debug, Clone)]
pub enum ViolationKind {
    Monotonicity {
        prev: Timestamp,
        got: Timestamp,
        sample: IssuedSample,
    },
    BatchInternalOrdering {
        client_id: ClientId,
        batch_id: BatchId,
        values: Vec<Timestamp>,
        detail: String,
    },
    FenceFreshness {
        pre_window_high_water: Timestamp,
        first_post_window_ts: Timestamp,
        window_kind: crate::chaos::ChaosKind,
    },
    Liveness {
        incident: LivenessIncident,
    },
}
