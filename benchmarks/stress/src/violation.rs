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
    /// Non-overlapping cross-client real-time monotonicity: this sample's RPC
    /// began strictly after another client's RPC had already completed, yet it
    /// received a `ts` no greater than that completed RPC's. Unlike
    /// `FenceFreshness` (scoped to a chaos window's post-grace tail), this fires
    /// in steady state — see the comment block in `supervisor::on_issued`.
    CrossClientRealtimeMonotonicity {
        prior_completed_ts: Timestamp,
        got: Timestamp,
        sample: IssuedSample,
    },
    Liveness {
        incident: LivenessIncident,
    },
}
