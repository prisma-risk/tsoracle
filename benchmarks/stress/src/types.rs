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

//! Identifier types used across the harness.

use serde::{Deserialize, Serialize};

/// Per-task client identifier. Assigned by the loadgen pool at task spawn;
/// zero-based, dense, stable for the run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ClientId(pub u32);

/// Loadgen-side batch correlator. Each client task increments its own counter
/// per `GetTs` / `GetTsBatch` call. Not a server-side identifier; pure harness
/// bookkeeping for the batch-internal-ordering invariant.
pub type BatchId = u32;
