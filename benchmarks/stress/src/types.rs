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
