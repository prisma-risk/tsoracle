//! `RaftTypeConfig` for the openraft-backed driver.
//!
//! Built via `openraft_toolkit::declare_raft_types_ext!` so the type config
//! inherits the toolkit's defaults (`NodeId = u64`, `Term = u64`, `LeaderId =
//! leader_id_adv::LeaderId<u64, u64>`, `Responder = OneshotResponder`).

use std::io::Cursor;

use openraft_toolkit::declare_raft_types_ext;
use serde::{Deserialize, Serialize};

use crate::log_entry::HighWaterCommand;

/// Peer identity carried in the membership entries.
///
/// Currently holds just an address; richer metadata can be added later without
/// breaking the wire format because openraft serializes via bincode and the
/// `Default` impl gives missing fields a deterministic value.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpenraftPeer {
    pub addr: String,
}

/// Per-entry apply result.
///
/// Returned by the state machine for each replicated entry. The driver reads
/// `value` from the `client_write` response to confirm the committed value
/// observed by the apply pipeline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HighWaterApplied {
    /// The high-water value after this entry's apply.
    pub value: u64,
}

declare_raft_types_ext! {
    pub TypeConfig:
        Node            = OpenraftPeer,
        AppData         = HighWaterCommand,
        AppDataResponse = HighWaterApplied,
        SnapshotData    = Cursor<Vec<u8>>,
}
