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
/// breaking the wire format because the codec is length-prefixed and the
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn openraft_peer_round_trips() {
        let peer = OpenraftPeer {
            addr: "10.0.0.1:50051".to_string(),
        };
        let bytes = postcard::to_stdvec(&peer).expect("serialize");
        let back: OpenraftPeer = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(back, peer);
    }

    #[test]
    fn openraft_peer_default_is_empty_addr() {
        let peer = OpenraftPeer::default();
        assert_eq!(peer.addr, "");
    }

    #[test]
    fn openraft_peer_round_trips_empty_addr() {
        let peer = OpenraftPeer { addr: String::new() };
        let bytes = postcard::to_stdvec(&peer).expect("serialize");
        let back: OpenraftPeer = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(back, peer);
    }

    #[test]
    fn high_water_applied_round_trips() {
        let applied = HighWaterApplied { value: 12_345 };
        let bytes = postcard::to_stdvec(&applied).expect("serialize");
        let back: HighWaterApplied = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(back, applied);
    }

    #[test]
    fn high_water_applied_round_trips_zero_and_max() {
        for v in [0u64, u64::MAX] {
            let applied = HighWaterApplied { value: v };
            let bytes = postcard::to_stdvec(&applied).expect("serialize");
            let back: HighWaterApplied = postcard::from_bytes(&bytes).expect("deserialize");
            assert_eq!(back, applied);
        }
    }
}
