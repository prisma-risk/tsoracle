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

//! `RaftTypeConfig` for the openraft-backed driver.
//!
//! Built via `tsoracle_openraft_toolkit::declare_raft_types_ext!` so the type config
//! inherits the toolkit's defaults (`NodeId = u64`, `Term = u64`, `LeaderId =
//! leader_id_adv::LeaderId<u64, u64>`, `Responder = OneshotResponder`).

use std::io::Cursor;

use serde::{Deserialize, Serialize};
use tsoracle_openraft_toolkit::declare_raft_types_ext;

use crate::log_entry::HighWaterCommand;

/// Peer identity carried in the membership entries.
///
/// Holds two stable addresses, mirroring etcd's peerURLs/clientURLs split:
///
/// * `addr` — the raft transport address, a scheme-less `host:port` (a DNS name
///   and port). The peer transport prepends `http://` itself, so a scheme here
///   would double up.
/// * `service_endpoint` — the tsoracle gRPC endpoint clients redirect to, as a scheme-less `host:port`. The client applies its own transport scheme (`https://` under TLS, `http://` otherwise); an explicit `http://` here is refused by a TLS client as a downgrade, so leave the scheme off. Empty means "no client redirect".
///
/// Widening this struct changes the postcard layout of membership log entries
/// and therefore requires a `tsoracle_openraft_toolkit::SCHEMA_VERSION` bump.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpenraftPeer {
    pub addr: String,
    pub service_endpoint: String,
}

/// Resolves a membership `Node`'s tsoracle client endpoint for `LeaderHint`
/// follower redirects. Implemented by every `Node` type used with
/// [`crate::OpenraftDriver`]; return `None` for nodes that do not redirect
/// tsoracle clients.
pub trait ServiceEndpoint {
    fn service_endpoint(&self) -> Option<&str>;
}

impl ServiceEndpoint for OpenraftPeer {
    fn service_endpoint(&self) -> Option<&str> {
        if self.service_endpoint.is_empty() {
            None
        } else {
            Some(&self.service_endpoint)
        }
    }
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

/// The concrete raft log entry for this type config, as decoded from the log
/// column family on recovery and replication.
///
/// Exposed only so the fuzz harness can decode the full `[version | postcard]`
/// log record (`log_store::decode_record`) against the real `Entry` shape —
/// including the variable-length `EntryPayload::Membership` variant that
/// decoding the inner [`HighWaterCommand`] alone never reaches. Hidden from the
/// public API because nothing else should depend on the entry representation.
#[doc(hidden)]
pub type OpenraftEntry = openraft::type_config::alias::EntryOf<TypeConfig>;

/// The concrete `Vote` and `LogId` types the log store reads back from the meta
/// column family on recovery.
///
/// Like the log and snapshot records, the meta singletons are persisted under
/// the `[SCHEMA_VERSION | postcard]` frame (the meta column was brought under it
/// in `SCHEMA_VERSION` v2) — see `log_store::meta::read`, which routes
/// `VoteOf<C>` (label `Vote`) and `LogIdOf<C>` (labels `Committed` and
/// `LastPurged`) through `log_store::decode_record`, so a foreign version
/// loud-rejects instead of silently misdecoding the recovery-critical vote.
/// Exposed only so the fuzz harness can reconstruct that framed decode
/// (`tsoracle_openraft_toolkit::decode::<_>(SCHEMA_VERSION, ..)`) against the
/// concrete meta types; hidden from the public API because nothing else should
/// depend on the meta representation.
#[doc(hidden)]
pub type OpenraftVote = openraft::type_config::alias::VoteOf<TypeConfig>;

/// See [`OpenraftVote`]. The `LogId` read back for the `Committed` and
/// `LastPurged` meta labels.
#[doc(hidden)]
pub type OpenraftLogId = openraft::type_config::alias::LogIdOf<TypeConfig>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn openraft_peer_round_trips() {
        let peer = OpenraftPeer {
            addr: "10.0.0.1:50051".to_string(),
            service_endpoint: String::new(),
        };
        let bytes = postcard::to_stdvec(&peer).expect("serialize");
        let back: OpenraftPeer = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(back, peer);
    }

    #[test]
    fn openraft_peer_default_has_all_empty_fields() {
        let peer = OpenraftPeer::default();
        assert_eq!(peer.addr, "");
        assert_eq!(peer.service_endpoint, "");
    }

    #[test]
    fn openraft_peer_round_trips_empty_addr() {
        let peer = OpenraftPeer {
            addr: String::new(),
            service_endpoint: String::new(),
        };
        let bytes = postcard::to_stdvec(&peer).expect("serialize");
        let back: OpenraftPeer = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(back, peer);
    }

    #[test]
    fn openraft_peer_pins_field_layout() {
        // Pins the postcard field order (addr, then service_endpoint) that
        // membership log entries embed. A reorder or inserted field changes
        // these bytes and trips this test, guarding the membership record
        // layout the SCHEMA_VERSION frame versions. postcard encodes each
        // String as a varint length prefix followed by its UTF-8 bytes.
        let peer = OpenraftPeer {
            addr: "a:1".into(),
            service_endpoint: "b:2".into(),
        };
        let bytes = postcard::to_stdvec(&peer).expect("serialize");
        assert_eq!(bytes, vec![3, b'a', b':', b'1', 3, b'b', b':', b'2']);
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

    #[test]
    fn openraft_peer_round_trips_both_fields() {
        let peer = OpenraftPeer {
            addr: "node-1:50052".to_string(),
            service_endpoint: "http://node-1:50051".to_string(),
        };
        let bytes = postcard::to_stdvec(&peer).expect("serialize");
        let back: OpenraftPeer = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(back, peer);
    }

    #[test]
    fn service_endpoint_is_some_when_set_none_when_empty() {
        use crate::type_config::ServiceEndpoint;
        let with = OpenraftPeer {
            addr: "node-1:50052".to_string(),
            service_endpoint: "http://node-1:50051".to_string(),
        };
        assert_eq!(with.service_endpoint(), Some("http://node-1:50051"));

        let without = OpenraftPeer {
            addr: "node-1:50052".to_string(),
            service_endpoint: String::new(),
        };
        assert_eq!(without.service_endpoint(), None);
    }
}
