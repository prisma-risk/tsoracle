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

use serde::{Deserialize, Serialize};
use tsoracle_core::PeerEndpoint;
use tsoracle_openraft_toolkit::declare_raft_types_ext;

use crate::log_entry::HighWaterCommand;

/// Peer identity carried in the membership entries.
///
/// Holds three stable addresses (etcd's peerURLs/clientURLs split, plus an
/// admin URL):
///
/// * `addr` — the raft transport address, a scheme-less `host:port`.
/// * `service_endpoint` — the tsoracle gRPC endpoint clients redirect to, a
///   scheme-less `host:port`. Empty means "no client redirect".
/// * `admin_endpoint` — the membership-admin gRPC endpoint the `tsoracle admin`
///   CLI redirects to when it reaches a follower, a scheme-less `host:port`.
///   Empty means "no admin redirect available for this node".
///
/// Widening this struct changes the postcard layout of membership log entries
/// and therefore requires growing `tsoracle_openraft_toolkit::MAX_READABLE_VERSION`
/// and advancing `BASELINE_WRITE_VERSION` through an activation barrier (the
/// historical fix was a global stop-the-world bump; the format-migration
/// design replaces that with a rolling upgrade).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpenraftPeer {
    pub addr: String,
    pub service_endpoint: String,
    pub admin_endpoint: String,
}

/// Resolves a membership `Node`'s tsoracle endpoints for follower redirects
/// (`service_endpoint`) and admin-plane requests (`admin_endpoint`).
/// Implemented by every `Node` type used with [`crate::OpenraftDriver`];
/// return `None` for nodes that do not expose the corresponding plane.
///
/// **Return-type rationale:** these methods *parse* the stored representation
/// into a typed [`PeerEndpoint`] on each call. The underlying field on a
/// concrete `Node` (e.g. [`OpenraftPeer`]) stays `String` to preserve the
/// postcard wire format used in openraft membership log entries; the typed
/// projection lives at the read boundary, identical to how `LeaderHint`'s
/// wire `String` is parsed by `tsoracle_client::leader_hint`. A malformed
/// or empty value silently maps to `None` — matching the historical
/// `is_empty -> None` sentinel — rather than panicking, so a misconfigured
/// peer never crashes the leader-watch task.
pub trait ServiceEndpoint {
    fn service_endpoint(&self) -> Option<PeerEndpoint>;
    fn admin_endpoint(&self) -> Option<PeerEndpoint>;
}

impl ServiceEndpoint for OpenraftPeer {
    fn service_endpoint(&self) -> Option<PeerEndpoint> {
        PeerEndpoint::try_from(self.service_endpoint.clone()).ok()
    }
    fn admin_endpoint(&self) -> Option<PeerEndpoint> {
        PeerEndpoint::try_from(self.admin_endpoint.clone()).ok()
    }
}

/// What an applied entry did, beyond moving the high-water value.
///
/// Returned on every [`HighWaterApplied`] so the proposer's `client_write`
/// response proves the apply's semantic outcome — specifically whether a
/// [`crate::log_entry::SetFormatVersionPayload`] bump took effect or applied
/// as a membership-subset no-op. A bare `Ok` from `client_write` proves the
/// entry committed and applied, but not that the subset check passed; the
/// leader's activation flip is keyed off observing
/// [`ApplyOutcome::FormatActivated`] here — apply-keyed, never commit-keyed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ApplyOutcome {
    /// A `Blank`, `Advance`, or `Membership` entry (the active write version
    /// is untouched).
    Advanced,
    /// A `SetFormatVersion` bump applied successfully: the membership at the
    /// entry's log position was a subset of the gated set, so the shared
    /// active-write-version cell was set to `target`. NO meta key was written.
    FormatActivated { target: u8 },
    /// A `SetFormatVersion` bump applied as a no-op: the membership at the
    /// entry's log position was NOT a subset of the gated set, so the shared
    /// cell was left untouched and `target` did not take effect. The operator
    /// re-gates and re-issues.
    FormatActivationNoop { target: u8 },
    /// A `SetFormatVersion` bump applied as a no-op for the apply arm's
    /// defense-in-depth range check: `target` was outside the LOCAL
    /// binary's `[MIN_READABLE_VERSION, MAX_READABLE_VERSION]` so the
    /// shared cell was left untouched. Distinct from
    /// [`ApplyOutcome::FormatActivationNoop`] because the failure class is "this entry
    /// should never have been proposed" (a gate bug or a log committed
    /// by an older binary), not "the membership drifted" — different
    /// remediation, different counter, different operator surface. The
    /// gate at proposal time normally prevents this from being committed
    /// (see `target_in_local_readable_range`).
    FormatActivationTargetOutOfRange { target: u8 },
    /// An `AdvanceDense` applied successfully: `start` is the pre-advance dense
    /// counter for the key (the issued block's first ordinal). `value` on the
    /// enclosing `HighWaterApplied` is the high-water, untouched.
    DenseAdvanced { start: u64 },
    /// An `AdvanceDense` applied as a deterministic rejection: the key was new
    /// and the cluster is at its immutable genesis cardinality cap.
    DenseCardinalityExceeded { cap: u64 },
    /// An `AdvanceDense` applied as a deterministic rejection: the counter would
    /// exceed u64::MAX.
    DenseOverflow,
    /// An `AdvanceDenseBatch` applied successfully: `starts[i]` is the
    /// pre-advance dense counter for request entry `i` (its block's first
    /// ordinal), in request order. `value` on the enclosing `HighWaterApplied`
    /// is the high-water, untouched.
    DenseBatchAdvanced { starts: Vec<u64> },
    /// An `AdvanceDenseBatch` applied as a deterministic rejection: applying it
    /// would exceed the immutable genesis cardinality cap. No counter moved.
    DenseBatchCardinalityExceeded { cap: u64 },
    /// An `AdvanceDenseBatch` applied as a deterministic rejection: some key's
    /// accumulated advance would exceed u64::MAX. No counter moved.
    DenseBatchOverflow,
}

/// Per-entry apply result.
///
/// Returned by the state machine for each replicated entry. The driver reads
/// `value` from the `client_write` response to confirm the committed value
/// observed by the apply pipeline; `outcome` proves which apply path ran
/// (specifically, whether a [`crate::log_entry::HighWaterCommand::SetFormatVersion`]
/// bump took effect or applied as a no-op).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HighWaterApplied {
    /// The high-water value after this entry's apply.
    pub value: u64,
    /// The semantic outcome of this entry's apply (see [`ApplyOutcome`]).
    pub outcome: ApplyOutcome,
}

declare_raft_types_ext! {
    pub TypeConfig:
        Node            = OpenraftPeer,
        AppData         = HighWaterCommand,
        AppDataResponse = HighWaterApplied,
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
/// the `[active write version | postcard]` frame (the meta column was brought
/// under it in v2) — see `log_store::meta::read`, which routes `VoteOf<C>`
/// (label `Vote`) and `LogIdOf<C>` (labels `Committed` and `LastPurged`)
/// through the toolkit's record helpers, so an out-of-range version
/// loud-rejects instead of silently misdecoding the recovery-critical vote.
/// Exposed only so the fuzz harness can reconstruct that framed decode
/// (`tsoracle_openraft_toolkit::decode::<_>(BASELINE_WRITE_VERSION, ..)`)
/// against the concrete meta types; hidden from the public API because nothing
/// else should depend on the meta representation.
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
            admin_endpoint: "10.0.0.1:50053".to_string(),
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
        assert_eq!(peer.admin_endpoint, "");
    }

    #[test]
    fn openraft_peer_round_trips_empty_addr() {
        let peer = OpenraftPeer {
            addr: String::new(),
            service_endpoint: String::new(),
            admin_endpoint: String::new(),
        };
        let bytes = postcard::to_stdvec(&peer).expect("serialize");
        let back: OpenraftPeer = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(back, peer);
    }

    #[test]
    fn openraft_peer_pins_field_layout() {
        // Pins the postcard field order (addr, service_endpoint, admin_endpoint)
        // that membership log entries embed. A reorder or inserted field changes
        // these bytes and trips this test, guarding the membership record layout
        // the active write version frame versions. postcard encodes each String
        // as a
        // varint length prefix followed by its UTF-8 bytes.
        let peer = OpenraftPeer {
            addr: "a:1".into(),
            service_endpoint: "b:2".into(),
            admin_endpoint: "c:3".into(),
        };
        let bytes = postcard::to_stdvec(&peer).expect("serialize");
        assert_eq!(
            bytes,
            vec![
                3, b'a', b':', b'1', 3, b'b', b':', b'2', 3, b'c', b':', b'3'
            ]
        );
    }

    #[test]
    fn high_water_applied_round_trips() {
        let applied = HighWaterApplied {
            value: 12_345,
            outcome: ApplyOutcome::Advanced,
        };
        let bytes = postcard::to_stdvec(&applied).expect("serialize");
        let back: HighWaterApplied = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(back, applied);
    }

    #[test]
    fn high_water_applied_round_trips_zero_and_max() {
        for v in [0u64, u64::MAX] {
            let applied = HighWaterApplied {
                value: v,
                outcome: ApplyOutcome::Advanced,
            };
            let bytes = postcard::to_stdvec(&applied).expect("serialize");
            let back: HighWaterApplied = postcard::from_bytes(&bytes).expect("deserialize");
            assert_eq!(back, applied);
        }
    }

    #[test]
    fn apply_outcome_variants_round_trip() {
        for outcome in [
            ApplyOutcome::Advanced,
            ApplyOutcome::FormatActivated { target: 4 },
            ApplyOutcome::FormatActivationNoop { target: 4 },
            ApplyOutcome::FormatActivationTargetOutOfRange { target: 1 },
            ApplyOutcome::DenseAdvanced { start: 99 },
            ApplyOutcome::DenseCardinalityExceeded { cap: 1024 },
            ApplyOutcome::DenseOverflow,
            ApplyOutcome::DenseBatchAdvanced {
                starts: vec![0, 1, u64::MAX],
            },
            ApplyOutcome::DenseBatchCardinalityExceeded { cap: 1024 },
            ApplyOutcome::DenseBatchOverflow,
        ] {
            let applied = HighWaterApplied {
                value: 42,
                outcome: outcome.clone(),
            };
            let bytes = postcard::to_stdvec(&applied).expect("serialize");
            let back: HighWaterApplied = postcard::from_bytes(&bytes).expect("deserialize");
            assert_eq!(back, applied);
        }
    }

    #[test]
    fn apply_outcome_distinguishes_success_from_noop() {
        let success = ApplyOutcome::FormatActivated { target: 5 };
        let noop = ApplyOutcome::FormatActivationNoop { target: 5 };
        assert_ne!(success, noop);
        assert!(matches!(
            success,
            ApplyOutcome::FormatActivated { target: 5 }
        ));
        assert!(matches!(
            noop,
            ApplyOutcome::FormatActivationNoop { target: 5 }
        ));
    }

    #[test]
    fn openraft_peer_round_trips_all_fields() {
        let peer = OpenraftPeer {
            addr: "node-1:50052".to_string(),
            service_endpoint: "http://node-1:50051".to_string(),
            admin_endpoint: "node-1:50053".to_string(),
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
            service_endpoint: "node-1:50051".to_string(),
            admin_endpoint: String::new(),
        };
        assert_eq!(
            with.service_endpoint(),
            Some(PeerEndpoint::try_from("node-1:50051").unwrap()),
        );
        // An empty admin_endpoint is mapped to None.
        assert_eq!(with.admin_endpoint(), None);

        let without = OpenraftPeer {
            addr: "node-1:50052".to_string(),
            service_endpoint: String::new(),
            admin_endpoint: String::new(),
        };
        assert_eq!(without.service_endpoint(), None);
        assert_eq!(without.admin_endpoint(), None);
    }

    #[test]
    fn service_endpoint_silently_drops_malformed() {
        // A persisted-then-recovered scheme-bearing endpoint (e.g. carried
        // forward from a pre-newtype configuration) must not crash the
        // leader-watch task; it is treated as "no redirect".
        use crate::type_config::ServiceEndpoint;
        let bad = OpenraftPeer {
            addr: "node-1:50052".to_string(),
            service_endpoint: "http://node-1:50051".to_string(),
            admin_endpoint: "https://node-1:50053".to_string(),
        };
        assert_eq!(bad.service_endpoint(), None);
        assert_eq!(bad.admin_endpoint(), None);
    }
}
