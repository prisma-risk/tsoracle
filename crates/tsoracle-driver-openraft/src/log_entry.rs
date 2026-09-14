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

// #[PerformanceCriticalPath]
//! Log entries replicated by the openraft cluster.
//!
//! The driver replicates a single command: advance the high-water mark to at
//! least [`AdvancePayload::at_least`]. The state machine treats this as
//! `current = max(current, at_least)`, which makes the operation idempotent
//! under retries and monotone under reordering — matching the
//! [`tsoracle_consensus::ConsensusDriver`] "advance to at least" contract. The
//! payload is the cross-backend [`tsoracle_consensus::AdvancePayload`], shared
//! with the paxos driver so the "advance" command carries one name and one
//! field across backends.

use std::collections::BTreeSet;
use std::fmt;

use serde::{Deserialize, Serialize};
use tsoracle_consensus::AdvancePayload;

/// Payload of [`HighWaterCommand::SetFormatVersion`]: the committed activation
/// barrier that flips the cluster's active write version.
///
/// `gated_members` is the exact member set (voters ∪ learners) the leader's
/// activation gate observed as capable of reading `target` at proposal time.
/// It travels inside the entry because the state-machine apply cannot perform
/// a live capability query; apply re-validates that the membership committed
/// as of this entry's own log position is a subset of `gated_members` before
/// taking effect. `NodeId` is `u64` (the type config's `Node` id type), so the
/// set is `BTreeSet<u64>` — a deterministic, ordered, postcard-stable layout.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetFormatVersionPayload {
    /// The active write version to flip to on a successful (non-no-op) apply.
    pub target: u8,
    /// Voters ∪ learners the leader gated as `target`-readable at proposal time.
    pub gated_members: BTreeSet<u64>,
}

/// One durable lease, as replicated in [`HighWaterCommand::SetLeases`] and carried in the state-machine snapshot.
///
/// A frozen, driver-local mirror of [`tsoracle_core::LeaseRecord`], which carries no serde. Field order is the postcard layout and must not change. The holder length is re-validated on every decode, like the embedded `SeqKey` of the dense commands, so a malformed record fails loud instead of landing in replicated state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "DurableLeaseWire")]
pub struct DurableLease {
    lease_id: u64,
    holder: Vec<u8>,
    holder_epoch: u64,
    ttl_ms: u64,
    ts_upper_bound: u64,
    expires_at_ms: u64,
    superseded: bool,
}

/// Unvalidated decode shape of [`DurableLease`]; identical field order, so the postcard layout is the same.
#[derive(Deserialize)]
struct DurableLeaseWire {
    lease_id: u64,
    holder: Vec<u8>,
    holder_epoch: u64,
    ttl_ms: u64,
    ts_upper_bound: u64,
    expires_at_ms: u64,
    superseded: bool,
}

/// A lease set or lease record that cannot be replicated.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LeaseSetError {
    #[error("lease {lease_id} holder must be 1..={max} bytes, got {len}")]
    HolderLen {
        lease_id: u64,
        len: usize,
        max: usize,
    },
    #[error("lease set must be strictly ordered by lease id, found {previous} before {next}")]
    Unordered { previous: u64, next: u64 },
}

impl TryFrom<DurableLeaseWire> for DurableLease {
    type Error = LeaseSetError;

    fn try_from(wire: DurableLeaseWire) -> Result<Self, Self::Error> {
        let len = wire.holder.len();
        if len == 0 || len > tsoracle_core::MAX_LEASE_HOLDER_LEN {
            return Err(LeaseSetError::HolderLen {
                lease_id: wire.lease_id,
                len,
                max: tsoracle_core::MAX_LEASE_HOLDER_LEN,
            });
        }
        Ok(DurableLease {
            lease_id: wire.lease_id,
            holder: wire.holder,
            holder_epoch: wire.holder_epoch,
            ttl_ms: wire.ttl_ms,
            ts_upper_bound: wire.ts_upper_bound,
            expires_at_ms: wire.expires_at_ms,
            superseded: wire.superseded,
        })
    }
}

impl TryFrom<&tsoracle_core::LeaseRecord> for DurableLease {
    type Error = LeaseSetError;

    fn try_from(record: &tsoracle_core::LeaseRecord) -> Result<Self, Self::Error> {
        DurableLease::try_from(DurableLeaseWire {
            lease_id: record.lease_id,
            holder: record.holder.clone(),
            holder_epoch: record.holder_epoch,
            ttl_ms: record.ttl_ms,
            ts_upper_bound: record.ts_upper_bound,
            expires_at_ms: record.expires_at_ms,
            superseded: record.superseded,
        })
    }
}

impl From<&DurableLease> for tsoracle_core::LeaseRecord {
    fn from(lease: &DurableLease) -> Self {
        tsoracle_core::LeaseRecord {
            lease_id: lease.lease_id,
            holder: lease.holder.clone(),
            holder_epoch: lease.holder_epoch,
            ttl_ms: lease.ttl_ms,
            ts_upper_bound: lease.ts_upper_bound,
            expires_at_ms: lease.expires_at_ms,
            superseded: lease.superseded,
        }
    }
}

impl DurableLease {
    /// The lease id, which orders a [`LeaseSet`].
    pub fn lease_id(&self) -> u64 {
        self.lease_id
    }
}

/// The complete durable lease set, strictly ordered by lease id.
///
/// The ordering is canonical so every replica encodes the same bytes for the same set, and it is re-validated on decode together with each record. An empty set is the state before any lease is granted and the state a v4 to v6 snapshot lifts into.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Vec<DurableLease>")]
pub struct LeaseSet(Vec<DurableLease>);

impl TryFrom<Vec<DurableLease>> for LeaseSet {
    type Error = LeaseSetError;

    fn try_from(leases: Vec<DurableLease>) -> Result<Self, Self::Error> {
        for pair in leases.windows(2) {
            if pair[0].lease_id >= pair[1].lease_id {
                return Err(LeaseSetError::Unordered {
                    previous: pair[0].lease_id,
                    next: pair[1].lease_id,
                });
            }
        }
        Ok(LeaseSet(leases))
    }
}

impl LeaseSet {
    /// Canonicalize a server-supplied live set: validate each record and order by lease id. Duplicate ids are refused rather than silently collapsed.
    pub fn from_records(records: &[tsoracle_core::LeaseRecord]) -> Result<Self, LeaseSetError> {
        let mut leases = records
            .iter()
            .map(DurableLease::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        leases.sort_by_key(|lease| lease.lease_id);
        LeaseSet::try_from(leases)
    }

    /// The set as core lease records, in lease-id order.
    pub fn to_records(&self) -> Vec<tsoracle_core::LeaseRecord> {
        self.0
            .iter()
            .map(tsoracle_core::LeaseRecord::from)
            .collect()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

/// Payload of [`HighWaterCommand::SetLeases`]: replace the durable lease set, but only if the entry commits in the term the proposer projected the set in.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetLeasesPayload {
    /// The raft term the server's leader epoch named when it projected `leases`. Apply compares it with the term of the entry's own log id.
    pub expected_term: u64,
    /// The complete live set to install.
    pub leases: LeaseSet,
}

/// One `(key, count)` entry of an [`HighWaterCommand::AdvanceDenseBatch`]. A
/// named struct (not a tuple) so the postcard layout is stable and pinnable and
/// the embedded `SeqKey` revalidates on decode exactly like single-key
/// `AdvanceDense`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DenseAdvance {
    pub key: tsoracle_core::SeqKey,
    pub count: u32,
}

/// Commands the state machine knows how to apply.
///
/// `Display` is implemented because openraft's `AppData` blanket requires it
/// (used in the `Entry`'s human-readable summary).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum HighWaterCommand {
    /// Advance the high-water mark to at least `at_least`. Idempotent.
    Advance(AdvancePayload),
    /// Activate a new active write version, gated on the carried member set.
    /// Applies as a no-op if the membership at this entry's log position is
    /// not a subset of `gated_members`.
    SetFormatVersion(SetFormatVersionPayload),
    /// Advance the dense counter for `key` by `count`, lazily creating the key
    /// at 0. The pre-advance value is the issued block's start; it is returned
    /// to the proposer via [`ApplyOutcome`](crate::ApplyOutcome) (the apply path computes it in
    /// committed log order). Only ever appended under write version
    /// >= DENSE_WRITE_VERSION (gated by activation).
    AdvanceDense {
        key: tsoracle_core::SeqKey,
        count: u32,
    },
    /// Atomically advance several distinct dense counters in one entry. Applied
    /// all-or-nothing: a cardinality or overflow rejection moves no counter (see
    /// the state-machine apply). Each entry's pre-advance value is the issued
    /// block's start, returned in request order via
    /// [`ApplyOutcome::DenseBatchAdvanced`](crate::ApplyOutcome). Only ever
    /// appended under write version >= BATCH_WRITE_VERSION (gated by activation).
    AdvanceDenseBatch { entries: Vec<DenseAdvance> },
    /// Replace the durable lease set wholesale. Applies only when the entry's log id carries `expected_term`; otherwise it is a no-op reported as [`ApplyOutcome::LeasesFenced`](crate::ApplyOutcome). The apply-time `max` that fences a stale high-water advance cannot fence a full-set replacement, so the term check is what keeps a set projected under an old leadership from overwriting a newer one. Only ever appended under write version >= LEASE_WRITE_VERSION (gated by activation).
    SetLeases(SetLeasesPayload),
}

impl fmt::Display for HighWaterCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HighWaterCommand::Advance(AdvancePayload { at_least }) => {
                write!(f, "Advance {{ at_least: {at_least} }}")
            }
            HighWaterCommand::SetFormatVersion(SetFormatVersionPayload {
                target,
                gated_members,
            }) => {
                let rendered: Vec<String> = gated_members.iter().map(|id| id.to_string()).collect();
                write!(
                    f,
                    "SetFormatVersion {{ target: {target}, gated_members: [{}] }}",
                    rendered.join(", ")
                )
            }
            HighWaterCommand::AdvanceDense { key, count } => {
                write!(
                    f,
                    "AdvanceDense {{ key: {}, count: {count} }}",
                    key.as_str()
                )
            }
            HighWaterCommand::AdvanceDenseBatch { entries } => {
                let rendered: Vec<String> = entries
                    .iter()
                    .map(|e| format!("{} x{}", e.key.as_str(), e.count))
                    .collect();
                write!(
                    f,
                    "AdvanceDenseBatch {{ entries: [{}] }}",
                    rendered.join(", ")
                )
            }
            // Holders are opaque bytes of up to 128 bytes each; the summary names the term and the set size only.
            HighWaterCommand::SetLeases(SetLeasesPayload {
                expected_term,
                leases,
            }) => {
                write!(
                    f,
                    "SetLeases {{ expected_term: {expected_term}, leases: {} }}",
                    leases.len()
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_renders_advance() {
        let cmd = HighWaterCommand::Advance(AdvancePayload { at_least: 42 });
        assert_eq!(format!("{cmd}"), "Advance { at_least: 42 }");
    }

    #[test]
    fn display_renders_zero_at_least() {
        let cmd = HighWaterCommand::Advance(AdvancePayload { at_least: 0 });
        assert_eq!(format!("{cmd}"), "Advance { at_least: 0 }");
    }

    #[test]
    fn postcard_round_trip_advance() {
        let cmd = HighWaterCommand::Advance(AdvancePayload {
            at_least: 1_234_567_890,
        });
        let bytes = postcard::to_stdvec(&cmd).expect("serialize");
        let back: HighWaterCommand = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(back, cmd);
    }

    #[test]
    fn postcard_round_trip_zero() {
        let cmd = HighWaterCommand::Advance(AdvancePayload { at_least: 0 });
        let bytes = postcard::to_stdvec(&cmd).expect("serialize");
        let back: HighWaterCommand = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(back, cmd);
    }

    #[test]
    fn postcard_round_trip_max() {
        let cmd = HighWaterCommand::Advance(AdvancePayload { at_least: u64::MAX });
        let bytes = postcard::to_stdvec(&cmd).expect("serialize");
        let back: HighWaterCommand = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(back, cmd);
    }

    #[test]
    fn display_renders_set_format_version() {
        let cmd = HighWaterCommand::SetFormatVersion(SetFormatVersionPayload {
            target: 4,
            gated_members: BTreeSet::from([1u64, 2u64, 3u64]),
        });
        assert_eq!(
            format!("{cmd}"),
            "SetFormatVersion { target: 4, gated_members: [1, 2, 3] }"
        );
    }

    #[test]
    fn postcard_round_trip_set_format_version() {
        let cmd = HighWaterCommand::SetFormatVersion(SetFormatVersionPayload {
            target: 7,
            gated_members: BTreeSet::from([10u64, 20u64]),
        });
        let bytes = postcard::to_stdvec(&cmd).expect("serialize");
        let back: HighWaterCommand = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(back, cmd);
    }

    #[test]
    fn postcard_round_trip_set_format_version_empty_gate() {
        // The gated set may legitimately be empty for a degenerate re-issue;
        // it must still round-trip so the apply-time subset check sees
        // exactly what the proposer recorded.
        let cmd = HighWaterCommand::SetFormatVersion(SetFormatVersionPayload {
            target: 4,
            gated_members: BTreeSet::new(),
        });
        let bytes = postcard::to_stdvec(&cmd).expect("serialize");
        let back: HighWaterCommand = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(back, cmd);
    }

    #[test]
    fn postcard_round_trip_advance_dense() {
        // A valid `AdvanceDense` round-trips unchanged: routing the embedded
        // `SeqKey` decode through `try_new` must not perturb the honest path.
        let cmd = HighWaterCommand::AdvanceDense {
            key: tsoracle_core::SeqKey::try_new("orders").unwrap(),
            count: 7,
        };
        let bytes = postcard::to_stdvec(&cmd).expect("serialize");
        let back: HighWaterCommand = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(back, cmd);
    }

    #[test]
    fn advance_dense_postcard_layout_is_pinned() {
        // Pin the byte layout the hand-crafted invalid-key payloads below rely
        // on: `AdvanceDense` is the 3rd `HighWaterCommand` variant (index 2),
        // followed by the newtype-transparent `SeqKey` (a postcard `String`:
        // varint length prefix + UTF-8 bytes) then the `count` varint. A
        // 1-byte key "a" with count 1 is therefore [2, 1, b'a', 1].
        let cmd = HighWaterCommand::AdvanceDense {
            key: tsoracle_core::SeqKey::try_new("a").unwrap(),
            count: 1,
        };
        let bytes = postcard::to_stdvec(&cmd).expect("serialize");
        assert_eq!(bytes, vec![2u8, 1, b'a', 1]);
    }

    #[test]
    fn decode_rejects_advance_dense_empty_key() {
        // Variant 2 (AdvanceDense), empty-string key (length prefix 0), count 1.
        // The bytes are structurally valid postcard, but the embedded `SeqKey`
        // violates the non-empty invariant, so the decode must fail loud rather
        // than land `SeqKey("")` in a replicated command.
        let bytes = vec![2u8, 0, 1];
        let decoded = postcard::from_bytes::<HighWaterCommand>(&bytes);
        assert!(
            decoded.is_err(),
            "AdvanceDense with empty key must fail to decode, got {decoded:?}",
        );
    }

    #[test]
    fn decode_rejects_advance_dense_oversized_key() {
        // Variant 2, a 129-byte key (one past MAX_SEQ_KEY_LEN), count 1. 129 as
        // a postcard varint is [0x81, 0x01].
        let mut bytes = vec![2u8, 0x81, 0x01];
        bytes.extend(std::iter::repeat_n(b'a', 129));
        bytes.push(1);
        let decoded = postcard::from_bytes::<HighWaterCommand>(&bytes);
        assert!(
            decoded.is_err(),
            "AdvanceDense with oversized key must fail to decode, got {decoded:?}",
        );
    }

    #[test]
    fn display_renders_advance_dense_batch() {
        let cmd = HighWaterCommand::AdvanceDenseBatch {
            entries: vec![
                DenseAdvance {
                    key: tsoracle_core::SeqKey::try_new("orders").unwrap(),
                    count: 3,
                },
                DenseAdvance {
                    key: tsoracle_core::SeqKey::try_new("users").unwrap(),
                    count: 5,
                },
            ],
        };
        assert_eq!(
            format!("{cmd}"),
            "AdvanceDenseBatch { entries: [orders x3, users x5] }"
        );
    }

    #[test]
    fn postcard_round_trip_advance_dense_batch() {
        let cmd = HighWaterCommand::AdvanceDenseBatch {
            entries: vec![DenseAdvance {
                key: tsoracle_core::SeqKey::try_new("orders").unwrap(),
                count: 7,
            }],
        };
        let bytes = postcard::to_stdvec(&cmd).expect("serialize");
        let back: HighWaterCommand = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(back, cmd);
    }

    #[test]
    fn advance_dense_batch_postcard_layout_is_pinned() {
        // AdvanceDenseBatch is the 4th HighWaterCommand variant (index 3),
        // followed by a postcard seq (varint len prefix) of DenseAdvance, each
        // = SeqKey (String: varint len + bytes) then count varint. One entry
        // {"a", 1} is therefore [3, 1, 1, b'a', 1]:
        //   3 = variant index, 1 = vec len, 1 = key len, b'a' = key, 1 = count.
        let cmd = HighWaterCommand::AdvanceDenseBatch {
            entries: vec![DenseAdvance {
                key: tsoracle_core::SeqKey::try_new("a").unwrap(),
                count: 1,
            }],
        };
        let bytes = postcard::to_stdvec(&cmd).expect("serialize");
        assert_eq!(bytes, vec![3u8, 1, 1, b'a', 1]);
    }

    #[test]
    fn decode_rejects_advance_dense_batch_empty_key() {
        // Variant 3, vec len 1, key len 0 (empty), count 1. Structurally valid
        // postcard, but the embedded SeqKey violates the non-empty invariant,
        // so decode must fail loud rather than land SeqKey("") in a command.
        let bytes = vec![3u8, 1, 0, 1];
        let decoded = postcard::from_bytes::<HighWaterCommand>(&bytes);
        assert!(
            decoded.is_err(),
            "AdvanceDenseBatch with empty key must fail to decode, got {decoded:?}",
        );
    }

    fn lease(lease_id: u64, holder: &[u8]) -> tsoracle_core::LeaseRecord {
        tsoracle_core::LeaseRecord {
            lease_id,
            holder: holder.to_vec(),
            holder_epoch: 2,
            ttl_ms: 20_000,
            ts_upper_bound: lease_id,
            expires_at_ms: lease_id + 20_000,
            superseded: false,
        }
    }

    #[test]
    fn set_leases_postcard_layout_is_pinned() {
        // SetLeases is the 5th HighWaterCommand variant (index 4). The payload is expected_term (varint), then the lease vec: length, then each record's lease_id, holder (length + bytes), holder_epoch, ttl_ms, ts_upper_bound, expires_at_ms varints and the superseded flag. The decode-rejection tests below edit these exact bytes.
        let cmd = HighWaterCommand::SetLeases(SetLeasesPayload {
            expected_term: 3,
            leases: LeaseSet::from_records(&[tsoracle_core::LeaseRecord {
                lease_id: 1,
                holder: b"a".to_vec(),
                holder_epoch: 2,
                ttl_ms: 5,
                ts_upper_bound: 1,
                expires_at_ms: 6,
                superseded: false,
            }])
            .unwrap(),
        });
        let bytes = postcard::to_stdvec(&cmd).expect("serialize");
        assert_eq!(bytes, vec![4u8, 3, 1, 1, 1, b'a', 2, 5, 1, 6, 0]);
    }

    #[test]
    fn postcard_round_trip_set_leases() {
        let cmd = HighWaterCommand::SetLeases(SetLeasesPayload {
            expected_term: 11,
            leases: LeaseSet::from_records(&[
                lease(300, &[0xff; tsoracle_core::MAX_LEASE_HOLDER_LEN]),
                lease(100, b"g1"),
            ])
            .unwrap(),
        });
        let bytes = postcard::to_stdvec(&cmd).expect("serialize");
        let back: HighWaterCommand = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(back, cmd);
    }

    #[test]
    fn display_renders_set_leases_without_holders() {
        let cmd = HighWaterCommand::SetLeases(SetLeasesPayload {
            expected_term: 9,
            leases: LeaseSet::from_records(&[lease(1, b"g1"), lease(2, b"g2")]).unwrap(),
        });
        assert_eq!(
            format!("{cmd}"),
            "SetLeases { expected_term: 9, leases: 2 }"
        );
    }

    #[test]
    fn decode_rejects_set_leases_empty_holder() {
        // The pinned layout with the holder length zeroed and the holder byte removed.
        let bytes = vec![4u8, 3, 1, 1, 0, 2, 5, 1, 6, 0];
        let decoded = postcard::from_bytes::<HighWaterCommand>(&bytes);
        assert!(
            decoded.is_err(),
            "empty holder must fail to decode, got {decoded:?}"
        );
    }

    #[test]
    fn decode_rejects_set_leases_oversized_holder() {
        // One byte past MAX_LEASE_HOLDER_LEN; 129 as a varint is [0x81, 0x01].
        let mut bytes = vec![4u8, 3, 1, 1, 0x81, 0x01];
        bytes.extend(std::iter::repeat_n(
            b'a',
            tsoracle_core::MAX_LEASE_HOLDER_LEN + 1,
        ));
        bytes.extend([2, 5, 1, 6, 0]);
        let decoded = postcard::from_bytes::<HighWaterCommand>(&bytes);
        assert!(
            decoded.is_err(),
            "oversized holder must fail to decode, got {decoded:?}"
        );
    }

    #[test]
    fn decode_rejects_set_leases_out_of_order_or_duplicate_ids() {
        // Two records with ids 2 then 1, and two with id 1 twice: neither is the canonical strictly increasing order every replica must agree on.
        for ids in [[2u8, 1], [1, 1]] {
            let mut bytes = vec![4u8, 3, 2];
            for id in ids {
                bytes.extend([id, 1, b'a', 2, 5, id, 6, 0]);
            }
            let decoded = postcard::from_bytes::<HighWaterCommand>(&bytes);
            assert!(
                decoded.is_err(),
                "lease ids {ids:?} must fail to decode, got {decoded:?}"
            );
        }
    }

    #[test]
    fn lease_set_from_records_orders_and_validates() {
        let set = LeaseSet::from_records(&[lease(300, b"g3"), lease(100, b"g1")]).unwrap();
        assert_eq!(set.to_records(), vec![lease(100, b"g1"), lease(300, b"g3")]);

        assert_eq!(
            LeaseSet::from_records(&[lease(100, b"g1"), lease(100, b"g2")]),
            Err(LeaseSetError::Unordered {
                previous: 100,
                next: 100
            })
        );
        assert_eq!(
            LeaseSet::from_records(&[lease(100, b"")]),
            Err(LeaseSetError::HolderLen {
                lease_id: 100,
                len: 0,
                max: tsoracle_core::MAX_LEASE_HOLDER_LEN
            })
        );
    }
}
