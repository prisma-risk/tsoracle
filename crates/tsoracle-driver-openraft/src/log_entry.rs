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
}
