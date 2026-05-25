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

//! The high-water advance command payload and its range guard.

use crate::error::ConsensusError;

/// The payload of an "advance the high-water to at least `at_least`" command,
/// shared by every consensus backend's replicated log entry.
///
/// Each backend's `HighWaterCommand` newtype-wraps this in its `Advance`
/// variant, so the "advance" semantics carry one name and one field across
/// backends. Apply semantics are `current = max(current, at_least)`, which
/// makes the command idempotent under retries and monotone under reordering —
/// matching the [`ConsensusDriver::persist_high_water`] "advance to at least"
/// contract.
///
/// `serde` is feature-gated to match the crate's optional-serde design; the
/// drivers that persist this enable `tsoracle-consensus`'s `serde` feature.
///
/// [`ConsensusDriver::persist_high_water`]: crate::ConsensusDriver::persist_high_water
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AdvancePayload {
    pub at_least: u64,
}

impl AdvancePayload {
    /// Apply this advance to a `current` high-water, returning the new value.
    ///
    /// This is the single home for the monotonic apply rule — `max(current,
    /// at_least)` — that every backend's state machine runs when it applies a
    /// decided `Advance`. Folding the rule into one named method (rather than
    /// re-spelling the `if at_least > current` comparison at each apply site)
    /// gives the "advance only ratchets up" invariant one test and one place to
    /// reason about its idempotence and reorder-independence.
    #[must_use]
    pub fn merge(self, current: u64) -> u64 {
        current.max(self.at_least)
    }
}

/// The error carried by a rejected out-of-range high-water advance. See
/// [`reject_out_of_range_advance`].
#[derive(Debug, thiserror::Error)]
#[error("high-water advance {0} exceeds the 46-bit physical_ms maximum")]
pub struct AdvanceOutOfRange(pub u64);

/// Reject a high-water advance whose `physical_ms` value exceeds
/// `tsoracle_core::PHYSICAL_MS_MAX`, before it is durably persisted.
///
/// `persist_high_water` carries a value in the `physical_ms` domain, which must
/// fit the 46-bit timestamp layout. A value above the cap is a *poison*: once
/// durably committed it can never be served — every subsequent leadership gain
/// reloads it and the allocator's `try_on_leadership_gained` rejects it
/// (`CoreError::PhysicalMsOutOfRange`), so the new leader can never serve. The
/// high-water only ratchets up, so it cannot self-heal.
///
/// Every `ConsensusDriver::persist_high_water` MUST call this before appending
/// the advance to durable storage (the consensus log, the on-disk record), so
/// an out-of-range value is rejected at the propose boundary and never
/// persisted. The single-node `FileDriver` already guards at persist time; this
/// is the shared check that keeps the consensus backends — which append through
/// a replicated log and apply with an unchecked `max` — from drifting away from
/// that contract.
///
/// Classified [`ConsensusError::PermanentDriver`]: an out-of-range advance
/// signals a saturated or misconfigured clock (`SystemClock::now_ms` saturates
/// to `u64::MAX` to surface a far-future clock visibly), not a transient
/// condition, so the server must surface `INTERNAL` rather than silently retry.
pub fn reject_out_of_range_advance(at_least: u64) -> Result<(), ConsensusError> {
    if at_least > tsoracle_core::PHYSICAL_MS_MAX {
        return Err(ConsensusError::PermanentDriver(Box::new(
            AdvanceOutOfRange(at_least),
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "serde")]
    #[test]
    fn advance_payload_postcard_pins_at_least_varint() {
        // Both backends embed this payload after their `Advance` variant tag,
        // so its on-the-wire shape is a cross-backend contract: a bare varint
        // of `at_least`, with no struct framing of its own.
        let payload = AdvancePayload { at_least: 5 };
        let bytes = postcard::to_stdvec(&payload).expect("serialize");
        assert_eq!(bytes, vec![5]);
        let back: AdvancePayload = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(back, payload);
    }

    #[cfg(feature = "serde")]
    #[test]
    fn advance_payload_postcard_round_trips_extremes() {
        for at_least in [0u64, 1, u64::MAX] {
            let payload = AdvancePayload { at_least };
            let bytes = postcard::to_stdvec(&payload).expect("serialize");
            let back: AdvancePayload = postcard::from_bytes(&bytes).expect("deserialize");
            assert_eq!(back, payload);
        }
    }

    #[test]
    fn merge_is_monotone_under_reordering_and_retries() {
        // The single home for the high-water apply rule: `current = max(current,
        // at_least)`. A higher advance ratchets the value up; a stale or
        // duplicate advance (at or below `current`) is absorbed without moving
        // it, which is exactly what makes the command idempotent and
        // order-independent across every backend's apply path.
        assert_eq!(
            AdvancePayload { at_least: 7 }.merge(3),
            7,
            "higher advance ratchets up"
        );
        assert_eq!(
            AdvancePayload { at_least: 3 }.merge(7),
            7,
            "stale advance is absorbed"
        );
        assert_eq!(
            AdvancePayload { at_least: 5 }.merge(5),
            5,
            "equal advance is idempotent"
        );
        assert_eq!(
            AdvancePayload { at_least: 0 }.merge(0),
            0,
            "zero from a fresh state stays zero"
        );
        assert_eq!(
            AdvancePayload { at_least: u64::MAX }.merge(0),
            u64::MAX,
            "the extreme advance ratchets to the maximum",
        );
    }

    #[test]
    fn reject_out_of_range_advance_accepts_up_to_the_cap() {
        // The cap itself is a valid physical_ms; only values strictly above it
        // are rejected.
        reject_out_of_range_advance(0).expect("zero is in range");
        reject_out_of_range_advance(tsoracle_core::PHYSICAL_MS_MAX)
            .expect("the maximum physical_ms is in range");
    }

    #[test]
    fn reject_out_of_range_advance_rejects_above_the_cap_as_permanent() {
        for at_least in [tsoracle_core::PHYSICAL_MS_MAX + 1, u64::MAX] {
            let err = reject_out_of_range_advance(at_least)
                .expect_err("a value above the cap must be rejected");
            match err {
                ConsensusError::PermanentDriver(source) => {
                    let downcast = source
                        .downcast_ref::<AdvanceOutOfRange>()
                        .expect("permanent driver error must carry AdvanceOutOfRange");
                    assert_eq!(downcast.0, at_least);
                }
                other => panic!("expected PermanentDriver, got {other:?}"),
            }
        }
    }
}
