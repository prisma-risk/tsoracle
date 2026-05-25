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
