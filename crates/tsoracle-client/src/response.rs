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

//! Decode a `GetTsResponse` into a validated [`TimestampRange`] with the
//! protocol-level safety checks the retry loop used to perform inline.
//!
//! All checks (count match, range, physical range, logical overflow) are moved
//! verbatim from `lib.rs::issue_rpc` and surface the same
//! `ClientError::Rpc(tonic::Status::internal(...))` errors. The check
//! ordering is preserved where it matters: count match first, then count
//! range and logical overflow, then per-timestamp packing.

use tsoracle_core::{LOGICAL_MAX, Timestamp};
use tsoracle_proto::v1::GetTsResponse;

use crate::MAX_TIMESTAMPS_PER_RPC;
use crate::error::ClientError;

/// A compact, validated view of a `GetTsResponse`'s contiguous timestamp
/// range: the three wire fields (`physical_ms`, `logical_start`, `count`)
/// instead of an expanded `Vec<Timestamp>`. A maxed-out chunk stays ~16 bytes
/// from decode through coalescing and delivery rather than ballooning into
/// ~2 MB; the `Vec<Timestamp>` is materialized only per waiter, in
/// `driver::deliver`, at the public-API boundary.
///
/// Valid-by-construction: the only non-test producer is
/// [`decode_get_ts_response`], which validates `physical_ms` and the full
/// logical span before constructing the range. That invariant is what lets
/// [`TimestampRange::iter`] pack with the infallible [`Timestamp::pack`].
#[derive(Clone, Copy, Debug)]
pub(crate) struct TimestampRange {
    physical_ms: u64,
    logical_start: u32,
    count: u32,
}

impl TimestampRange {
    /// Construct a range from its wire fields. The caller must ensure the
    /// fields are in range — `physical_ms <= PHYSICAL_MS_MAX` and
    /// `logical_start + count - 1 <= LOGICAL_MAX`. `decode_get_ts_response`
    /// validates this before calling; test stubs pass known-good values.
    pub(crate) fn new(physical_ms: u64, logical_start: u32, count: u32) -> Self {
        TimestampRange {
            physical_ms,
            logical_start,
            count,
        }
    }

    /// Number of timestamps in the range.
    pub(crate) fn count(&self) -> u32 {
        self.count
    }

    /// Iterate the `count` contiguous timestamps, packing each on demand.
    /// Uses the infallible [`Timestamp::pack`]: the range is
    /// valid-by-construction, so every `(physical_ms, logical_start + i)` is
    /// in range and cannot panic.
    pub(crate) fn iter(&self) -> impl Iterator<Item = Timestamp> {
        let physical_ms = self.physical_ms;
        let logical_start = self.logical_start;
        (0..self.count).map(move |i| Timestamp::pack(physical_ms, logical_start + i))
    }
}

pub(crate) fn decode_get_ts_response(
    resp: GetTsResponse,
    requested: u32,
) -> Result<TimestampRange, ClientError> {
    // Defense in depth: a buggy or malicious server could return fields that
    // do not fit the packed timestamp layout. Catch them before constructing
    // any Timestamp so invalid wire data cannot panic or truncate.
    if resp.count != requested {
        return Err(ClientError::Rpc(tonic::Status::internal(format!(
            "server returned count={} for requested count={}",
            resp.count, requested
        ))));
    }
    if resp.count == 0 || resp.count > MAX_TIMESTAMPS_PER_RPC {
        return Err(ClientError::Rpc(tonic::Status::internal(format!(
            "server returned out-of-range count={}",
            resp.count
        ))));
    }
    let last_logical = resp.logical_start.checked_add(resp.count.saturating_sub(1));
    match last_logical {
        Some(last) if last <= LOGICAL_MAX => {}
        _ => {
            return Err(ClientError::Rpc(tonic::Status::internal(format!(
                "server returned logical_start={} + count={} that overflows \
                 LOGICAL_MAX={}",
                resp.logical_start, resp.count, LOGICAL_MAX
            ))));
        }
    }
    // Validate the packing invariants once, on the range's start endpoint.
    // The logical-overflow check above already bounds
    // `logical_start + count - 1 <= LOGICAL_MAX` (and `count >= 1`, so
    // `logical_start <= LOGICAL_MAX`), so a single successful pack proves
    // `physical_ms` fits the 46-bit field and the whole contiguous range
    // packs — no per-element loop, no `count`-sized `Vec`.
    Timestamp::try_pack(resp.physical_ms, resp.logical_start).map_err(|e| {
        ClientError::Rpc(tonic::Status::internal(format!(
            "server returned invalid timestamp fields: {e}"
        )))
    })?;
    Ok(TimestampRange::new(
        resp.physical_ms,
        resp.logical_start,
        resp.count,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use tsoracle_core::PHYSICAL_MS_MAX;

    #[test]
    fn timestamp_range_iter_yields_contiguous_packed_timestamps() {
        let range = TimestampRange::new(1_000, 5, 3);
        assert_eq!(range.count(), 3);
        let timestamps: Vec<Timestamp> = range.iter().collect();
        assert_eq!(
            timestamps,
            vec![
                Timestamp::pack(1_000, 5),
                Timestamp::pack(1_000, 6),
                Timestamp::pack(1_000, 7),
            ],
        );
    }

    #[test]
    fn rejects_out_of_range_physical_ms() {
        let err = decode_get_ts_response(
            GetTsResponse {
                physical_ms: PHYSICAL_MS_MAX + 1,
                logical_start: 0,
                count: 1,
                epoch_hi: 0,
                epoch_lo: 0,
            },
            1,
        )
        .unwrap_err();

        assert!(matches!(err, ClientError::Rpc(_)));
    }

    #[test]
    fn rejects_zero_count_when_requested_zero() {
        // The Client crate's public surface caps `count` at MAX_TIMESTAMPS_PER_RPC
        // and rejects 0 up-front, so `decode_get_ts_response` would not normally
        // see these values. The defense-in-depth count-range guard here catches
        // a server bug or wire tamper that bypasses that outer check.
        let err = decode_get_ts_response(
            GetTsResponse {
                physical_ms: 1,
                logical_start: 0,
                count: 0,
                epoch_hi: 0,
                epoch_lo: 0,
            },
            0,
        )
        .unwrap_err();
        let ClientError::Rpc(status) = err else {
            panic!("expected Rpc, got something else");
        };
        assert!(status.message().contains("out-of-range count=0"));
    }

    #[test]
    fn rejects_oversized_count_when_requested_oversized() {
        let oversized = MAX_TIMESTAMPS_PER_RPC + 1;
        let err = decode_get_ts_response(
            GetTsResponse {
                physical_ms: 1,
                logical_start: 0,
                count: oversized,
                epoch_hi: 0,
                epoch_lo: 0,
            },
            oversized,
        )
        .unwrap_err();
        let ClientError::Rpc(status) = err else {
            panic!("expected Rpc, got something else");
        };
        assert!(status.message().contains("out-of-range"));
    }

    /// Strategy producing a `GetTsResponse` that satisfies every precondition
    /// `decode_get_ts_response` checks: count in `[1, MAX_TIMESTAMPS_PER_RPC]`,
    /// physical_ms in 46-bit range, and `logical_start + count - 1 <= LOGICAL_MAX`.
    /// Tests using this strategy are exercising the success path only.
    fn well_formed_response() -> impl Strategy<Value = GetTsResponse> {
        (
            0u64..=PHYSICAL_MS_MAX,
            1u32..=MAX_TIMESTAMPS_PER_RPC,
            any::<u64>(),
        )
            .prop_flat_map(|(physical_ms, count, epoch)| {
                let max_logical_start = (LOGICAL_MAX + 1).saturating_sub(count);
                (0u32..=max_logical_start).prop_map(move |logical_start| GetTsResponse {
                    physical_ms,
                    logical_start,
                    count,
                    epoch_hi: 0,
                    epoch_lo: epoch,
                })
            })
    }

    proptest! {
        // Negative case: when the wire-level count disagrees with what we
        // requested, decode must reject regardless of other fields. This guards
        // against a server bug or wire tamper that returns the wrong batch size.
        #[test]
        fn rejects_count_mismatch(
            response in well_formed_response(),
            requested in 1u32..=MAX_TIMESTAMPS_PER_RPC,
        ) {
            prop_assume!(response.count != requested);
            prop_assert!(matches!(
                decode_get_ts_response(response, requested),
                Err(ClientError::Rpc(_))
            ));
        }

        // Negative case: any response whose logical range would cross the 18-bit
        // wall must be rejected. The decoder's check is `logical_start + count - 1
        // <= LOGICAL_MAX`; this asserts the boundary holds for arbitrary overflowing
        // pairs.
        #[test]
        fn rejects_logical_overflow(
            physical_ms in 0u64..=PHYSICAL_MS_MAX,
            logical_start in 1u32..=LOGICAL_MAX,
            count in 1u32..=MAX_TIMESTAMPS_PER_RPC,
        ) {
            prop_assume!((logical_start as u64) + (count as u64) > (LOGICAL_MAX as u64) + 1);
            let response =
                GetTsResponse { physical_ms, logical_start, count, epoch_hi: 0, epoch_lo: 0 };
            prop_assert!(matches!(
                decode_get_ts_response(response, count),
                Err(ClientError::Rpc(_))
            ));
        }

        // Positive case: a well-formed response decodes to exactly the
        // contiguous timestamp range described by the wire fields.
        #[test]
        fn well_formed_response_decodes_successfully(response in well_formed_response()) {
            let GetTsResponse { physical_ms, logical_start, count, .. } = response;
            let range = decode_get_ts_response(response, count).unwrap();

            prop_assert_eq!(range.count(), count);
            let timestamps: Vec<Timestamp> = range.iter().collect();
            prop_assert_eq!(timestamps.len(), count as usize);
            for (index, timestamp) in timestamps.iter().enumerate() {
                prop_assert_eq!(timestamp.physical_ms(), physical_ms);
                prop_assert_eq!(timestamp.logical(), logical_start + index as u32);
            }
            for pair in timestamps.windows(2) {
                prop_assert!(pair[0] < pair[1]);
            }
        }
    }
}
