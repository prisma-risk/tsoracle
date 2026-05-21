//! Decode a `GetTsResponse` into `Vec<Timestamp>` with the protocol-level
//! safety checks the retry loop used to perform inline.
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

pub(crate) fn decode_get_ts_response(
    resp: GetTsResponse,
    requested: u32,
) -> Result<Vec<Timestamp>, ClientError> {
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
    let mut out = Vec::with_capacity(resp.count as usize);
    for i in 0..resp.count {
        let ts = Timestamp::try_pack(resp.physical_ms, resp.logical_start + i).map_err(|e| {
            ClientError::Rpc(tonic::Status::internal(format!(
                "server returned invalid timestamp fields: {e}"
            )))
        })?;
        out.push(ts);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use tsoracle_core::PHYSICAL_MS_MAX;

    #[test]
    fn rejects_out_of_range_physical_ms() {
        let err = decode_get_ts_response(
            GetTsResponse {
                physical_ms: PHYSICAL_MS_MAX + 1,
                logical_start: 0,
                count: 1,
                epoch: 0,
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
                epoch: 0,
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
                epoch: 0,
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
                    epoch,
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
            let response = GetTsResponse { physical_ms, logical_start, count, epoch: 0 };
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
            let timestamps = decode_get_ts_response(response, count).unwrap();

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
