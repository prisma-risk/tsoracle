//! 64-bit packed timestamp: 46 high bits for `physical_ms`, 18 low bits for `logical`.

const LOGICAL_BITS: u32 = 18;
const LOGICAL_MASK: u64 = (1 << LOGICAL_BITS) - 1;
pub const LOGICAL_MAX: u32 = (1 << LOGICAL_BITS) - 1;
pub const PHYSICAL_MS_MAX: u64 = (1 << (64 - LOGICAL_BITS)) - 1;

#[derive(Copy, Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum TimestampError {
    #[error("physical_ms {physical_ms} exceeds 46-bit maximum {max}")]
    PhysicalMsOutOfRange { physical_ms: u64, max: u64 },
    #[error("logical {logical} exceeds 18-bit maximum {max}")]
    LogicalOutOfRange { logical: u32, max: u32 },
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Timestamp(pub u64);

impl Timestamp {
    pub const fn pack(physical_ms: u64, logical: u32) -> Self {
        assert!(
            physical_ms <= PHYSICAL_MS_MAX,
            "physical_ms exceeds 46-bit timestamp field"
        );
        assert!(
            logical <= LOGICAL_MAX,
            "logical exceeds 18-bit timestamp field"
        );
        Timestamp((physical_ms << LOGICAL_BITS) | (logical as u64))
    }

    pub const fn try_pack(physical_ms: u64, logical: u32) -> Result<Self, TimestampError> {
        if physical_ms > PHYSICAL_MS_MAX {
            return Err(TimestampError::PhysicalMsOutOfRange {
                physical_ms,
                max: PHYSICAL_MS_MAX,
            });
        }
        if logical > LOGICAL_MAX {
            return Err(TimestampError::LogicalOutOfRange {
                logical,
                max: LOGICAL_MAX,
            });
        }
        Ok(Timestamp((physical_ms << LOGICAL_BITS) | (logical as u64)))
    }

    pub const fn physical_ms(self) -> u64 {
        self.0 >> LOGICAL_BITS
    }
    pub const fn logical(self) -> u32 {
        (self.0 & LOGICAL_MASK) as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pack_unpack_roundtrip() {
        let ts = Timestamp::pack(1_700_000_000_000, 12345);
        assert_eq!(ts.physical_ms(), 1_700_000_000_000);
        assert_eq!(ts.logical(), 12345);
    }

    #[test]
    fn pack_zero() {
        let ts = Timestamp::pack(0, 0);
        assert_eq!(ts.0, 0);
        assert_eq!(ts.physical_ms(), 0);
        assert_eq!(ts.logical(), 0);
    }

    #[test]
    fn pack_max_logical() {
        let ts = Timestamp::pack(1000, LOGICAL_MAX);
        assert_eq!(ts.physical_ms(), 1000);
        assert_eq!(ts.logical(), LOGICAL_MAX);
    }

    #[test]
    fn try_pack_rejects_out_of_range_physical_ms() {
        assert!(matches!(
            Timestamp::try_pack(PHYSICAL_MS_MAX + 1, 0),
            Err(TimestampError::PhysicalMsOutOfRange { .. })
        ));
    }

    #[test]
    fn try_pack_rejects_out_of_range_logical() {
        assert!(matches!(
            Timestamp::try_pack(0, LOGICAL_MAX + 1),
            Err(TimestampError::LogicalOutOfRange { .. })
        ));
    }

    #[test]
    fn ordering_follows_packed_value() {
        let earliest = Timestamp::pack(1000, 5);
        let middle = Timestamp::pack(1000, 6);
        let latest = Timestamp::pack(1001, 0);
        assert!(earliest < middle);
        assert!(middle < latest);
        assert!(earliest < latest);
    }

    use proptest::prelude::*;

    proptest! {
        // Roundtrip: pack then unpack returns the original fields for any valid
        // input in the 46-bit physical / 18-bit logical domain. A single failure
        // here means the bit layout is wrong (mask, shift, or width).
        #[test]
        fn pack_unpack_roundtrip_property(
            physical_ms in 0u64..=PHYSICAL_MS_MAX,
            logical in 0u32..=LOGICAL_MAX,
        ) {
            let packed = Timestamp::pack(physical_ms, logical);
            prop_assert_eq!(packed.physical_ms(), physical_ms);
            prop_assert_eq!(packed.logical(), logical);
        }

        // Order preservation: the packed u64 ordering equals lexicographic
        // ordering on (physical_ms, logical). This is the property that lets
        // every consumer compare Timestamps directly without unpacking.
        #[test]
        fn ordering_matches_lexicographic_pair(
            physical_a in 0u64..=PHYSICAL_MS_MAX,
            logical_a in 0u32..=LOGICAL_MAX,
            physical_b in 0u64..=PHYSICAL_MS_MAX,
            logical_b in 0u32..=LOGICAL_MAX,
        ) {
            let timestamp_a = Timestamp::pack(physical_a, logical_a);
            let timestamp_b = Timestamp::pack(physical_b, logical_b);
            let pair_ordering = (physical_a, logical_a).cmp(&(physical_b, logical_b));
            prop_assert_eq!(timestamp_a.cmp(&timestamp_b), pair_ordering);
        }

        // try_pack accepts exactly the values pack accepts and rejects everything
        // else, with the right error variant. Catches drift between the two APIs.
        #[test]
        fn try_pack_accepts_iff_in_range(
            physical_ms in 0u64..=u64::MAX >> 10,
            logical in 0u32..=u32::MAX >> 10,
        ) {
            let result = Timestamp::try_pack(physical_ms, logical);
            if physical_ms > PHYSICAL_MS_MAX {
                let rejected_physical =
                    matches!(result, Err(TimestampError::PhysicalMsOutOfRange { .. }));
                prop_assert!(rejected_physical);
            } else if logical > LOGICAL_MAX {
                let rejected_logical =
                    matches!(result, Err(TimestampError::LogicalOutOfRange { .. }));
                prop_assert!(rejected_logical);
            } else {
                let timestamp = result.unwrap();
                prop_assert_eq!(timestamp.physical_ms(), physical_ms);
                prop_assert_eq!(timestamp.logical(), logical);
            }
        }
    }
}
