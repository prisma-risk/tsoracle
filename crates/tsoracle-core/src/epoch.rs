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

//! Leader epoch — opaque monotonic identifier chosen by the consensus driver.
//!
//! The width is `u128` so a driver can encode its full leadership identity
//! without truncation. The paxos driver packs an OmniPaxos `Ballot`'s
//! `(config_id: u32, n: u32, pid: u64)` — 128 bits — losslessly; raft-style
//! drivers use the low bits for a `u64` term. The fence compares epochs by
//! equality and the client's leader cache orders them, so the encoding a
//! driver chooses must be both injective and monotonic.

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Epoch(pub u128);

impl Epoch {
    pub const ZERO: Epoch = Epoch(0);

    /// Split into `(hi, lo)` 64-bit halves for the two-`uint64` wire form.
    /// `hi` is the more significant half, so lexicographic `(hi, lo)`
    /// comparison equals numeric `u128` comparison.
    #[must_use]
    pub fn to_wire(self) -> (u64, u64) {
        ((self.0 >> 64) as u64, self.0 as u64)
    }

    /// Reassemble from the `(hi, lo)` halves produced by [`Self::to_wire`].
    #[must_use]
    pub fn from_wire(hi: u64, lo: u64) -> Self {
        Epoch((u128::from(hi) << 64) | u128::from(lo))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn default_is_zero() {
        assert_eq!(Epoch::default(), Epoch::ZERO);
    }

    #[test]
    fn ordering() {
        assert!(Epoch(1) < Epoch(2));
        assert!(Epoch::ZERO < Epoch(1));
    }

    #[test]
    fn wire_round_trip_spans_the_64_bit_boundary() {
        for epoch in [
            Epoch::ZERO,
            Epoch(1),
            Epoch(u128::from(u64::MAX)),
            Epoch(u128::from(u64::MAX) + 1),
            Epoch(u128::MAX),
        ] {
            let (hi, lo) = epoch.to_wire();
            assert_eq!(Epoch::from_wire(hi, lo), epoch);
        }
    }

    #[test]
    fn wire_halves_preserve_numeric_order() {
        // A value one above u64::MAX (hi=1, lo=0) must outrank the largest
        // value that fits in the low half alone (hi=0, lo=u64::MAX).
        let just_over = Epoch::from_wire(1, 0);
        let max_low = Epoch::from_wire(0, u64::MAX);
        assert!(just_over > max_low);
    }

    proptest! {
        // from_wire is the exact inverse of to_wire across the full u128 domain.
        #[test]
        fn wire_round_trip(raw in any::<u128>()) {
            let epoch = Epoch(raw);
            let (hi, lo) = epoch.to_wire();
            prop_assert_eq!(Epoch::from_wire(hi, lo), epoch);
        }

        // Lexicographic (hi, lo) order equals numeric Epoch order — the
        // property the client's monotone-forward leader cache relies on.
        #[test]
        fn wire_halves_order_matches_numeric(a in any::<u128>(), b in any::<u128>()) {
            let (ea, eb) = (Epoch(a), Epoch(b));
            prop_assert_eq!(ea.to_wire().cmp(&eb.to_wire()), ea.cmp(&eb));
        }
    }
}
