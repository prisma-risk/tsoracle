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

//! Source of physical-time milliseconds for the TSO algorithm.
//!
//! The allocator's monotonicity is independent of clock correctness — a clock
//! jumping backward cannot cause timestamp regression because the persisted
//! high-water always wins. A clock pinned far in the past stalls new windows
//! until wall time catches up past the persisted high-water.
//!
//! The `Send + Sync + 'static` bound exists because the server stores its clock
//! as an `Arc<dyn Clock>` shared across request-handler tasks; it is not a
//! property the underlying `tsoracle-core` allocator requires.

pub trait Clock: Send + Sync + 'static {
    /// Milliseconds since Unix epoch.
    fn now_ms(&self) -> u64;
}

/// Default implementation backed by `std::time::SystemTime`.
///
/// The conversion to `u64` milliseconds saturates at both ends rather than
/// wrapping or panicking, so a misconfigured clock surfaces visibly instead of
/// silently stalling window advance:
///
/// - A time so far in the future that the millisecond count exceeds `u64::MAX`
///   saturates to `u64::MAX`. A bare `as u64` cast would wrap to a small value,
///   making a far-future clock masquerade as the distant past and stall the
///   allocator — the opposite of the truth. `u64::MAX` instead drives the
///   allocator straight into visible window exhaustion.
/// - A pre-Unix-epoch time saturates to `0` (the earliest representable
///   instant). Per the module docs, a clock pinned in the past stalls new
///   windows until wall time catches up past the persisted high-water; `0` is
///   the faithful representation of such a clock, not a swallowed error.
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(saturating_millis)
            .unwrap_or(0)
    }
}

/// Milliseconds in `d`, saturating to `u64::MAX` rather than truncating when
/// the count overflows `u64`.
fn saturating_millis(d: std::time::Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_clock_returns_nonzero() {
        let clock = SystemClock;
        let now = clock.now_ms();
        assert!(now > 1_700_000_000_000, "current time after 2023-11");
    }

    #[test]
    fn saturating_millis_passes_through_in_range() {
        use std::time::Duration;
        assert_eq!(saturating_millis(Duration::from_millis(123)), 123);
    }

    #[test]
    fn saturating_millis_saturates_instead_of_truncating() {
        use std::time::Duration;
        // u64::MAX seconds is ~1000x more milliseconds than u64 can hold, so a
        // bare `as u64` cast would wrap to a small value; saturation must pin
        // it to u64::MAX so a far-future clock never masquerades as the past.
        assert_eq!(saturating_millis(Duration::from_secs(u64::MAX)), u64::MAX);
    }
}
