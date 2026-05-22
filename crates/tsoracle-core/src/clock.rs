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

// #[PerformanceCriticalPath]
//! Source of physical-time milliseconds for the TSO algorithm.
//!
//! The Allocator's monotonicity is independent of clock correctness — a clock
//! jumping backward cannot cause timestamp regression because the persisted
//! high-water always wins. A clock pinned far in the past stalls new windows
//! until wall time catches up past the persisted high-water.

#[cfg(any(test, feature = "test-clock"))]
use core::sync::atomic::{AtomicU64, Ordering};

pub trait Clock: Send + Sync + 'static {
    /// Milliseconds since Unix epoch.
    fn now_ms(&self) -> u64;
}

/// Default implementation backed by `std::time::SystemTime`.
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }
}

#[cfg(any(test, feature = "test-clock"))]
pub mod testing {
    use super::*;
    use std::sync::Arc;

    /// Hand-driven clock for deterministic tests.
    #[derive(Clone, Default)]
    pub struct MockClock {
        ms: Arc<AtomicU64>,
    }

    impl MockClock {
        pub fn new(start_ms: u64) -> Self {
            MockClock {
                ms: Arc::new(AtomicU64::new(start_ms)),
            }
        }
        pub fn advance(&self, by_ms: u64) {
            self.ms.fetch_add(by_ms, Ordering::AcqRel);
        }
        pub fn set(&self, to_ms: u64) {
            self.ms.store(to_ms, Ordering::Release);
        }
    }

    impl Clock for MockClock {
        fn now_ms(&self) -> u64 {
            self.ms.load(Ordering::Acquire)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use testing::MockClock;

    #[test]
    fn system_clock_returns_nonzero() {
        let clock = SystemClock;
        let now = clock.now_ms();
        assert!(now > 1_700_000_000_000, "current time after 2023-11");
    }

    #[test]
    fn mock_clock_starts_at_seed() {
        let clock = MockClock::new(42);
        assert_eq!(clock.now_ms(), 42);
    }

    #[test]
    fn mock_clock_advance() {
        let clock = MockClock::new(100);
        clock.advance(50);
        assert_eq!(clock.now_ms(), 150);
    }

    #[test]
    fn mock_clock_set() {
        let clock = MockClock::new(100);
        clock.set(999);
        assert_eq!(clock.now_ms(), 999);
    }
}
