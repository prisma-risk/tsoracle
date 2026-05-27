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

//! Hand-driven `Clock` for deterministic tests (integration + DST suites).

use crate::Clock;
use core::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

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

#[cfg(test)]
mod tests {
    use super::*;

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
