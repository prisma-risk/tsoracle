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

/// Records the outcome of a sequence of `get_ts` calls and decides pass/fail.
///
/// Generic over the timestamp type so the unit tests can drive it with `u64`
/// while production uses `tsoracle_core::Timestamp`. A run passes only if it
/// made at least one call, saw zero errors, and every successful timestamp was
/// strictly greater than the previous one.
pub struct Tracker<T> {
    pub calls: u64,
    pub errors: u64,
    pub violations: u64,
    last: Option<T>,
}

impl<T: Ord + Copy> Tracker<T> {
    pub fn new() -> Self {
        Tracker {
            calls: 0,
            errors: 0,
            violations: 0,
            last: None,
        }
    }

    pub fn record_ok(&mut self, ts: T) {
        self.calls += 1;
        if let Some(prev) = self.last {
            if ts <= prev {
                self.violations += 1;
            }
        }
        self.last = Some(ts);
    }

    pub fn record_err(&mut self) {
        self.calls += 1;
        self.errors += 1;
    }

    pub fn passed(&self) -> bool {
        self.calls > 0 && self.errors == 0 && self.violations == 0
    }

    /// Print a one-line summary and return whether the run passed.
    pub fn report(&self, label: &str) -> bool {
        let passed = self.passed();
        println!(
            "{label}: calls={} errors={} monotonicity_violations={} -> {}",
            self.calls,
            self.errors,
            self.violations,
            if passed { "PASS" } else { "FAIL" }
        );
        passed
    }
}

impl<T: Ord + Copy> Default for Tracker<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::Tracker;

    #[test]
    fn strictly_increasing_passes() {
        let mut t = Tracker::<u64>::new();
        t.record_ok(1);
        t.record_ok(2);
        t.record_ok(3);
        assert_eq!(t.violations, 0);
        assert!(t.passed());
    }

    #[test]
    fn equal_or_decreasing_is_a_violation() {
        let mut t = Tracker::<u64>::new();
        t.record_ok(5);
        t.record_ok(5); // not strictly greater
        t.record_ok(4); // decreasing
        assert_eq!(t.violations, 2);
        assert!(!t.passed());
    }

    #[test]
    fn any_error_fails_the_run() {
        let mut t = Tracker::<u64>::new();
        t.record_ok(1);
        t.record_err();
        assert_eq!(t.errors, 1);
        assert!(!t.passed());
    }

    #[test]
    fn no_calls_does_not_pass() {
        let t = Tracker::<u64>::new();
        assert!(!t.passed());
    }
}
