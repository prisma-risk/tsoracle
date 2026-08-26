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
        if let Some(prev) = self.last
            && ts <= prev
        {
            self.violations += 1;
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

    /// Soak variant of [`report`](Self::report): monotonicity stays the hard,
    /// zero-tolerance invariant, but a small final-error rate is allowed.
    ///
    /// A graceful rolling restart of a consensus cluster has an irreducible
    /// window where an in-flight RPC to a terminating pod is severed — a
    /// transport error the client's retry cannot always mask, and which the
    /// batch driver can fan out to several counted errors. Mature clients
    /// (etcd, CockroachDB, TiKV) tolerate this transient rather than
    /// guaranteeing zero client-visible errors across an election/restart. A
    /// run passes if it made at least one call, never went backwards, and its
    /// final error rate is below `max_error_rate`.
    pub fn report_within_error_tolerance(&self, label: &str, max_error_rate: f64) -> bool {
        let error_rate = if self.calls == 0 {
            1.0
        } else {
            self.errors as f64 / self.calls as f64
        };
        let passed = self.calls > 0 && self.violations == 0 && error_rate < max_error_rate;
        println!(
            "{label}: calls={} errors={} error_rate={:.4}% (max {:.4}%) \
             monotonicity_violations={} -> {}",
            self.calls,
            self.errors,
            error_rate * 100.0,
            max_error_rate * 100.0,
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

    #[test]
    fn error_rate_below_tolerance_passes() {
        let mut t = Tracker::<u64>::new();
        for i in 1..=1000u64 {
            t.record_ok(i);
        }
        t.record_err(); // 1 / 1001 ≈ 0.0999% < 0.5%
        assert!(t.report_within_error_tolerance("soak", 0.005));
    }

    #[test]
    fn error_rate_above_tolerance_fails() {
        let mut t = Tracker::<u64>::new();
        t.record_ok(1);
        t.record_err(); // 1 / 2 = 50% >= 0.5%
        assert!(!t.report_within_error_tolerance("soak", 0.005));
    }

    #[test]
    fn monotonicity_violation_fails_regardless_of_error_rate() {
        let mut t = Tracker::<u64>::new();
        for i in 1..=1000u64 {
            t.record_ok(i);
        }
        t.record_ok(5); // goes backwards → violation, even with 0 errors
        assert_eq!(t.errors, 0);
        assert!(!t.report_within_error_tolerance("soak", 0.005));
    }

    #[test]
    fn no_calls_within_tolerance_does_not_pass() {
        let t = Tracker::<u64>::new();
        assert!(!t.report_within_error_tolerance("soak", 0.005));
    }

    /// `report` prints a one-line summary and returns the same verdict as
    /// `passed`. Driving it through both a clean run and a failing run exercises
    /// both arms of the PASS/FAIL formatting. Also reaches the `Default` impl,
    /// which production constructs the tracker through.
    #[test]
    fn report_mirrors_passed_for_clean_and_failing_runs() {
        let mut clean = Tracker::<u64>::default();
        clean.record_ok(1);
        clean.record_ok(2);
        assert!(
            clean.report("clean"),
            "a strictly-increasing run reports PASS"
        );

        let mut dirty = Tracker::<u64>::default();
        dirty.record_ok(1);
        dirty.record_err();
        assert!(!dirty.report("dirty"), "a run with an error reports FAIL");
    }
}
