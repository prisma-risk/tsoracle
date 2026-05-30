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

//! Per-key gaplessness tracker for the dense `GetSeq` soak. A served block
//! `[start, start + count)` for a key must abut the previous block for that
//! key. `SeqUncertain` is a contiguity barrier (the advance may or may not
//! have committed, so the jump across it is not a server gap), never a hard
//! violation; it counts against the error budget like a severed RPC.

use std::collections::HashMap;

#[derive(Default)]
pub struct SeqTracker {
    pub calls: u64,
    pub errors: u64,
    pub gap_violations: u64,
    pub serving: bool,
    expected_next: HashMap<String, u64>,
}

impl SeqTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// A served block `[start, start + count)` for `key`. Within an unbroken
    /// run of successes for a key, the next block's start must equal the
    /// previous block's end; otherwise it is a gap/overlap violation.
    pub fn record_block(&mut self, key: &str, start: u64, count: u32) {
        self.calls += 1;
        self.serving = true;
        if let Some(&expected) = self.expected_next.get(key) {
            if start != expected {
                self.gap_violations += 1;
            }
        }
        self.expected_next
            .insert(key.to_string(), start + u64::from(count));
    }

    /// Pre-activation expected state (UNIMPLEMENTED / FAILED_PRECONDITION):
    /// counted as a call, not an error, not a block.
    pub fn record_not_serving(&mut self) {
        self.calls += 1;
    }

    /// Ambiguous post-send failure (`SeqUncertain`): error-budget hit AND a
    /// contiguity barrier — drop the per-key baseline so the next success
    /// re-bases without a false gap.
    pub fn record_uncertain(&mut self, key: &str) {
        self.calls += 1;
        self.errors += 1;
        self.expected_next.remove(key);
    }

    /// A definitive transport/RPC error: error-budget hit, contiguity baseline
    /// untouched (no block was issued).
    pub fn record_err(&mut self) {
        self.calls += 1;
        self.errors += 1;
    }

    pub fn report_within_error_tolerance(&self, label: &str, max_error_rate: f64) -> bool {
        let error_rate = if self.calls == 0 {
            1.0
        } else {
            self.errors as f64 / self.calls as f64
        };
        let passed = self.serving && self.gap_violations == 0 && error_rate < max_error_rate;
        println!(
            "{label}: seq_calls={} seq_errors={} seq_error_rate={:.4}% (max {:.4}%) \
             gap_violations={} serving={} -> {}",
            self.calls,
            self.errors,
            error_rate * 100.0,
            max_error_rate * 100.0,
            self.gap_violations,
            self.serving,
            if passed { "PASS" } else { "FAIL" }
        );
        passed
    }
}

#[cfg(test)]
mod tests {
    use super::SeqTracker;

    #[test]
    fn contiguous_blocks_have_no_violation() {
        let mut t = SeqTracker::new();
        t.record_block("orders", 0, 5);
        t.record_block("orders", 5, 3);
        t.record_block("orders", 8, 1);
        assert_eq!(t.gap_violations, 0);
        assert!(t.serving);
    }

    #[test]
    fn a_gap_is_a_violation() {
        let mut t = SeqTracker::new();
        t.record_block("orders", 0, 5);
        t.record_block("orders", 6, 1); // expected 5, got 6
        assert_eq!(t.gap_violations, 1);
    }

    #[test]
    fn an_overlap_is_a_violation() {
        let mut t = SeqTracker::new();
        t.record_block("orders", 0, 5);
        t.record_block("orders", 4, 1); // expected 5, got 4
        assert_eq!(t.gap_violations, 1);
    }

    #[test]
    fn seq_uncertain_rebaselines_without_a_false_gap() {
        let mut t = SeqTracker::new();
        t.record_block("orders", 0, 5); // expected_next = 5
        t.record_uncertain("orders"); // committed-or-not: drop baseline
        t.record_block("orders", 9, 2); // re-base: NOT a gap
        assert_eq!(t.gap_violations, 0);
        assert_eq!(t.errors, 1);
    }

    #[test]
    fn keys_are_independent() {
        let mut t = SeqTracker::new();
        t.record_block("orders", 0, 5);
        t.record_block("users", 0, 1); // different key, own baseline
        t.record_block("orders", 5, 1);
        t.record_block("users", 1, 1);
        assert_eq!(t.gap_violations, 0);
    }
}
