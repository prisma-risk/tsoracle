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

//! Format-migration observability: the metric-name constants and the emit
//! helpers the activation path and the state-machine apply arm call. Every
//! emission is gated behind the crate's `metrics` feature so the `metrics`
//! dependency stays opt-in (matching `tsoracle-server`). The helpers are
//! deliberately the single place each `tsoracle.schema.*` name is written,
//! so the apply arm, the gate, the boot path, and the metrics test cannot
//! drift apart on a name. When the feature is off, every helper compiles to
//! a no-op with an identical signature.

/// Gauge: the node's current durable active write version.
pub const ACTIVE_WRITE_VERSION: &str = "tsoracle.schema.active_write_version";
/// Gauge: the compile-time `MIN_READABLE_VERSION`.
pub const MIN_READABLE_VERSION: &str = "tsoracle.schema.min_readable_version";
/// Gauge: the compile-time `MAX_READABLE_VERSION`.
pub const MAX_READABLE_VERSION: &str = "tsoracle.schema.max_readable_version";
/// Gauge: the lowest `max_readable_version` seen across all current members
/// at the most recent gate run (the binding constraint on activation).
pub const MIN_MEMBER_READ_CAPABILITY: &str = "tsoracle.schema.min_member_read_capability";

/// Counter: a `SetFormatVersion` entry was proposed (gate passed).
pub const PROPOSED_TOTAL: &str = "tsoracle.schema.format_version.proposed.total";
/// Counter: a proposed `SetFormatVersion` entry was observed committed.
pub const COMMITTED_TOTAL: &str = "tsoracle.schema.format_version.committed.total";
/// Counter: a `SetFormatVersion` entry applied successfully (flip occurred).
pub const APPLIED_TOTAL: &str = "tsoracle.schema.format_version.applied.total";
/// Counter: a `SetFormatVersion` entry applied as a no-op because the
/// membership at its log position was not a subset of its gated set.
pub const NOOP_MEMBERSHIP_SUBSET_TOTAL: &str =
    "tsoracle.schema.format_version.noop_membership_subset.total";
/// Counter: an activation attempt was rejected by the all-members gate (a
/// member's `max_readable_version` was below the target).
pub const REJECTED_BY_GATE_TOTAL: &str = "tsoracle.schema.format_version.rejected_by_gate.total";

/// Set the active-write-version gauge. Called on a successful apply-flip
/// and on boot/recovery once the durable active write version is known.
#[cfg(feature = "metrics")]
pub fn record_active_write_version(version: u8) {
    metrics::gauge!(ACTIVE_WRITE_VERSION).set(f64::from(version));
}
#[cfg(not(feature = "metrics"))]
pub fn record_active_write_version(_version: u8) {}

/// Set the compile-time readable-bounds gauges. Called once at boot.
#[cfg(feature = "metrics")]
pub fn record_readable_bounds(min_readable: u8, max_readable: u8) {
    metrics::gauge!(MIN_READABLE_VERSION).set(f64::from(min_readable));
    metrics::gauge!(MAX_READABLE_VERSION).set(f64::from(max_readable));
}
#[cfg(not(feature = "metrics"))]
pub fn record_readable_bounds(_min_readable: u8, _max_readable: u8) {}

/// Set the min-member-read-capability gauge from a gate run's gathered
/// capabilities (the lowest `max_readable_version` across all current
/// members).
#[cfg(feature = "metrics")]
pub fn record_min_member_read_capability(min_member_max_readable: u8) {
    metrics::gauge!(MIN_MEMBER_READ_CAPABILITY).set(f64::from(min_member_max_readable));
}
#[cfg(not(feature = "metrics"))]
pub fn record_min_member_read_capability(_min_member_max_readable: u8) {}

/// Counter: increment on each proposal that passes the gate.
#[cfg(feature = "metrics")]
pub fn record_proposed() {
    metrics::counter!(PROPOSED_TOTAL).increment(1);
}
#[cfg(not(feature = "metrics"))]
pub fn record_proposed() {}

/// Counter: increment when the proposed entry is observed committed.
#[cfg(feature = "metrics")]
pub fn record_committed() {
    metrics::counter!(COMMITTED_TOTAL).increment(1);
}
#[cfg(not(feature = "metrics"))]
pub fn record_committed() {}

/// Counter: increment on a successful apply-flip (subset check passed).
#[cfg(feature = "metrics")]
pub fn record_applied() {
    metrics::counter!(APPLIED_TOTAL).increment(1);
}
#[cfg(not(feature = "metrics"))]
pub fn record_applied() {}

/// Counter: increment when apply is a no-op (membership not a subset of
/// the gated set carried by the entry).
#[cfg(feature = "metrics")]
pub fn record_noop_membership_subset() {
    metrics::counter!(NOOP_MEMBERSHIP_SUBSET_TOTAL).increment(1);
}
#[cfg(not(feature = "metrics"))]
pub fn record_noop_membership_subset() {}

/// Counter: increment when the gate rejects an activation attempt (a
/// member cannot read the target).
#[cfg(feature = "metrics")]
pub fn record_rejected_by_gate() {
    metrics::counter!(REJECTED_BY_GATE_TOTAL).increment(1);
}
#[cfg(not(feature = "metrics"))]
pub fn record_rejected_by_gate() {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metric_names_match_the_interface_contract() {
        // Pin the exact strings so the apply arm, the gate, the boot
        // path, and the metrics test cannot drift apart on a name.
        assert_eq!(ACTIVE_WRITE_VERSION, "tsoracle.schema.active_write_version");
        assert_eq!(MIN_READABLE_VERSION, "tsoracle.schema.min_readable_version");
        assert_eq!(MAX_READABLE_VERSION, "tsoracle.schema.max_readable_version");
        assert_eq!(
            MIN_MEMBER_READ_CAPABILITY,
            "tsoracle.schema.min_member_read_capability"
        );
        assert_eq!(
            PROPOSED_TOTAL,
            "tsoracle.schema.format_version.proposed.total"
        );
        assert_eq!(
            COMMITTED_TOTAL,
            "tsoracle.schema.format_version.committed.total"
        );
        assert_eq!(
            APPLIED_TOTAL,
            "tsoracle.schema.format_version.applied.total"
        );
        assert_eq!(
            NOOP_MEMBERSHIP_SUBSET_TOTAL,
            "tsoracle.schema.format_version.noop_membership_subset.total"
        );
        assert_eq!(
            REJECTED_BY_GATE_TOTAL,
            "tsoracle.schema.format_version.rejected_by_gate.total"
        );
    }

    #[test]
    fn emit_helpers_are_callable_without_metrics_feature() {
        // No-op helpers must exist and be callable regardless of the
        // `metrics` feature; the call sites in state_machine.rs /
        // standalone.rs compile unconditionally.
        record_active_write_version(4);
        record_readable_bounds(4, 4);
        record_min_member_read_capability(4);
        record_proposed();
        record_committed();
        record_applied();
        record_noop_membership_subset();
        record_rejected_by_gate();
    }
}
