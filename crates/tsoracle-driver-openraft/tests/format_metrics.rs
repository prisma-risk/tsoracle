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

//! Recorder-fake test for the format-migration metric catalog. Installs a
//! `DebuggingRecorder` as the process-global recorder for this test binary
//! and asserts each `tsoracle.schema.*` signal fires for an end-to-end
//! activation scenario:
//!
//! - boot gauges (`active_write_version`, `min_readable_version`,
//!   `max_readable_version`) on state-machine construction;
//! - gate run gauge (`min_member_read_capability`) on every
//!   `run_activation_gate`;
//! - `proposed` + `committed` + `applied` counters and an
//!   `active_write_version` gauge update on a successful activation;
//! - `rejected_by_gate` on a target above the local readable max;
//! - `noop_membership_subset` on a direct apply whose committed
//!   membership is not a subset of the gated set.
//!
//! Today `MIN_READABLE_VERSION == MAX_READABLE_VERSION ==
//! BASELINE_WRITE_VERSION == 4`, so the successful activation flips the
//! cell from 4 back to 4 — the value is unchanged, but the apply-keyed
//! signal still fires. The test asserts the signal plumbing, not a real
//! cross-version data migration.

#![cfg(feature = "metrics")]

mod common;

use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use metrics_util::{
    CompositeKey, MetricKind,
    debugging::{DebugValue, DebuggingRecorder, Snapshotter},
};
use tokio::time::timeout;
use tsoracle_consensus::{ConsensusDriver, LeaderState};
use tsoracle_driver_openraft::{
    CapabilitySource, FormatActivationError, HighWaterCommand, HighWaterStateMachine,
    NodeCapabilities, OpenraftPeer, SetFormatVersionPayload, StandaloneHost,
};
use tsoracle_openraft_toolkit::{
    BASELINE_WRITE_VERSION, MAX_READABLE_VERSION, MIN_READABLE_VERSION,
};

use common::build_single_node;

type RecordedMetric = (
    CompositeKey,
    Option<metrics::Unit>,
    Option<metrics::SharedString>,
    DebugValue,
);

fn install_recorder() -> Snapshotter {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install metrics recorder");
    snapshotter
}

fn counter_value(snapshot: &[RecordedMetric], name: &str) -> u64 {
    for (composite, _unit, _desc, value) in snapshot {
        if composite.kind() == MetricKind::Counter
            && composite.key().name() == name
            && let DebugValue::Counter(n) = value
        {
            return *n;
        }
    }
    0
}

fn gauge_value(snapshot: &[RecordedMetric], name: &str) -> Option<f64> {
    for (composite, _unit, _desc, value) in snapshot {
        if composite.kind() == MetricKind::Gauge
            && composite.key().name() == name
            && let DebugValue::Gauge(g) = value
        {
            return Some(g.into_inner());
        }
    }
    None
}

/// A `CapabilitySource` that must never be called: the single-node gate
/// short-circuits to the local node, so any remote query is a bug.
struct UnusedSource;

#[async_trait]
impl CapabilitySource for UnusedSource {
    type Node = OpenraftPeer;

    async fn query(
        &self,
        node_id: u64,
        _member: &OpenraftPeer,
    ) -> Result<NodeCapabilities, String> {
        panic!("single-node gate must not query remote node {node_id}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn format_migration_signals_fire_end_to_end() {
    // Install the recorder BEFORE building anything so the boot gauges
    // fired during `HighWaterStateMachine::with_store*` are captured.
    let snapshotter = install_recorder();

    // ---- 1. Boot a single-node host. The state machine's construction
    //         emits the readable-bounds + initial active-write-version
    //         gauges; build_single_node returns once the cluster exists
    //         (leadership is awaited separately below).
    let cluster = build_single_node().await;
    let driver = &cluster.drivers[0];

    let mut events = driver.leadership_events();
    timeout(Duration::from_secs(5), async {
        loop {
            let state = events.next().await.expect("event stream alive");
            if matches!(state, LeaderState::Leader { .. }) {
                break;
            }
        }
    })
    .await
    .expect("single-node openraft did not elect itself within 5s");

    let host = StandaloneHost::new(cluster.nodes[0].raft.clone(), cluster.nodes[0].sm.clone());

    // ---- 2. Membership-subset no-op: drive the apply directly on a
    //         bare state machine (the live host's gate would refuse this
    //         scenario before apply, but the apply arm itself is the
    //         counter site we're verifying). Apply a Membership entry
    //         establishing {1, 2, 9}, then a SetFormatVersion gated only
    //         on {1, 2}; committed_members ⊄ gated → no-op counter fires.
    //
    //         Order matters: this constructs a FRESH state machine via
    //         `with_store_and_active_version`, which fires the boot
    //         `record_active_write_version(BASELINE)` and would clobber
    //         a later apply-flip gauge value. Doing it BEFORE the
    //         successful activation below keeps the activation's
    //         `record_active_write_version(target)` as the latest write
    //         and observable in the snapshot.
    drive_membership_subset_noop_apply().await;

    // ---- 3. Successful activation: gate passes (proposed + committed +
    //         applied), apply-flip sets the active_write_version gauge.
    //         Target == MAX_READABLE_VERSION trivially passes today since
    //         the local node's max_readable_version == MAX_READABLE_VERSION.
    host.initiate_format_activation(MAX_READABLE_VERSION, &UnusedSource)
        .await
        .expect("activation to MAX_READABLE_VERSION must pass on a single-node leader");

    // ---- 4. Gate rejection: target above the local readable max.
    //         The local-binary range short-circuit fires BEFORE any
    //         per-member RPC, surfacing `TargetOutOfRange` rather than
    //         `MembersBelowTarget`. Both increment the same
    //         `rejected_by_gate` counter, which is what this end-to-end
    //         metrics test cares about.
    let rejected = host
        .initiate_format_activation(MAX_READABLE_VERSION + 1, &UnusedSource)
        .await;
    assert!(
        matches!(
            rejected,
            Err(FormatActivationError::TargetOutOfRange { .. })
        ),
        "target above MAX_READABLE_VERSION must be rejected by the gate, got: {rejected:?}"
    );

    // ---- Assertions over the recorder snapshot.
    let snapshot: Vec<RecordedMetric> = snapshotter.snapshot().into_vec();

    assert!(
        counter_value(&snapshot, "tsoracle.schema.format_version.proposed.total") >= 1,
        "proposed.total did not increment for the gate-passing activation"
    );
    assert!(
        counter_value(&snapshot, "tsoracle.schema.format_version.committed.total") >= 1,
        "committed.total did not increment after the entry committed"
    );
    assert!(
        counter_value(&snapshot, "tsoracle.schema.format_version.applied.total") >= 1,
        "applied.total did not increment after the successful flip"
    );
    assert!(
        counter_value(
            &snapshot,
            "tsoracle.schema.format_version.rejected_by_gate.total"
        ) >= 1,
        "rejected_by_gate.total did not increment for the over-target activation"
    );
    assert!(
        counter_value(
            &snapshot,
            "tsoracle.schema.format_version.noop_membership_subset.total"
        ) >= 1,
        "noop_membership_subset.total did not increment for the subset no-op apply"
    );
    assert_eq!(
        gauge_value(&snapshot, "tsoracle.schema.active_write_version"),
        Some(f64::from(MAX_READABLE_VERSION)),
        "active_write_version gauge should reflect the flipped version"
    );
    assert_eq!(
        gauge_value(&snapshot, "tsoracle.schema.min_readable_version"),
        Some(f64::from(MIN_READABLE_VERSION)),
        "min_readable_version gauge should be the compile-time constant"
    );
    assert_eq!(
        gauge_value(&snapshot, "tsoracle.schema.max_readable_version"),
        Some(f64::from(MAX_READABLE_VERSION)),
        "max_readable_version gauge should be the compile-time constant"
    );
    assert!(
        gauge_value(&snapshot, "tsoracle.schema.min_member_read_capability").is_some(),
        "min_member_read_capability gauge should be set on every gate run"
    );
    // Sanity: the cell really did flip (BASELINE == MAX today so the value
    // is unchanged, but the signal plumbing is what we care about).
    assert_eq!(host.active_write_version(), MAX_READABLE_VERSION);
    let _ = BASELINE_WRITE_VERSION; // referenced in the doc comment intent
}

/// Apply a Membership({1,2,9}) entry followed by a SetFormatVersion gated
/// on {1,2}; the committed membership is NOT a subset of the gated set,
/// so the apply arm returns `FormatActivationNoop` and increments the
/// `noop_membership_subset.total` counter.
async fn drive_membership_subset_noop_apply() {
    use std::collections::BTreeSet;

    use futures::stream;
    use openraft::EntryPayload;
    use openraft::entry::RaftEntry;
    use openraft::storage::{EntryResponder, RaftStateMachine};
    use openraft::type_config::alias::EntryOf;
    use tsoracle_driver_openraft::TypeConfig;

    fn log_id(index: u64) -> openraft::type_config::alias::LogIdOf<TypeConfig> {
        openraft::testing::log_id::<TypeConfig>(1, 1, index)
    }

    fn entry(
        index: u64,
        payload: EntryPayload<HighWaterCommand, u64, OpenraftPeer>,
    ) -> EntryResponder<TypeConfig> {
        let e: EntryOf<TypeConfig> = match payload {
            EntryPayload::Blank => EntryOf::<TypeConfig>::new_blank(log_id(index)),
            EntryPayload::Normal(d) => EntryOf::<TypeConfig>::new_normal(log_id(index), d),
            EntryPayload::Membership(m) => EntryOf::<TypeConfig>::new_membership(log_id(index), m),
        };
        (e, None)
    }

    let mut sm = HighWaterStateMachine::new();

    // Membership with voters {1,2} and learner {9}; an un-gated learner
    // forces the no-op.
    let membership: openraft::Membership<u64, OpenraftPeer> =
        openraft::Membership::new_with_defaults(
            vec![BTreeSet::from([1u64, 2u64])],
            [1u64, 2u64, 9u64],
        );
    sm.apply(stream::iter([Ok(entry(
        1,
        EntryPayload::Membership(membership),
    ))]))
    .await
    .expect("apply membership");

    // SetFormatVersion gated only on {1,2} — node 9 is in the committed
    // membership but not in the gated set, so apply must no-op.
    let bump = HighWaterCommand::SetFormatVersion(SetFormatVersionPayload {
        target: MAX_READABLE_VERSION,
        gated_members: BTreeSet::from([1u64, 2u64]),
    });
    sm.apply(stream::iter([Ok(entry(2, EntryPayload::Normal(bump)))]))
        .await
        .expect("apply set_format_version");
}
