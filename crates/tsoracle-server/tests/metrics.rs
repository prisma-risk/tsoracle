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

//! Recorder-fake test for the documented metrics catalog.
//!
//! Drives a representative scenario against a `DebuggingRecorder` from
//! `metrics-util` and asserts each documented signal in
//! `docs/operations.md` fires at least once. The recorder is installed as
//! the process-global recorder for this test binary; integration tests in
//! other files run in separate binaries and are unaffected.

use std::{sync::Arc, time::Duration};

use metrics_util::{
    CompositeKey, MetricKind,
    debugging::{DebugValue, DebuggingRecorder, Snapshotter},
};
use tokio::time::sleep;
use tsoracle_core::Epoch;
use tsoracle_proto::v1::{GetTsRequest, tso_service_client::TsoServiceClient};
use tsoracle_server::test_fakes::InMemoryDriver;
use tsoracle_server::test_support::{
    boot_server, wait_for_grpc_handshake, wait_until, wait_until_serving,
};
use tsoracle_server::{Server, ServingState};

/// Tuple shape produced by `Snapshot::into_vec` in `metrics-util` 0.19.
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
        if composite.kind() == MetricKind::Counter && composite.key().name() == name {
            if let DebugValue::Counter(n) = value {
                return *n;
            }
        }
    }
    0
}

fn histogram_sample_count(snapshot: &[RecordedMetric], name: &str) -> usize {
    for (composite, _unit, _desc, value) in snapshot {
        if composite.kind() == MetricKind::Histogram && composite.key().name() == name {
            if let DebugValue::Histogram(samples) = value {
                return samples.len();
            }
        }
    }
    0
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn emits_documented_signals_end_to_end() {
    let snapshotter = install_recorder();

    let driver = Arc::new(InMemoryDriver::new());
    // Small failover_advance so sleeping past it forces a WindowExhausted
    // retry in the GetTs handler, which drives extend_window → the
    // documented window.* signals. The InMemoryDriver's persist is in-
    // memory, so the latency sample we record is tiny but nonzero.
    let server = Server::builder()
        .consensus_driver(driver.clone())
        .failover_advance(Duration::from_millis(20))
        .window_ahead(Duration::from_millis(50))
        .build()
        .unwrap();
    let mut booted = boot_server(server).await;

    driver.become_leader(Epoch(1));
    wait_until_serving(&mut booted.state_rx).await;
    wait_for_grpc_handshake(booted.addr, Duration::from_secs(5))
        .await
        .expect("tonic never accepted gRPC handshake");

    let mut client = TsoServiceClient::connect(format!("http://{}", booted.addr))
        .await
        .unwrap();

    // First call lands inside the post-fence window: drives
    // get_ts.total (+1) and get_ts.timestamps_issued (+5).
    let resp = client
        .get_ts(GetTsRequest { count: 5 })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.count, 5);

    // Sleep past failover_advance so the allocator's clock_now exceeds the
    // committed ceiling on the next try_grant: that returns WindowExhausted,
    // and the inner attempt loop reaches extend_window → persist_high_water.
    sleep(Duration::from_millis(60)).await;
    let resp = client
        .get_ts(GetTsRequest { count: 3 })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.count, 3);

    // Drop leadership, then issue one more GetTs to drive a NOT_LEADER
    // rejection through `not_leader_status`.
    driver.become_follower(Some("10.9.8.7:50551".into()));
    wait_until(&mut booted.state_rx, |s| {
        matches!(
            s,
            ServingState::NotServing {
                leader_endpoint: Some(_)
            }
        )
    })
    .await;
    let err = client.get_ts(GetTsRequest { count: 1 }).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);

    booted.shutdown().await.unwrap();

    // `Snapshotter::snapshot()` drains histogram samples each call, so take
    // a single snapshot and pass the resulting `Vec` into every helper.
    let snapshot: Vec<RecordedMetric> = snapshotter.snapshot().into_vec();

    // Every documented signal must have fired at least once. Inequalities
    // (not strict equalities) keep the test robust against the initial
    // LeaderState::Unknown the watch channel may surface ahead of Leader.
    assert!(
        counter_value(&snapshot, "tsoracle.get_ts.total") >= 2,
        "tsoracle.get_ts.total did not increment for both GetTs calls"
    );
    assert!(
        counter_value(&snapshot, "tsoracle.get_ts.timestamps_issued") >= 8,
        "tsoracle.get_ts.timestamps_issued did not sum counts (expected >= 5 + 3)"
    );
    assert!(
        counter_value(&snapshot, "tsoracle.window.extensions.total") >= 1,
        "tsoracle.window.extensions.total never incremented; extend_window may not have run"
    );
    assert!(
        histogram_sample_count(&snapshot, "tsoracle.window.extension_latency") >= 1,
        "tsoracle.window.extension_latency recorded no samples"
    );
    assert!(
        counter_value(&snapshot, "tsoracle.leader_transition.total") >= 2,
        "tsoracle.leader_transition.total missed at least one of Leader/Follower transitions"
    );
    assert!(
        histogram_sample_count(&snapshot, "tsoracle.leader_transition.fence_latency") >= 1,
        "tsoracle.leader_transition.fence_latency recorded no samples"
    );
    assert!(
        counter_value(&snapshot, "tsoracle.not_leader.total") >= 1,
        "tsoracle.not_leader.total never incremented despite a NOT_LEADER rejection"
    );
}
