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

//! Recorder-fake test for the documented `tsoracle.client.*` metrics catalog.
//!
//! Drives three back-to-back scenarios through a process-global
//! `DebuggingRecorder` and asserts every documented client signal fires at
//! least once. Scenarios are sequenced inside one test binary because the
//! `metrics` recorder slot is process-global; running each scenario as its
//! own `#[tokio::test]` would race on `install()`.
//!
//! One documented client signal is deliberately not exercised here:
//! `tsoracle.client.driver.abandoned_waiters.total` fires only when a caller
//! drops its `get_ts()` future *after* the driver has dispatched but *before*
//! delivery — a race that is timing-dependent to force end to end. It has
//! deterministic unit coverage in `tsoracle_client::driver` instead, where the
//! delivery helper is driven directly against a dropped receiver.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use metrics_util::{
    CompositeKey, MetricKind,
    debugging::{DebugValue, DebuggingRecorder, Snapshotter},
};
use tokio::net::TcpListener;
use tonic::{
    Request, Response, Status,
    metadata::{BinaryMetadataValue, MetadataKey},
    transport::Server as TonicServer,
};
use tsoracle_client::Client;
use tsoracle_core::{Epoch, PeerEndpoint};
use tsoracle_proto::v1::{
    GetTsRequest, GetTsResponse, LEADER_HINT_TRAILER_KEY,
    tso_service_server::{TsoService, TsoServiceServer},
};
use tsoracle_server::test_fakes::InMemoryDriver;
use tsoracle_server::test_support::{
    boot_server, wait_for_grpc_handshake, wait_until, wait_until_serving,
};
use tsoracle_server::{Server, ServingState};

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
    let mut total = 0u64;
    for (composite, _unit, _desc, value) in snapshot {
        if composite.kind() == MetricKind::Counter && composite.key().name() == name {
            if let DebugValue::Counter(n) = value {
                total = total.saturating_add(*n);
            }
        }
    }
    total
}

fn counter_value_with_label(
    snapshot: &[RecordedMetric],
    name: &str,
    label_key: &str,
    label_value: &str,
) -> u64 {
    for (composite, _unit, _desc, value) in snapshot {
        if composite.kind() != MetricKind::Counter || composite.key().name() != name {
            continue;
        }
        let matches_label = composite
            .key()
            .labels()
            .any(|l| l.key() == label_key && l.value() == label_value);
        if matches_label {
            if let DebugValue::Counter(n) = value {
                return *n;
            }
        }
    }
    0
}

fn histogram_sample_count(snapshot: &[RecordedMetric], name: &str) -> usize {
    let mut total = 0usize;
    for (composite, _unit, _desc, value) in snapshot {
        if composite.kind() == MetricKind::Histogram && composite.key().name() == name {
            if let DebugValue::Histogram(samples) = value {
                total = total.saturating_add(samples.len());
            }
        }
    }
    total
}

fn gauge_registered(snapshot: &[RecordedMetric], name: &str) -> bool {
    snapshot.iter().any(|(composite, _unit, _desc, value)| {
        composite.kind() == MetricKind::Gauge
            && composite.key().name() == name
            && matches!(value, DebugValue::Gauge(_))
    })
}

/// Bind a TCP listener, capture its assigned port, and drop the listener.
/// The OS will not re-assign that port to another process within the same
/// test run; subsequent connect attempts will fail with ECONNREFUSED, which
/// is exactly the `connect.failures` signal we want to exercise.
async fn closed_port_endpoint() -> String {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local_addr");
    drop(listener);
    format!("http://{addr}")
}

/// Minimal `TsoService` that always returns FAILED_PRECONDITION with a
/// deliberately-malformed `tsoracle-leader-hint-bin` trailer. Bytes are
/// chosen to fail `LeaderHint::decode` (field tag 1 declares a 5-byte
/// length-delimited string but only 2 bytes follow) — exercising the
/// `LeaderHintLookup::Malformed` arm in `decode_leader_hint`.
struct MalformedHintService;

#[tonic::async_trait]
impl TsoService for MalformedHintService {
    async fn get_ts(
        &self,
        _request: Request<GetTsRequest>,
    ) -> Result<Response<GetTsResponse>, Status> {
        let mut status = Status::failed_precondition("not leader");
        let key = MetadataKey::from_bytes(LEADER_HINT_TRAILER_KEY.as_bytes())
            .expect("trailer key is ascii");
        let garbage: &[u8] = &[0x0a, 0x05, b'h', b'i'];
        status
            .metadata_mut()
            .insert_bin(key, BinaryMetadataValue::from_bytes(garbage));
        Err(status)
    }

    async fn get_current_max_safe(
        &self,
        _request: tonic::Request<tsoracle_proto::v1::GetCurrentMaxSafeRequest>,
    ) -> Result<tonic::Response<tsoracle_proto::v1::GetCurrentMaxSafeResponse>, tonic::Status> {
        Ok(tonic::Response::new(
            tsoracle_proto::v1::GetCurrentMaxSafeResponse::default(),
        ))
    }
}

/// Bind a malformed-hint fake server on a random port. Returns its socket
/// address; the spawned task is detached and lives until the runtime shuts
/// down at end of test.
async fn boot_malformed_hint_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let server = TonicServer::builder().add_service(TsoServiceServer::new(MalformedHintService));
    tokio::spawn(async move {
        let _ = server.serve_with_incoming(incoming).await;
    });
    wait_for_grpc_handshake(addr, Duration::from_secs(5))
        .await
        .expect("malformed-hint server never accepted gRPC handshake");
    addr
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn emits_documented_client_signals_end_to_end() {
    let snapshotter = install_recorder();

    // ── Scenario 1: follower→leader pivot via in-band hint ──────────────
    //
    // Drives connect.duration (cache-miss on both A and B), not_leader.total,
    // leader_pivots.total, retries.total{reason=not_leader}, and both driver
    // gauges (queue_depth, in_flight).
    let driver_a = Arc::new(InMemoryDriver::new());
    let driver_b = Arc::new(InMemoryDriver::new());
    let server_a = Server::builder()
        .consensus_driver(driver_a.clone())
        .build()
        .unwrap();
    let server_b = Server::builder()
        .consensus_driver(driver_b.clone())
        .build()
        .unwrap();
    let mut booted_a = boot_server(server_a).await;
    let mut booted_b = boot_server(server_b).await;
    driver_b.become_leader(Epoch(1));
    driver_a.become_follower(Some(
        PeerEndpoint::try_from(booted_b.addr.to_string()).unwrap(),
    ));
    wait_until_serving(&mut booted_b.state_rx).await;
    wait_until(&mut booted_a.state_rx, |s| {
        matches!(
            s,
            ServingState::NotServing {
                leader_endpoint: Some(_),
                ..
            }
        )
    })
    .await;
    wait_for_grpc_handshake(booted_a.addr, Duration::from_secs(5))
        .await
        .expect("server A handshake");
    wait_for_grpc_handshake(booted_b.addr, Duration::from_secs(5))
        .await
        .expect("server B handshake");

    let client_pivot = Client::connect(vec![format!("http://{}", booted_a.addr)])
        .await
        .expect("pivot-scenario client connect");
    client_pivot
        .get_ts()
        .await
        .expect("scenario 1 must succeed via hint");

    // ── Scenario 2: connect failure emits its connect signals ──────────
    //
    // A single unreachable endpoint is dialed, hits ECONNREFUSED, and
    // increments connect.failures + retries{connect_failure} before the
    // worklist empties and the request errors out. The sole endpoint keeps
    // this deterministic: iter_round_robin now starts the configured tail at
    // a *random* rotation offset (issue #342), so a two-endpoint
    // [dead, live] list would dial the live peer first roughly half the time
    // and never touch the dead one. The "fall through to a live peer and
    // recover" path is covered by the retry-loop unit tests; here we only
    // need the connect-failure signals, which fire regardless of any
    // follow-up endpoint.
    let dead_endpoint = closed_port_endpoint().await;
    let client_unreachable = Client::connect(vec![dead_endpoint])
        .await
        .expect("unreachable-endpoint client connect");
    client_unreachable
        .get_ts()
        .await
        .expect_err("scenario 2 must surface an error when the only endpoint is unreachable");

    // ── Scenario 3: FAILED_PRECONDITION carrying a malformed hint trailer
    //
    // The only peer returns a hint trailer whose bytes don't decode as
    // `LeaderHint`. retry::issue_rpc reaches `LeaderHintLookup::Malformed`,
    // counts a decode failure, and surfaces the FAILED_PRECONDITION up to
    // the caller because there's no follow-up endpoint to pivot to.
    let malformed_addr = boot_malformed_hint_server().await;
    let client_malformed = Client::connect(vec![format!("http://{}", malformed_addr)])
        .await
        .expect("malformed-hint client connect");
    let err = client_malformed
        .get_ts()
        .await
        .expect_err("malformed-hint scenario must surface FAILED_PRECONDITION");
    assert!(
        matches!(
            err,
            tsoracle_client::ClientError::Rpc(ref status)
                if status.code() == tonic::Code::FailedPrecondition,
        ),
        "expected ClientError::Rpc(FAILED_PRECONDITION), got {err:?}",
    );

    booted_a.shutdown().await.unwrap();
    booted_b.shutdown().await.unwrap();

    let snapshot: Vec<RecordedMetric> = snapshotter.snapshot().into_vec();

    // Counters with shape we drove directly.
    assert!(
        counter_value(&snapshot, "tsoracle.client.not_leader.total") >= 1,
        "tsoracle.client.not_leader.total never incremented despite a FAILED_PRECONDITION reply"
    );
    assert!(
        counter_value(&snapshot, "tsoracle.client.leader_pivots.total") >= 1,
        "tsoracle.client.leader_pivots.total never incremented despite a follower→leader hint"
    );
    assert!(
        counter_value_with_label(
            &snapshot,
            "tsoracle.client.retries.total",
            "reason",
            "not_leader",
        ) >= 1,
        "retries.total{{reason=not_leader}} did not fire on the hint-pivot scenario"
    );
    assert!(
        counter_value_with_label(
            &snapshot,
            "tsoracle.client.retries.total",
            "reason",
            "connect_failure",
        ) >= 1,
        "retries.total{{reason=connect_failure}} did not fire when an endpoint was unreachable"
    );
    assert!(
        counter_value(&snapshot, "tsoracle.client.connect.failures.total") >= 1,
        "tsoracle.client.connect.failures.total never incremented for the closed-port endpoint"
    );
    assert!(
        counter_value(
            &snapshot,
            "tsoracle.client.leader_hint.decode_failures.total"
        ) >= 1,
        "tsoracle.client.leader_hint.decode_failures.total never incremented for the \
         malformed-trailer scenario"
    );

    // Histogram: connect.duration must have at least one sample — across all
    // three scenarios we performed multiple cache-miss connects.
    assert!(
        histogram_sample_count(&snapshot, "tsoracle.client.connect.duration") >= 1,
        "tsoracle.client.connect.duration recorded no samples; histogram path never executed"
    );

    // Gauges: any get_ts call flows through driver_task and sets both
    // queue_depth (≥0 on enqueue) and in_flight (1 on dispatch, 0 on
    // completion). We assert registration rather than a specific level
    // because both gauges land at 0 once the driver settles.
    assert!(
        gauge_registered(&snapshot, "tsoracle.client.driver.queue_depth"),
        "tsoracle.client.driver.queue_depth was never registered with the recorder"
    );
    assert!(
        gauge_registered(&snapshot, "tsoracle.client.driver.in_flight"),
        "tsoracle.client.driver.in_flight was never registered with the recorder"
    );
}
