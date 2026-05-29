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

//! In-process GetSeq handler tests against a FileDriver-backed server.
//!
//! FileDriver always emits `Leader { epoch: Epoch::ZERO }` on open, so the
//! server fences and enters Serving automatically — no explicit
//! `become_leader` call is needed. These tests verify: contiguous block
//! allocation, sequential ordinals across calls, and InvalidArgument for bad
//! count/key inputs.

use std::time::Duration;

use tsoracle_driver_file::FileDriver;
use tsoracle_proto::v1::{GetSeqRequest, tso_service_client::TsoServiceClient};
use tsoracle_server::Server;
use tsoracle_server::test_support::{boot_server, wait_for_grpc_handshake, wait_until_serving};

/// Boot a FileDriver-backed server, drive it to Serving, and return the
/// booted server, a connected gRPC client, and the temp directory handle.
/// The caller must keep the `TempDir` alive for the duration of the test —
/// dropping it deletes the directory that the FileDriver is using.
async fn boot_file_server() -> (
    tsoracle_server::test_support::BootedServer,
    TsoServiceClient<tonic::transport::Channel>,
    tempfile::TempDir,
) {
    let dir = tempfile::tempdir().unwrap();
    // FileDriver::open_or_init already returns Arc<FileDriver>; no additional
    // wrapping needed — pass it directly to consensus_driver, which accepts
    // Arc<dyn ConsensusDriver> and will coerce Arc<FileDriver> accordingly.
    let driver = FileDriver::open_or_init(dir.path()).unwrap();

    let server = Server::builder()
        .consensus_driver(driver)
        .window_ahead(Duration::from_secs(1))
        .failover_advance(Duration::from_millis(500))
        .build()
        .unwrap();

    let mut booted = boot_server(server).await;

    // FileDriver emits Leader{Epoch::ZERO} immediately on open; wait for the
    // fence to complete and the server to publish Serving.
    wait_until_serving(&mut booted.state_rx).await;
    wait_for_grpc_handshake(booted.addr, Duration::from_secs(5))
        .await
        .expect("tonic never accepted gRPC handshake");

    let client = TsoServiceClient::connect(format!("http://{}", booted.addr))
        .await
        .unwrap();

    (booted, client, dir)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_seq_returns_contiguous_blocks() {
    let (booted, mut client, _dir) = boot_file_server().await;

    // First block for "orders": [0, 5).
    let resp = client
        .get_seq(GetSeqRequest {
            key: "orders".to_string(),
            count: 5,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.key, "orders");
    assert_eq!(resp.start, 0);
    assert_eq!(resp.count, 5);
    assert!(resp.epoch.is_some(), "epoch must be present on success");

    // Second block: [5, 8) — contiguous, no gap.
    let resp2 = client
        .get_seq(GetSeqRequest {
            key: "orders".to_string(),
            count: 3,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp2.start, 5);
    assert_eq!(resp2.count, 3);
    // Epoch is stable across calls within the same leader term.
    assert_eq!(resp2.epoch, resp.epoch);

    booted.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_seq_count_zero_returns_invalid_argument() {
    let (booted, mut client, _dir) = boot_file_server().await;

    let err = client
        .get_seq(GetSeqRequest {
            key: "orders".to_string(),
            count: 0,
        })
        .await
        .unwrap_err();
    assert_eq!(
        err.code(),
        tonic::Code::InvalidArgument,
        "count=0 must return InvalidArgument, got: {err:?}"
    );

    booted.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_seq_empty_key_returns_invalid_argument() {
    let (booted, mut client, _dir) = boot_file_server().await;

    let err = client
        .get_seq(GetSeqRequest {
            key: String::new(),
            count: 1,
        })
        .await
        .unwrap_err();
    assert_eq!(
        err.code(),
        tonic::Code::InvalidArgument,
        "empty key must return InvalidArgument, got: {err:?}"
    );

    booted.shutdown().await.unwrap();
}
