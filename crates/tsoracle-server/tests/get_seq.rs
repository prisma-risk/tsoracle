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
    // Epoch must be present on success and pinned to the FileDriver's
    // Epoch::ZERO (both halves zero) — guards against a future client ever
    // mistaking a zero-valued epoch for an absent one.
    let epoch = resp.epoch.expect("epoch must be present on success");
    assert_eq!((epoch.hi, epoch.lo), (0, 0));

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

/// A driver that is a healthy leader for the timestamp path but has no dense
/// support (it inherits the trait's default `advance_dense` → `DenseUnsupported`,
/// exactly like the openraft/paxos drivers do until their follow-up PRs). Used
/// to assert how the GetSeq handler surfaces an unsupported driver.
struct DenseUnsupportedDriver;

/// A driver that supports dense sequences but returns `DenseNotActivated` from
/// `advance_dense`, simulating a cluster that has not yet activated write
/// version 5 across all members.
struct DenseNotActivatedDriver;

#[async_trait::async_trait]
impl tsoracle_consensus::ConsensusDriver for DenseNotActivatedDriver {
    fn leadership_events(
        &self,
    ) -> std::pin::Pin<Box<dyn futures::Stream<Item = tsoracle_consensus::LeaderState> + Send>>
    {
        use futures::StreamExt;
        Box::pin(
            futures::stream::once(async {
                tsoracle_consensus::LeaderState::Leader {
                    epoch: tsoracle_core::Epoch::ZERO,
                }
            })
            .chain(futures::stream::pending()),
        )
    }

    async fn load_high_water(&self) -> Result<u64, tsoracle_consensus::ConsensusError> {
        Ok(0)
    }

    async fn persist_high_water(
        &self,
        at_least: u64,
        _epoch: tsoracle_core::Epoch,
    ) -> Result<u64, tsoracle_consensus::ConsensusError> {
        Ok(at_least)
    }

    async fn advance_dense(
        &self,
        _key: &tsoracle_core::SeqKey,
        _count: u32,
        _epoch: tsoracle_core::Epoch,
    ) -> Result<u64, tsoracle_consensus::ConsensusError> {
        Err(tsoracle_consensus::ConsensusError::DenseNotActivated {
            required: 5,
            active: 4,
        })
    }
}

#[async_trait::async_trait]
impl tsoracle_consensus::ConsensusDriver for DenseUnsupportedDriver {
    fn leadership_events(
        &self,
    ) -> std::pin::Pin<Box<dyn futures::Stream<Item = tsoracle_consensus::LeaderState> + Send>>
    {
        use futures::StreamExt;
        // Honour the first-item-synchronous contract by emitting Leader
        // immediately, then stay open (`pending`) so the fence holds Serving — a
        // finished stream would read as the driver relinquishing leadership.
        Box::pin(
            futures::stream::once(async {
                tsoracle_consensus::LeaderState::Leader {
                    epoch: tsoracle_core::Epoch::ZERO,
                }
            })
            .chain(futures::stream::pending()),
        )
    }

    async fn load_high_water(&self) -> Result<u64, tsoracle_consensus::ConsensusError> {
        Ok(0)
    }

    async fn persist_high_water(
        &self,
        at_least: u64,
        _epoch: tsoracle_core::Epoch,
    ) -> Result<u64, tsoracle_consensus::ConsensusError> {
        Ok(at_least)
    }
    // advance_dense / load_dense_seq: trait defaults → DenseUnsupported.
}

/// A leader whose driver does not support dense sequences must surface GetSeq as
/// `UNIMPLEMENTED`, not be disguised as a NOT_LEADER redirect. Folding
/// `DenseUnsupported` into the not-leader status makes a healthy leader claim it
/// is not the leader, sending the client into a pointless election ride-out and
/// polluting not-leader metrics. `UNIMPLEMENTED` is immediately diagnosable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_seq_unsupported_driver_returns_unimplemented() {
    let server = Server::builder()
        .consensus_driver(std::sync::Arc::new(DenseUnsupportedDriver))
        .window_ahead(Duration::from_secs(1))
        .failover_advance(Duration::from_millis(500))
        .build()
        .unwrap();

    let mut booted = boot_server(server).await;
    wait_until_serving(&mut booted.state_rx).await;
    wait_for_grpc_handshake(booted.addr, Duration::from_secs(5))
        .await
        .expect("tonic never accepted gRPC handshake");
    let mut client = TsoServiceClient::connect(format!("http://{}", booted.addr))
        .await
        .unwrap();

    let err = client
        .get_seq(GetSeqRequest {
            key: "orders".to_string(),
            count: 1,
        })
        .await
        .unwrap_err();
    assert_eq!(
        err.code(),
        tonic::Code::Unimplemented,
        "a leader without dense support must return UNIMPLEMENTED, got: {err:?}"
    );

    booted.shutdown().await.unwrap();
}

/// A leader whose driver returns `DenseNotActivated` must surface GetSeq as
/// `FAILED_PRECONDITION` with a "not yet activated" message — not as `INTERNAL`
/// (the catch-all). The response carries no leader-hint trailer so the client
/// surfaces it definitively rather than riding out an election.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_seq_dense_not_activated_returns_failed_precondition() {
    let server = Server::builder()
        .consensus_driver(std::sync::Arc::new(DenseNotActivatedDriver))
        .window_ahead(Duration::from_secs(1))
        .failover_advance(Duration::from_millis(500))
        .build()
        .unwrap();

    let mut booted = boot_server(server).await;
    wait_until_serving(&mut booted.state_rx).await;
    wait_for_grpc_handshake(booted.addr, Duration::from_secs(5))
        .await
        .expect("tonic never accepted gRPC handshake");
    let mut client = TsoServiceClient::connect(format!("http://{}", booted.addr))
        .await
        .unwrap();

    let err = client
        .get_seq(GetSeqRequest {
            key: "orders".to_string(),
            count: 1,
        })
        .await
        .unwrap_err();
    assert_eq!(
        err.code(),
        tonic::Code::FailedPrecondition,
        "DenseNotActivated must return FAILED_PRECONDITION, got: {err:?}"
    );
    assert!(
        err.message().contains("not yet activated"),
        "message must mention 'not yet activated', got: {:?}",
        err.message()
    );

    booted.shutdown().await.unwrap();
}

/// `ServerBuilder::max_seq_count` overrides the default per-call ceiling: a
/// request above the configured cap is rejected with InvalidArgument, while one
/// at the cap is served. Proves the cap is the server's configured value, not
/// the `DEFAULT_MAX_SEQ_COUNT` constant.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_seq_honours_configured_max_seq_count() {
    let dir = tempfile::tempdir().unwrap();
    let driver = FileDriver::open_or_init(dir.path()).unwrap();
    let server = Server::builder()
        .consensus_driver(driver)
        .window_ahead(Duration::from_secs(1))
        .failover_advance(Duration::from_millis(500))
        .max_seq_count(2)
        .build()
        .unwrap();

    let mut booted = boot_server(server).await;
    wait_until_serving(&mut booted.state_rx).await;
    wait_for_grpc_handshake(booted.addr, Duration::from_secs(5))
        .await
        .expect("tonic never accepted gRPC handshake");
    let mut client = TsoServiceClient::connect(format!("http://{}", booted.addr))
        .await
        .unwrap();

    // count = 3 exceeds the configured cap of 2 → InvalidArgument.
    let err = client
        .get_seq(GetSeqRequest {
            key: "orders".to_string(),
            count: 3,
        })
        .await
        .unwrap_err();
    assert_eq!(
        err.code(),
        tonic::Code::InvalidArgument,
        "count above the configured max_seq_count must be rejected, got: {err:?}"
    );

    // count = 2 is exactly at the cap → served.
    let resp = client
        .get_seq(GetSeqRequest {
            key: "orders".to_string(),
            count: 2,
        })
        .await
        .expect("count at the configured cap must be served")
        .into_inner();
    assert_eq!(resp.count, 2);
    assert_eq!(resp.start, 0);

    booted.shutdown().await.unwrap();
}
