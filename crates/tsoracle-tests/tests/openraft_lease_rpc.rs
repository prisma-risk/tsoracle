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

//! Lease RPCs through the real server on an in-process, single-node openraft node.
//!
//! The driver tests prove the openraft side in isolation. This test proves the two halves agree: the epoch the server's fence seeds its allocator with is the raft term the `SetLeases` entry commits in, so leases are accepted rather than fenced, and the not-activated refusal reaches a client as the documented status.

use std::collections::BTreeMap;
use std::time::Duration;

use tokio_stream::StreamExt;
use tonic::Code;
use tsoracle_consensus::LeaderState;
use tsoracle_proto::v1::{AcquireLeaseRequest, ReleaseLeaseRequest, RenewLeaseRequest};
use tsoracle_server::Server;
use tsoracle_server::test_support::{boot_server, connect_tso_client, wait_until_serving};
use tsoracle_standalone::{MemberAddr, OpenraftConfig, RaftTuning, build_openraft_with_listeners};

const TTL_MS: u64 = 10_000;
/// The openraft lease write version; the server test only needs its value.
const LEASE_WRITE_VERSION: u8 = 7;

fn acquire(holder: &[u8]) -> AcquireLeaseRequest {
    AcquireLeaseRequest {
        holder: holder.to_vec(),
        holder_epoch: 1,
        ttl_ms: TTL_MS,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lease_rpcs_round_trip_on_openraft_after_activation() {
    let dir = tempfile::tempdir().unwrap();
    let raft_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let raft_addr = raft_listener.local_addr().unwrap();
    let mut members = BTreeMap::new();
    members.insert(
        1,
        MemberAddr {
            raft_addr: raft_addr.to_string(),
            service_endpoint: "127.0.0.1:1".to_string(),
            admin_endpoint: "127.0.0.1:2".to_string(),
        },
    );
    let node = build_openraft_with_listeners(
        OpenraftConfig {
            id: 1,
            raft_addr,
            raft_dir: dir.path().join("raft"),
            bootstrap: true,
            initial_membership: Some(members),
            tuning: RaftTuning {
                heartbeat_ms: 50,
                election_min_ms: 150,
                election_max_ms: 300,
            },
            peer_tls: None,
            admin_listen: None,
            admin_tls: None,
            allow_insecure_peer: false,
        },
        raft_listener,
        None,
    )
    .await
    .expect("build openraft node");

    let mut events = node.driver.leadership_events();
    tokio::time::timeout(Duration::from_secs(15), async {
        while let Some(state) = events.next().await {
            if matches!(state, LeaderState::Leader { .. }) {
                return;
            }
        }
    })
    .await
    .expect("single-node openraft elected itself");
    drop(events);

    let server = Server::builder()
        .consensus_driver(node.driver.clone())
        .build()
        .unwrap();
    let mut booted = boot_server(server).await;
    wait_until_serving(&mut booted.state_rx).await;
    let mut client = connect_tso_client(booted.addr).await;

    // Before activation the RPC is refused definitively, not as a leader redirect and not as unimplemented.
    let refused = client
        .acquire_lease(acquire(b"group-a"))
        .await
        .expect_err("acquire before lease activation must be refused");
    assert_eq!(refused.code(), Code::FailedPrecondition);
    assert!(
        refused.message().contains("lease format not yet activated"),
        "unexpected status: {refused:?}"
    );

    node.admin
        .activate_format(LEASE_WRITE_VERSION)
        .await
        .expect("activate the lease format");

    let granted = client
        .acquire_lease(acquire(b"group-a"))
        .await
        .expect("acquire after activation")
        .into_inner();
    let other = client
        .acquire_lease(acquire(b"group-b"))
        .await
        .expect("second holder")
        .into_inner();
    let durable = node.driver.load_leases().await.expect("load leases");
    let mut durable_ids: Vec<u64> = durable.iter().map(|record| record.lease_id).collect();
    durable_ids.sort_unstable();
    let mut granted_ids = vec![granted.lease_id, other.lease_id];
    granted_ids.sort_unstable();
    assert_eq!(
        durable_ids, granted_ids,
        "both grants are durable in the raft state machine"
    );

    let renewed = client
        .renew_lease(RenewLeaseRequest {
            lease_id: granted.lease_id,
        })
        .await
        .expect("renew")
        .into_inner();
    assert!(renewed.ts_upper_bound >= granted.ts_upper_bound);

    client
        .release_lease(ReleaseLeaseRequest {
            lease_id: other.lease_id,
        })
        .await
        .expect("release");
    let durable = node.driver.load_leases().await.expect("load leases");
    assert_eq!(
        durable
            .iter()
            .map(|record| record.lease_id)
            .collect::<Vec<_>>(),
        vec![granted.lease_id],
        "release removes exactly the released lease from durable state"
    );

    booted.shutdown().await.unwrap();
    node.shutdown().await;
}
