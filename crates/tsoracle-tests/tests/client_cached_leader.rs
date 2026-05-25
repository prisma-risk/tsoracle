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

//! End-to-end coverage for `Client::cached_leader`, the read-only diagnostic
//! accessor onto the channel pool's leader cache (issue #96). The pool-level
//! cache mechanics are unit-tested in `tsoracle-client`; this test pins the
//! observable client-facing contract through the real gRPC stack: the cache
//! is empty until a `GetTs` completes, and a successful round-trip seats the
//! endpoint that served it.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tsoracle_client::Client;
use tsoracle_core::Epoch;
use tsoracle_server::Server;
use tsoracle_server::test_fakes::InMemoryDriver;
use tsoracle_server::test_support::{boot_server, wait_until_serving};

/// A single successful `get_ts` is the only proof that every layer (tonic
/// accept loop, gRPC handshake, leader fence, allocator) is live; the early
/// attempts may race server readiness, so retry within a bounded budget.
async fn first_successful_get_ts(client: &Client, budget: Duration) {
    let deadline = Instant::now() + budget;
    loop {
        if client.get_ts().await.is_ok() {
            return;
        }
        if Instant::now() >= deadline {
            panic!("server never became responsive within {budget:?}");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// `cached_leader()` reports `None` before any RPC and converges to the
/// endpoint that served a successful `GetTs`. With a single configured
/// endpoint pointing at the booted leader, the dialed endpoint and the
/// cached leader are the same string (`record_success` caches the worklist
/// entry the client dialed), so we can assert the exact address.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cached_leader_converges_to_the_server_after_a_successful_rpc() {
    let driver = Arc::new(InMemoryDriver::new());
    let server = Server::builder()
        .consensus_driver(driver.clone())
        .build()
        .unwrap();
    let mut booted = boot_server(server).await;
    driver.become_leader(Epoch(1));
    wait_until_serving(&mut booted.state_rx).await;

    let endpoint = booted.addr.to_string();
    let client = Client::connect(vec![endpoint.clone()])
        .await
        .expect("connect with a non-empty endpoint list must succeed");

    // No RPC has been issued yet, so the leader cache is empty.
    assert_eq!(
        client.cached_leader(),
        None,
        "a freshly connected client has observed no leader"
    );

    first_successful_get_ts(&client, Duration::from_secs(5)).await;

    // The completed round-trip seated the endpoint that served it.
    assert_eq!(
        client.cached_leader().as_deref(),
        Some(endpoint.as_str()),
        "a successful GetTs must cache the serving endpoint as the leader"
    );

    booted.shutdown().await.expect("server shutdown");
}
