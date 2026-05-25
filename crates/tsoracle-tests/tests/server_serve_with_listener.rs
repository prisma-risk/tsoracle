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

//! Verifies `Server::serve_with_listener` binds a caller-owned `TcpListener`,
//! so callers using `127.0.0.1:0` can capture the OS-picked port before
//! clients connect.

use std::sync::Arc;
use std::time::Duration;
use tsoracle_core::Epoch;
use tsoracle_server::Server;
use tsoracle_server::test_fakes::InMemoryDriver;
use tsoracle_server::test_support::{boot_server, wait_for_grpc_handshake, wait_until_serving};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serve_with_listener_uses_caller_owned_socket() {
    let driver = Arc::new(InMemoryDriver::new());
    driver.become_leader(Epoch(1));

    let server = Server::builder()
        .consensus_driver(driver.clone())
        .build()
        .unwrap();

    let mut booted = boot_server(server).await;
    assert_ne!(booted.addr.port(), 0, "OS must have picked a real port");

    wait_until_serving(&mut booted.state_rx).await;
    wait_for_grpc_handshake(booted.addr, Duration::from_secs(5))
        .await
        .expect("tonic never accepted gRPC handshake");

    // Make a real client call against the captured port.
    let endpoint = format!("http://{}", booted.addr);
    let client = tsoracle_client::Client::connect(vec![endpoint])
        .await
        .expect("client connect");
    let ts = client.get_ts().await.expect("get_ts");
    // Verify the server returned a real (non-zero) timestamp.
    assert!(
        ts.physical_ms() > 1_700_000_000_000,
        "expected a real wall-clock timestamp, got physical_ms={}",
        ts.physical_ms()
    );

    drop(client);
    booted.shutdown().await.expect("server exited Err");
}
