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

//! Shared test scaffolding for `tsoracle-standalone` integration tests.
//!
//! Each `tests/*.rs` declares `mod common;` and imports via `use common::*;`.
//! Rust compiles this module per integration-test binary (a known minor
//! duplication; negligible at our scale).

#![allow(dead_code)] // each test binary uses a subset; allow the rest

use std::net::SocketAddr;

use tokio::net::TcpListener;

/// Holds an open ephemeral-port listener so the kernel will not re-assign
/// that port to any other `bind(:0)` in the system. Drop just before the
/// consumer's own bind to keep the residual TOCTOU window microscopic.
pub struct PortLease {
    _listener: Option<TcpListener>,
}

/// Bind an ephemeral port and return its address paired with a `PortLease`
/// holding the listener open. Drop the `PortLease` at the exact moment the
/// consumer is about to bind the address.
///
/// The earlier helper just dropped the listener and returned the address —
/// a classic TOCTOU race. Under parallel `#[tokio::test]` execution (each
/// integration-test binary spreads tests across worker threads), the kernel
/// would re-issue a freshly-freed ephemeral port to another test's
/// `bind(:0)` before the original test's consumer got around to binding,
/// causing `EADDRINUSE` panics. Holding the listener until the consumer is
/// ready makes the port unassignable for the entire lease window.
pub async fn lease_port() -> (SocketAddr, PortLease) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("read local addr");
    (
        addr,
        PortLease {
            _listener: Some(listener),
        },
    )
}
