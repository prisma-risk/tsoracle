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
/// that port to any other `bind(:0)` in the system. Either drop it (the
/// kernel frees the port; the consumer must immediately rebind, racing any
/// other `bind(:0)` in the meantime) or hand the listener directly to a
/// consumer that supports pre-bound listeners — eliminating the close/rebind
/// window entirely. The latter is the only race-free path; see
/// `into_listener`.
pub struct PortLease {
    listener: Option<TcpListener>,
}

impl PortLease {
    /// Consume the lease and return the underlying open `TcpListener`. Hand
    /// this to a build path that accepts a pre-bound listener (e.g.
    /// `tsoracle_standalone::build_openraft_with_listeners`) so the port is
    /// never released between lease and use.
    pub fn into_listener(mut self) -> TcpListener {
        self.listener
            .take()
            .expect("PortLease already consumed (into_listener called twice?)")
    }
}

/// Bind an ephemeral port and return its address paired with a `PortLease`
/// holding the listener open. Prefer `PortLease::into_listener` to hand the
/// bound socket directly to the consumer; only `drop` the lease when the
/// consumer cannot accept a pre-bound listener (legacy path).
///
/// History: the original helper just dropped the listener and returned the
/// address — a classic TOCTOU race. Under parallel `#[tokio::test]` execution
/// (each integration-test binary spreads tests across worker threads), the
/// kernel would re-issue a freshly-freed ephemeral port to another test's
/// `bind(:0)` before the original test's consumer got around to binding,
/// causing `EADDRINUSE` panics. Holding the listener narrowed the window;
/// `into_listener` closes it.
pub async fn lease_port() -> (SocketAddr, PortLease) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("read local addr");
    (
        addr,
        PortLease {
            listener: Some(listener),
        },
    )
}

/// Mint a self-signed CA + node leaf (SAN `127.0.0.1`) and write all
/// three PEMs into `dir`. Returns `(cert, key, ca)` as paths — the
/// triple consumed by `tsoracle_standalone::PeerTlsConfig`.
///
/// Suitable for build-time guard tests: the cert is well-formed enough
/// to pass `peer_tls::build_peer_tls` dry-validation, which is all the
/// `peer_tls.is_some()` arm of the guard needs.
pub fn write_peer_pems(
    dir: &std::path::Path,
) -> (std::path::PathBuf, std::path::PathBuf, std::path::PathBuf) {
    use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};

    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::new(vec!["tso-peer-ca".into()]).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();

    let node_key = KeyPair::generate().unwrap();
    let node_params = CertificateParams::new(vec!["127.0.0.1".into()]).unwrap();
    let node_cert = node_params.signed_by(&node_key, &ca_cert, &ca_key).unwrap();

    let cert_path = dir.join("peer.crt");
    let key_path = dir.join("peer.key");
    let ca_path = dir.join("peer-ca.crt");
    std::fs::write(&cert_path, node_cert.pem()).unwrap();
    std::fs::write(&key_path, node_key.serialize_pem()).unwrap();
    std::fs::write(&ca_path, ca_cert.pem()).unwrap();
    (cert_path, key_path, ca_path)
}
