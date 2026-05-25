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

//! Bootstrap helpers for integration tests that spin up a [`crate::Server`]
//! on `127.0.0.1:0`, drive it to a known state, and tear it down explicitly.
//!
//! Tests across `tsoracle-server` and `tsoracle-client` repeatedly bind a
//! random TCP port, spawn `serve_with_listener` (or the `into_router` +
//! tonic pair), and poll the [`ServingState`] watch channel before making
//! RPCs. This module collapses that boilerplate behind two `boot_*`
//! functions plus a small set of wait helpers, leaving each test's
//! per-scenario configuration (builder knobs, custom drivers, leader vs
//! follower) intact at the call site.

// This module compiles only for tests or behind the `test-support` feature.
// The crate-level lint that warns against `unwrap`/`expect` in non-test
// builds still applies under `feature = "test-support"` without `cfg(test)`,
// so we opt out here — expressive panics are the right idiom for test
// helpers that fail loudly on bring-up errors.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use tokio::net::TcpListener;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::service::Routes;
use tonic::transport::{Endpoint, Server as TonicServer};

use crate::{Server, ServerError, ServingState};

#[cfg(any(feature = "tls-rustls", feature = "tls-native"))]
use crate::leader_hint::not_leader_status;
#[cfg(any(feature = "tls-rustls", feature = "tls-native"))]
use tsoracle_proto::v1::{
    GetTsRequest, GetTsResponse, LeaderHint,
    tso_service_server::{TsoService, TsoServiceServer},
};

/// A running [`Server`] with its captured bind address, observable
/// [`ServingState`], a graceful-shutdown signal, and the spawned task's
/// `JoinHandle`.
///
/// Dropping a `BootedServer` does not shut the server down — the spawned
/// task continues until the test process exits. Most tests should call
/// [`BootedServer::shutdown`] to send the shutdown signal and join the
/// task. Tests that probe failure modes where shutdown never fires
/// (`futures::future::pending`-style waits, watch-task panics) can move
/// `serve_handle` out directly and observe its outcome.
pub struct BootedServer {
    /// Address bound on `127.0.0.1` with an OS-picked port.
    pub addr: SocketAddr,
    /// Live receiver for the server's [`ServingState`]. Tests typically
    /// use [`wait_until`] / [`wait_until_serving`] /
    /// [`wait_until_not_serving`] against this; immutable borrows after
    /// waiting (e.g. `matches!(*state_rx.borrow(), ServingState::Serving)`)
    /// work fine because `watch::Receiver` exposes both.
    pub state_rx: watch::Receiver<ServingState>,
    /// Handle for the task running [`Server::serve_with_listener`].
    pub serve_handle: JoinHandle<Result<(), ServerError>>,
    shutdown_tx: oneshot::Sender<()>,
}

impl BootedServer {
    /// Send the shutdown signal and join the spawned task. Returns the
    /// server's exit result. The send is best-effort: if the server has
    /// already exited (for example because its leader-watch task died),
    /// the receiver is gone and the send returns `Err` — that is the
    /// expected outcome for those failure-mode tests, and the task's
    /// recorded result is surfaced regardless. A panicked or cancelled
    /// task surfaces via `.expect` so the test fails loudly.
    pub async fn shutdown(self) -> Result<(), ServerError> {
        let _ = self.shutdown_tx.send(());
        self.serve_handle
            .await
            .expect("server task panicked or was cancelled before shutdown")
    }
}

/// Bind `127.0.0.1:0`, capture the OS-picked port, and spawn `server` on it
/// via [`Server::serve_with_listener`] with an explicit oneshot shutdown
/// channel.
///
/// The caller drives the server to the desired [`ServingState`] by
/// manipulating its [`tsoracle_consensus::ConsensusDriver`] and then awaits
/// readiness through [`wait_until_serving`] / [`wait_for_grpc_handshake`].
pub async fn boot_server(server: Server) -> BootedServer {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind 127.0.0.1:0 for test server");
    let addr = listener
        .local_addr()
        .expect("local_addr on freshly bound listener");
    let state_rx = server.subscribe();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let serve_handle = tokio::spawn(async move {
        server
            .serve_with_listener(listener, async {
                let _ = shutdown_rx.await;
            })
            .await
    });
    BootedServer {
        addr,
        state_rx,
        serve_handle,
        shutdown_tx,
    }
}

/// Companion to [`BootedServer`] for tests that opt out of
/// [`Server::serve_with_listener`] — typically because they invoke
/// [`Server::into_router`] to manipulate or detach the leader-watch handle.
pub struct BootedRouter {
    pub addr: SocketAddr,
    pub serve_handle: JoinHandle<Result<(), tonic::transport::Error>>,
    shutdown_tx: oneshot::Sender<()>,
}

impl BootedRouter {
    /// Send the shutdown signal and join the spawned task. A panicked or
    /// cancelled router task surfaces via `.expect` so the test fails loudly.
    pub async fn shutdown(self) -> Result<(), tonic::transport::Error> {
        let _ = self.shutdown_tx.send(());
        self.serve_handle
            .await
            .expect("router task panicked or was cancelled before shutdown")
    }

    /// Abort the spawned task without graceful shutdown. Failpoints tests
    /// that intentionally crash the watch task use this — the abort signal
    /// must not mask the failure being observed.
    pub fn abort(self) {
        self.serve_handle.abort();
    }
}

/// Bind `127.0.0.1:0` and spawn `routes` under a fresh `tonic::Server`
/// via `serve_with_incoming_shutdown`. The shutdown future fires on the
/// oneshot held by the returned [`BootedRouter`].
pub async fn boot_router(routes: Routes) -> BootedRouter {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind 127.0.0.1:0 for test router");
    let addr = listener
        .local_addr()
        .expect("local_addr on freshly bound listener");
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let serve_handle = tokio::spawn(async move {
        TonicServer::builder()
            .add_routes(routes)
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_rx.await;
            })
            .await
    });
    BootedRouter {
        addr,
        serve_handle,
        shutdown_tx,
    }
}

/// Block until `state_rx` reports a value satisfying `predicate`.
///
/// Panics if the watch sender is dropped before `predicate` ever holds —
/// that means the server's state stream closed unexpectedly and the test
/// would otherwise wedge.
pub async fn wait_until<F>(rx: &mut watch::Receiver<ServingState>, predicate: F)
where
    F: Fn(&ServingState) -> bool,
{
    loop {
        if predicate(&rx.borrow_and_update()) {
            return;
        }
        rx.changed()
            .await
            .expect("state stream closed before reaching expected state");
    }
}

/// Convenience: wait for [`ServingState::Serving`].
pub async fn wait_until_serving(rx: &mut watch::Receiver<ServingState>) {
    wait_until(rx, |s| matches!(s, ServingState::Serving)).await;
}

/// Convenience: wait for any [`ServingState::NotServing`] variant.
pub async fn wait_until_not_serving(rx: &mut watch::Receiver<ServingState>) {
    wait_until(rx, |s| matches!(s, ServingState::NotServing { .. })).await;
}

/// Bridge the residual race between `ServingState::Serving` and tonic's
/// accept future having been polled. Probes by opening a real gRPC channel
/// (with bounded retry) until one succeeds.
pub async fn wait_for_grpc_handshake(
    addr: SocketAddr,
    budget: Duration,
) -> Result<(), tonic::transport::Error> {
    let deadline = Instant::now() + budget;
    let endpoint: Endpoint = format!("http://{addr}")
        .parse()
        .expect("constructed endpoint URI must parse");
    let mut last_err: Option<tonic::transport::Error> = None;
    loop {
        match endpoint.connect().await {
            Ok(channel) => {
                drop(channel);
                return Ok(());
            }
            Err(err) => {
                if Instant::now() >= deadline {
                    return Err(last_err.unwrap_or(err));
                }
                last_err = Some(err);
                sleep(Duration::from_millis(25)).await;
            }
        }
    }
}

/// TLS-aware counterpart to [`wait_for_grpc_handshake`]. Probes the server
/// by dialing `https://{addr}` with the provided `ClientTlsConfig` so the
/// readiness check actually completes a TLS handshake.
#[cfg(any(feature = "tls-rustls", feature = "tls-native"))]
pub async fn wait_for_grpc_handshake_tls(
    addr: SocketAddr,
    tls_config: tonic::transport::ClientTlsConfig,
    budget: Duration,
) -> Result<(), tonic::transport::Error> {
    let deadline = Instant::now() + budget;
    let endpoint: Endpoint = format!("https://{addr}")
        .parse()
        .expect("constructed endpoint URI must parse");
    let endpoint = endpoint.tls_config(tls_config)?;
    let mut last_err: Option<tonic::transport::Error> = None;
    loop {
        match endpoint.connect().await {
            Ok(channel) => {
                drop(channel);
                return Ok(());
            }
            Err(err) => {
                if Instant::now() >= deadline {
                    return Err(last_err.unwrap_or(err));
                }
                last_err = Some(err);
                sleep(Duration::from_millis(25)).await;
            }
        }
    }
}

/// A minimal [`TsoService`] that always rejects with `NOT_LEADER`, carrying a
/// well-formed leader-hint trailer pointing at `hint_endpoint`. Booted over TLS
/// by [`boot_fixed_hint_server_tls`].
///
/// Exists so TLS client tests can simulate a misconfigured or adversarial peer
/// that surfaces a plaintext `http://` leader hint *at the wire* — the input a
/// TLS-configured client must refuse to follow. Injecting the trailer here
/// bypasses a real server's leader-watch path, whose debug guard (correctly)
/// rejects a scheme-bearing `leader_endpoint` as a driver-contract violation;
/// the behaviour under test is the *client's* downgrade defence, not the
/// server's contract enforcement. The trailer is produced by the production
/// [`not_leader_status`] encoder, so it is byte-identical to a real rejection.
#[cfg(any(feature = "tls-rustls", feature = "tls-native"))]
struct FixedHintService {
    hint_endpoint: String,
}

#[cfg(any(feature = "tls-rustls", feature = "tls-native"))]
#[tonic::async_trait]
impl TsoService for FixedHintService {
    async fn get_ts(
        &self,
        _request: tonic::Request<GetTsRequest>,
    ) -> Result<tonic::Response<GetTsResponse>, tonic::Status> {
        Err(not_leader_status(LeaderHint {
            leader_endpoint: Some(self.hint_endpoint.clone()),
            leader_epoch: None,
        }))
    }
}

/// Bind a TLS gRPC peer on `127.0.0.1:0` that always replies `NOT_LEADER` with a
/// leader-hint trailer pointing at `hint_endpoint`, and return its address. The
/// spawned task is detached and lives until the test process exits. See
/// [`FixedHintService`] for why tests inject the hint here rather than through a
/// real server's leader-watch path.
#[cfg(any(feature = "tls-rustls", feature = "tls-native"))]
pub async fn boot_fixed_hint_server_tls(
    hint_endpoint: String,
    tls_config: tonic::transport::ServerTlsConfig,
) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind 127.0.0.1:0 for fixed-hint TLS server");
    let addr = listener
        .local_addr()
        .expect("local_addr for fixed-hint TLS server");
    let server = TonicServer::builder()
        .tls_config(tls_config)
        .expect("fixed-hint server tls config")
        .add_service(TsoServiceServer::new(FixedHintService { hint_endpoint }));
    tokio::spawn(async move {
        let _ = server
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    addr
}
