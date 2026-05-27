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

use core::time::Duration;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::watch;
use tonic::service::Routes;
use tonic::transport::Server as TonicServer;
use tsoracle_consensus::ConsensusDriver;
#[cfg(any(test, feature = "test-fakes"))]
use tsoracle_core::{CoreError, WindowGrant};
use tsoracle_core::{Epoch, PeerEndpoint};
use tsoracle_proto::v1::tso_service_server::TsoServiceServer;

use crate::bt::Bt;
use crate::clock::{Clock, SystemClock};
use crate::service::TsoServiceImpl;
use crate::serving_core::ServingCore;

#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error("consensus_driver is required")]
    MissingConsensusDriver,
}

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("transport: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("consensus: {0}")]
    Consensus(#[from] tsoracle_consensus::ConsensusError),
    #[error("core: {0}")]
    Core(#[from] tsoracle_core::CoreError),
    /// The leader-watch task panicked. Distinct from a clean error return so
    /// operators can tell "driver returned Err" (recoverable design) from
    /// "task panicked" (programming bug).
    #[error("leader-watch task panicked: {payload}{bt}")]
    WatchPanic { payload: String, bt: Bt },
    /// The consensus driver's `leadership_events()` stream ended cleanly while
    /// the leader-watch task was running. The stream is contracted to live for
    /// the life of the server, so its end is anomalous (driver shutdown, lost
    /// session, etc.) — distinct from a `Consensus` error returned mid-fence.
    /// The watch task publishes `ServingState::NotServing` before returning
    /// this variant so embedders who never observe the [`WatchGuard`] still get
    /// the documented fail-safe behavior.
    #[error("consensus driver leadership stream closed")]
    WatchStreamClosed,
    /// The embedded protobuf descriptor set failed to decode while building the
    /// gRPC reflection service. `tsoracle-proto`'s `build.rs` emits these bytes
    /// from checked-in `.proto` sources, so a failure here signals build-artifact
    /// drift (a corrupt or stale descriptor) rather than a runtime condition —
    /// surfaced as a diagnosable startup error instead of a process panic.
    #[cfg(feature = "reflection")]
    #[error("failed to build gRPC reflection service from embedded descriptor set: {0}")]
    ReflectionInit(#[source] tonic_reflection::server::Error),
}

#[derive(Clone, Debug)]
pub enum ServingState {
    NotServing {
        leader_endpoint: Option<PeerEndpoint>,
        leader_epoch: Option<Epoch>,
    },
    Serving,
}

/// Default bound on how long a graceful shutdown waits for the leader-watch
/// task to stop cooperatively before forcibly aborting it. The abort is a
/// last-resort safety net for a consensus driver whose `load_high_water` /
/// `persist_high_water` is wedged (the trait places no latency bound; see
/// [`ConsensusDriver`]). Chosen to sit comfortably under a typical Kubernetes
/// `terminationGracePeriodSeconds` (30s) so the abort, the tonic drain, and
/// process exit all complete before the kubelet escalates to SIGKILL.
const DEFAULT_SHUTDOWN_GRACE: Duration = Duration::from_secs(10);

pub struct ServerBuilder {
    consensus: Option<Arc<dyn ConsensusDriver>>,
    clock: Option<Arc<dyn Clock>>,
    window_ahead: Duration,
    failover_advance: Duration,
    shutdown_grace: Duration,
    heartbeat_interval: Duration,
    #[cfg(any(feature = "tls-rustls", feature = "tls-native"))]
    tls_config: Option<tonic::transport::ServerTlsConfig>,
}

impl Default for ServerBuilder {
    fn default() -> Self {
        ServerBuilder {
            consensus: None,
            clock: None,
            window_ahead: Duration::from_secs(3),
            failover_advance: Duration::from_secs(1),
            shutdown_grace: DEFAULT_SHUTDOWN_GRACE,
            heartbeat_interval: Duration::from_secs(10),
            #[cfg(any(feature = "tls-rustls", feature = "tls-native"))]
            tls_config: None,
        }
    }
}

impl ServerBuilder {
    pub fn consensus_driver(mut self, driver: Arc<dyn ConsensusDriver>) -> Self {
        self.consensus = Some(driver);
        self
    }
    pub fn clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = Some(clock);
        self
    }
    pub fn window_ahead(mut self, window_ahead: Duration) -> Self {
        self.window_ahead = window_ahead;
        self
    }
    pub fn failover_advance(mut self, failover_advance: Duration) -> Self {
        self.failover_advance = failover_advance;
        self
    }

    /// Bound on how long a graceful shutdown waits for the leader-watch task to
    /// stop cooperatively before forcibly aborting it.
    ///
    /// On shutdown the server drops the watch task's cancel signal and waits for
    /// it to publish `NotServing` and return. That wait is normally
    /// near-instant, but the task observes cancellation only at its `select!`
    /// boundaries — never inside a fence attempt. A consensus driver whose
    /// `load_high_water` / `persist_high_water` never returns (the trait places
    /// no latency bound) would otherwise park the task mid-fence and block
    /// process exit indefinitely, leading to a SIGKILL on a Kubernetes drain.
    /// Once `shutdown_grace` elapses the server aborts the task so exit always
    /// makes progress. Set this comfortably below your deployment's
    /// `terminationGracePeriodSeconds`. Defaults to 10s. A value of zero aborts
    /// immediately without waiting for a cooperative stop.
    pub fn shutdown_grace(mut self, shutdown_grace: Duration) -> Self {
        self.shutdown_grace = shutdown_grace;
        self
    }

    /// Interval between heartbeat log lines emitted at `target = "tsoracle::heartbeat"`.
    /// Defaults to 10 seconds. Pass `Duration::ZERO` to disable the heartbeat task entirely.
    ///
    /// The heartbeat surfaces serving role, current epoch, requests served,
    /// timestamps issued, and key error counters every interval — proof-of-life
    /// for production deployments that may not have a metrics exporter installed.
    ///
    /// Requires `feature = "tracing"` to emit output; with `tracing` off the
    /// setter is accepted but no task is spawned (no subscriber to log to).
    pub fn heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = interval;
        self
    }

    /// Configure TLS termination for this server. Applied inside
    /// [`Server::serve`], [`Server::serve_with_shutdown`], and
    /// [`Server::serve_with_listener`]. Not applied to [`Server::into_router`] —
    /// embedders mounting tsoracle alongside their own services control TLS
    /// on their own tonic builder.
    #[cfg(any(feature = "tls-rustls", feature = "tls-native"))]
    pub fn tls_config(mut self, cfg: tonic::transport::ServerTlsConfig) -> Self {
        self.tls_config = Some(cfg);
        self
    }

    pub fn build(self) -> Result<Server, BuildError> {
        let consensus = self.consensus.ok_or(BuildError::MissingConsensusDriver)?;
        let clock = self.clock.unwrap_or_else(|| Arc::new(SystemClock));
        Ok(Server {
            consensus,
            clock,
            window_ahead: self.window_ahead,
            failover_advance: self.failover_advance,
            shutdown_grace: self.shutdown_grace,
            heartbeat_interval: self.heartbeat_interval,
            core: Arc::new(ServingCore::new(self.window_ahead)),
            reporter: Arc::new(crate::reporter::Reporter::new()),
            #[cfg(any(feature = "tls-rustls", feature = "tls-native"))]
            tls_config: self.tls_config,
        })
    }
}

pub struct Server {
    pub(crate) consensus: Arc<dyn ConsensusDriver>,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) window_ahead: Duration,
    pub(crate) failover_advance: Duration,
    /// Bound on the graceful-shutdown wait for the leader-watch task before a
    /// forced abort. See [`ServerBuilder::shutdown_grace`].
    pub(crate) shutdown_grace: Duration,
    /// Interval between periodic heartbeat log lines. See [`ServerBuilder::heartbeat_interval`].
    ///
    /// The only reader is the `cfg(feature = "tracing")` spawn block in
    /// `into_router_parts`; without `tracing` there is no subscriber to log to
    /// and the spawn arm is compiled out, so the field is genuinely unread.
    #[cfg_attr(not(feature = "tracing"), allow(dead_code))]
    pub(crate) heartbeat_interval: Duration,
    /// Owns the allocator, serving-state channel, and both extension locks, with
    /// the lock-ordering and step-down invariants private behind its methods.
    ///
    /// Held behind an `Arc` so the leader-watch task, the gRPC service, and the
    /// [`WatchGuard`] / [`serve_inner`] shutdown paths can all reach the same
    /// core. The guard and the serve loop use their clone to close the serving
    /// gate *synchronously* at shutdown, leaving the watch task's later
    /// `step_down` a harmless idempotent repeat.
    pub(crate) core: Arc<ServingCore>,
    pub(crate) reporter: Arc<crate::reporter::Reporter>,
    #[cfg(any(feature = "tls-rustls", feature = "tls-native"))]
    pub(crate) tls_config: Option<tonic::transport::ServerTlsConfig>,
}

/// Raw parts produced by [`Server::into_router_parts`]: the gRPC `Routes`, the
/// leader-watch task's cooperative-cancel sender (dropping it stops the task),
/// the task's join handle, and the optional heartbeat task's cancel sender /
/// join handle. [`Server::into_router`] wraps these into a [`WatchGuard`]; the
/// `serve_*` methods consume them directly via [`serve_inner`].
///
/// The heartbeat fields are `None` when the heartbeat is disabled — either by
/// `ServerBuilder::heartbeat_interval(Duration::ZERO)` or by building without
/// the `tracing` feature (there is no subscriber to log to).
pub(crate) struct RouterParts {
    pub routes: Routes,
    pub cancel_tx: tokio::sync::oneshot::Sender<()>,
    pub watch_handle: tokio::task::JoinHandle<Result<(), ServerError>>,
    pub heartbeat_cancel_tx: Option<tokio::sync::oneshot::Sender<()>>,
    pub heartbeat_handle: Option<tokio::task::JoinHandle<()>>,
}

impl Server {
    pub fn builder() -> ServerBuilder {
        ServerBuilder::default()
    }

    /// Subscribe to serving-state transitions.
    ///
    /// Returns a fresh `watch::Receiver` observing the same `ServingState`
    /// the server publishes as leadership comes and goes. Embedders use this
    /// to gate their own startup on `ServingState::Serving` (see the
    /// `embedded_router` and piggyback examples). Because `into_router`
    /// consumes the `Server`, capture the receiver before mounting.
    ///
    /// This method is the stable observation API: the receiver is minted from
    /// the server's `watch::Sender`, so the receiver's type can evolve (e.g. a
    /// future newtype around `ServingState`) without breaking embedders that
    /// go through it.
    pub fn subscribe(&self) -> watch::Receiver<ServingState> {
        self.core.subscribe()
    }
}

impl Server {
    /// Return the configured `TsoServiceServer<TsoServiceImpl>` as a tonic
    /// `Routes` value plus a [`WatchGuard`] for the spawned leader-watch task,
    /// so callers can mount tsoracle's service alongside their own services
    /// on a shared tonic listener instead of binding a dedicated port.
    ///
    /// The returned [`WatchGuard`] owns the leader-watch task. **Keep it alive
    /// for as long as the mounted `Routes` should serve**: the watch task holds
    /// an `Arc<Server>` (and the consensus driver) and maintains serving state
    /// across leadership transitions. Dropping the guard — or calling
    /// [`WatchGuard::shutdown`] — cooperatively stops the task at the embedder's
    /// own shutdown. Without the guard the task would keep `Arc<Server>` alive
    /// until the leadership stream happened to close.
    ///
    /// Every termination of the task — cooperative cancellation, driver error,
    /// panic, or clean EOF on the leadership stream (surfaced as
    /// `ServerError::WatchStreamClosed`) — publishes
    /// `ServingState::NotServing { leader_endpoint: None }` before returning, so
    /// all subsequent RPCs fail fast with `FAILED_PRECONDITION`. Embedders who
    /// drop the guard without awaiting still get fail-safe behavior.
    ///
    /// The `Server::serve()` method is a thin wrapper over this — it calls
    /// `into_router`, builds a tonic `Server`, and binds a listener.
    ///
    /// Returns `Err(ServerError::ReflectionInit)` (only reachable under the
    /// `reflection` feature) if the embedded descriptor set fails to decode.
    /// That decode happens before the leader-watch task is spawned, so a failure
    /// leaves nothing running to clean up.
    pub fn into_router(self) -> Result<(Routes, WatchGuard), ServerError> {
        // Read the `Copy` grace before `into_router_parts` consumes `self`, so
        // the returned guard can bound its own shutdown wait identically to the
        // `serve_*` paths.
        let shutdown_grace = self.shutdown_grace;
        // Clone the shared core and reporter before `into_router_parts` consumes
        // `self`, so the guard can close the serving gate synchronously on drop /
        // shutdown rather than relying on the watch task's later publish, and can
        // record the shutdown_watch_aborted counter if the grace-bounded reap fires.
        let core = self.core.clone();
        let reporter = self.reporter.clone();
        let parts = self.into_router_parts()?;
        Ok((
            parts.routes,
            WatchGuard {
                cancel_tx: Some(parts.cancel_tx),
                handle: Some(parts.watch_handle),
                shutdown_grace,
                core,
                reporter,
                heartbeat_cancel_tx: parts.heartbeat_cancel_tx,
                heartbeat_handle: parts.heartbeat_handle,
            },
        ))
    }

    /// Spawn the leader-watch task and assemble the gRPC `Routes`, returning
    /// the raw parts: the routes, the task's cooperative-cancel sender, and its
    /// `JoinHandle`. [`Self::into_router`] wraps these into a [`WatchGuard`] for
    /// embedders; the `serve_*` methods drive the parts directly via
    /// [`serve_inner`], so neither path needs to unwrap the guard's `Option`
    /// fields.
    fn into_router_parts(self) -> Result<RouterParts, ServerError> {
        // Build the reflection service first: a descriptor-decode failure must
        // surface before we spawn the leader-watch task below, so an error path
        // never leaks a running task.
        #[cfg(feature = "reflection")]
        let reflection = build_reflection_service(tsoracle_proto::FILE_DESCRIPTOR_SET)?;

        let server = Arc::new(self);

        // Cooperative cancellation channel. The `WatchGuard` holds the sender;
        // the task's `cancel` future resolves on either an explicit send or a
        // sender drop, so dropping the guard is sufficient to stop the task.
        let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<()>();

        let watch_server = server.clone();
        let watch_handle = tokio::spawn(async move {
            use futures::FutureExt;
            // Resolves when the WatchGuard signals cancellation or is dropped.
            let cancel = async move {
                let _ = cancel_rx.await;
            };
            // catch_unwind so a panic in run_leader_watch still routes through
            // the poisoning path. Without this, embedders who mount into_router
            // directly and never observe the guard would see
            // ServingState::Serving remain published while the watch task is
            // dead — the inverse of the fail-safe guarantee documented above.
            // The panic is re-raised after poisoning so serve / serve_with_*
            // continue to translate it into ServerError::WatchPanic via
            // join_to_server_result.
            let outcome = std::panic::AssertUnwindSafe(crate::fence::run_leader_watch(
                watch_server.clone(),
                cancel,
            ))
            .catch_unwind()
            .await;
            match outcome {
                Ok(result) => {
                    if let Err(ref _e) = result {
                        // Poison BEFORE returning so embedders who do not observe
                        // the guard still get fail-safe behavior.
                        watch_server.core.step_down(None, None);
                        #[cfg(feature = "tracing")]
                        tracing::error!(error = %_e, "leader-watch terminated; serving disabled");
                    }
                    result
                }
                Err(panic_payload) => {
                    // Mirror the Err branch: poison BEFORE re-raising so
                    // guard-dropping embedders still observe NotServing.
                    watch_server.core.step_down(None, None);
                    #[cfg(feature = "tracing")]
                    tracing::error!("leader-watch panicked; serving disabled");
                    std::panic::resume_unwind(panic_payload);
                }
            }
        });

        // Spawn the heartbeat task, if enabled. Gated on `feature = "tracing"`
        // because the heartbeat module is only compiled with `tracing`
        // (no subscriber to emit to without it) — and on a non-zero interval,
        // since `Duration::ZERO` is the documented opt-out.
        //
        // The task body is wrapped in `AssertUnwindSafe(...).catch_unwind()`
        // mirroring the leader-watch spawn above: on panic we bump the
        // `heartbeat_task_panicked` counter and log at error level, then let
        // the task end (no restart — the heartbeat is observability, not
        // correctness, so a panicked task must not be allowed to thrash).
        let (heartbeat_cancel_tx, heartbeat_handle) = {
            #[cfg(feature = "tracing")]
            {
                if server.heartbeat_interval.is_zero() {
                    (None, None)
                } else {
                    use futures::FutureExt;
                    let (htx, hrx) = tokio::sync::oneshot::channel::<()>();
                    let hb_reporter = server.reporter.clone();
                    let hb_core = server.core.clone();
                    let hb_interval = server.heartbeat_interval;
                    let handle = tokio::spawn(async move {
                        let outcome =
                            std::panic::AssertUnwindSafe(crate::heartbeat::run_heartbeat(
                                hb_interval,
                                hb_core,
                                hb_reporter.clone(),
                                hrx,
                            ))
                            .catch_unwind()
                            .await;
                        if outcome.is_err() {
                            hb_reporter.heartbeat_task_panicked.increment(1);
                            tracing::error!(
                                target: "tsoracle::heartbeat",
                                "heartbeat task panicked; liveness logs disabled until restart"
                            );
                        }
                    });
                    (Some(htx), Some(handle))
                }
            }
            #[cfg(not(feature = "tracing"))]
            {
                (None, None)
            }
        };

        let service = TsoServiceImpl { server };
        #[allow(unused_mut)]
        let mut routes = Routes::new(TsoServiceServer::new(service));
        #[cfg(feature = "reflection")]
        {
            routes = routes.add_service(reflection);
        }
        Ok(RouterParts {
            routes,
            cancel_tx,
            watch_handle,
            heartbeat_cancel_tx,
            heartbeat_handle,
        })
    }

    pub async fn serve(self, addr: SocketAddr) -> Result<(), ServerError> {
        self.serve_with_shutdown(addr, futures::future::pending())
            .await
    }

    /// Run the gRPC server until either the caller's `shutdown` fires or the
    /// leader-watch task terminates.
    ///
    /// Three outcomes:
    /// 1. `shutdown` fires first → tonic drains in-flights and returns Ok.
    ///    The watch task is then stopped cooperatively, bounded by
    ///    `shutdown_grace` and forcibly aborted if it overruns (e.g. parked in a
    ///    wedged consensus-driver call); any error it had been about to return
    ///    is forfeited (the process is shutting down anyway).
    /// 2. Watch returns `Ok(Err(e))` → poisoned state is already published;
    ///    `cancel_tx` triggers tonic's graceful shutdown; in-flight `GetTs`
    ///    calls whose `try_grant` already succeeded complete with the
    ///    timestamps they were allocated; new calls fail fast. Returns `Err(e)`
    ///    — the watch error wins even if the drain itself also errors (see
    ///    `combine_watch_and_drain`); a drain error is surfaced only when the
    ///    watch ended cleanly.
    /// 3. Watch task panics → returns `Err(ServerError::WatchPanic{..})`
    ///    with the panic payload stringified. Same drain semantics as (2).
    pub async fn serve_with_shutdown(
        self,
        addr: SocketAddr,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> Result<(), ServerError> {
        #[cfg(any(feature = "tls-rustls", feature = "tls-native"))]
        let tls_config = self.tls_config.clone();

        // Read the `Copy` grace and clone the shared core and reporter before
        // `into_router_parts` consumes `self`.
        let shutdown_grace = self.shutdown_grace;
        let core = self.core.clone();
        let reporter = self.reporter.clone();
        let parts = self.into_router_parts()?;
        let (combined_shutdown, cancel_tx) = combined_shutdown_with_cancel(shutdown);

        let mut tonic = TonicServer::builder();
        #[cfg(any(feature = "tls-rustls", feature = "tls-native"))]
        if let Some(cfg) = tls_config {
            tonic = tonic.tls_config(cfg).map_err(ServerError::Transport)?;
        }
        let serve = tonic
            .add_routes(parts.routes)
            .serve_with_shutdown(addr, combined_shutdown);

        serve_inner(
            parts.cancel_tx,
            parts.watch_handle,
            parts.heartbeat_cancel_tx,
            parts.heartbeat_handle,
            serve,
            cancel_tx,
            shutdown_grace,
            core,
            reporter,
        )
        .await
    }

    /// Run the gRPC server on a caller-provided `TcpListener` until either
    /// the caller-provided `shutdown` fires or the leader-watch task terminates.
    ///
    /// Use this instead of [`Self::serve_with_shutdown`] when you need to
    /// observe the OS-picked port (`127.0.0.1:0`) before clients connect, or
    /// when you want to wrap the listener in an outer adapter before passing it
    /// in. The listening socket is owned by the caller and passed here; tsoracle
    /// starts accepting on it immediately.
    ///
    /// Three outcomes:
    /// 1. `shutdown` fires first → tonic drains in-flights and returns `Ok`.
    ///    The watch handle is aborted; any error it had been about to return
    ///    is forfeited (the process is shutting down anyway).
    /// 2. Watch returns `Ok(Err(e))` → poisoned state is already published;
    ///    the caller-provided shutdown is cancelled internally so tonic begins
    ///    graceful shutdown; in-flight `GetTs` calls whose `try_grant` already
    ///    succeeded complete with the timestamps they were allocated; new calls
    ///    fail fast. Returns `Err(e)` — the watch error wins even if the drain
    ///    itself also errors (see `combine_watch_and_drain`); a drain error is
    ///    surfaced only when the watch ended cleanly.
    /// 3. Watch task panics → returns `Err(ServerError::WatchPanic{..})`
    ///    with the panic payload stringified. Same drain semantics as (2).
    pub async fn serve_with_listener(
        self,
        listener: tokio::net::TcpListener,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> Result<(), ServerError> {
        #[cfg(any(feature = "tls-rustls", feature = "tls-native"))]
        let tls_config = self.tls_config.clone();

        // Read the `Copy` grace and clone the shared core and reporter before
        // `into_router_parts` consumes `self`.
        let shutdown_grace = self.shutdown_grace;
        let core = self.core.clone();
        let reporter = self.reporter.clone();
        let parts = self.into_router_parts()?;
        let (combined_shutdown, cancel_tx) = combined_shutdown_with_cancel(shutdown);

        let incoming = tonic::transport::server::TcpIncoming::from(listener);

        let mut tonic = TonicServer::builder();
        #[cfg(any(feature = "tls-rustls", feature = "tls-native"))]
        if let Some(cfg) = tls_config {
            tonic = tonic.tls_config(cfg).map_err(ServerError::Transport)?;
        }
        let serve = tonic
            .add_routes(parts.routes)
            .serve_with_incoming_shutdown(incoming, combined_shutdown);

        serve_inner(
            parts.cancel_tx,
            parts.watch_handle,
            parts.heartbeat_cancel_tx,
            parts.heartbeat_handle,
            serve,
            cancel_tx,
            shutdown_grace,
            core,
            reporter,
        )
        .await
    }
}

/// RAII handle to the leader-watch task spawned by [`Server::into_router`].
///
/// The watch task holds an `Arc<Server>` (and thus the consensus driver) and
/// maintains serving state across leadership transitions. This guard ties the
/// task's lifetime to the guard's: dropping it cooperatively cancels the task,
/// and the task publishes [`ServingState::NotServing`] before it stops, so any
/// `Routes` an embedder still has mounted fails subsequent RPCs fast.
///
/// Cancellation is cooperative — the task stops at its next await boundary and
/// never mid-fence, so it is never torn down while holding internal locks, in
/// contrast to a raw [`tokio::task::JoinHandle::abort`].
pub struct WatchGuard {
    // `Option` so `Drop` and the consuming `shutdown` / `abort` methods can
    // each take a field without a partial-move conflict against the `Drop`
    // impl. Dropping the sender (rather than sending) is itself the cancel
    // signal: the task's `cancel` future resolves on sender-drop too.
    cancel_tx: Option<tokio::sync::oneshot::Sender<()>>,
    handle: Option<tokio::task::JoinHandle<Result<(), ServerError>>>,
    /// Bound on the cooperative-stop wait in [`Self::shutdown`] before a forced
    /// abort. Inherited from [`ServerBuilder::shutdown_grace`].
    shutdown_grace: Duration,
    /// Shared serving core, cloned from the `Server`. Lets `Drop` (and the
    /// consuming `shutdown` / `abort`, which trigger `Drop` on return) close the
    /// serving gate synchronously at the drop site, instead of waiting for the
    /// watch task to observe cancellation and publish `NotServing` on its own
    /// timeline — a window during which the fast gate would still admit RPCs.
    core: Arc<ServingCore>,
    /// Metrics reporter, cloned from the `Server`. Used to record the
    /// `shutdown_watch_aborted` counter if the grace-bounded reap fires.
    reporter: Arc<crate::reporter::Reporter>,
    /// Cooperative-cancel sender for the heartbeat task. `None` when the
    /// heartbeat is disabled (interval == 0 or built without `tracing`).
    /// Dropping the sender resolves the task's cancel future.
    heartbeat_cancel_tx: Option<tokio::sync::oneshot::Sender<()>>,
    /// Join handle for the heartbeat task. `None` when the heartbeat is
    /// disabled. Output is `()` because the task never returns an error —
    /// panics are caught inside the task body and recorded via the
    /// `heartbeat_task_panicked` counter.
    heartbeat_handle: Option<tokio::task::JoinHandle<()>>,
}

impl WatchGuard {
    /// Signal the leader-watch task to stop, wait for it to drain, and report
    /// its outcome.
    ///
    /// A cooperatively cancelled task returns `Ok(())` — the stop was
    /// requested, so it is not an error. If the task had already terminated on
    /// its own (driver error, stream EOF, or panic) the original outcome is
    /// surfaced verbatim: `Err(e)` or [`ServerError::WatchPanic`]. Either way
    /// serving state is `NotServing` once this returns.
    ///
    /// The cooperative wait is bounded by the configured
    /// [`ServerBuilder::shutdown_grace`]: if the task is parked in a
    /// consensus-driver call that never returns it is aborted once the grace
    /// elapses (still reported as `Ok(())`), so an embedder's shutdown can never
    /// wedge behind a hung driver.
    pub async fn shutdown(mut self) -> Result<(), ServerError> {
        // Dropping the senders fires each task's cancel future. The heartbeat
        // task is reaped first because it is bounded by `tokio::time::sleep`
        // (cooperative stop is fast and never wedges on a driver call), so its
        // reap returns quickly and leaves the grace budget for the watch task
        // — which may be parked in a wedged consensus-driver call.
        self.heartbeat_cancel_tx.take();
        self.cancel_tx.take();
        if let Some(mut hb_handle) = self.heartbeat_handle.take() {
            match tokio::time::timeout(self.shutdown_grace, &mut hb_handle).await {
                Ok(Ok(())) => {}
                // Task panicked — already counted + logged via catch_unwind in
                // the task body. Nothing more to do here.
                Ok(Err(_join_err)) => {}
                // Grace overrun — sleep + select! should always observe a
                // dropped cancel sender, so this is a backstop. Abort and
                // reap; no separate metric (the heartbeat is observability
                // only — its lateness is not a serving correctness signal).
                Err(_elapsed) => {
                    hb_handle.abort();
                    let _ = (&mut hb_handle).await;
                }
            }
        }
        match self.handle.take() {
            Some(mut handle) => join_to_server_result(
                await_watch_within_grace(&mut handle, self.shutdown_grace, &self.reporter).await,
            ),
            None => Ok(()),
        }
    }

    /// Hard-abort the leader-watch task without waiting for a cooperative stop.
    ///
    /// Prefer [`Self::shutdown`] or simply dropping the guard; both let the
    /// task stop at a safe point. This is an escape hatch for callers that
    /// cannot await and accept that the task may be torn down mid-fence.
    pub fn abort(mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
        // Hard-abort the heartbeat task too — leaving it running after the
        // watch is torn down would publish heartbeats describing a stale
        // (typically `NotServing`) view until the Arc<Reporter> is dropped.
        if let Some(hb_handle) = self.heartbeat_handle.take() {
            hb_handle.abort();
        }
    }

    /// Whether the leader-watch task has finished — terminated for any reason
    /// (cooperative cancel, driver error, stream EOF, or panic).
    ///
    /// A read-only liveness probe that neither consumes the guard nor disturbs
    /// its cancel-on-drop behavior, so an embedder can poll task health while
    /// keeping the guard alive.
    pub fn is_finished(&self) -> bool {
        self.handle
            .as_ref()
            .is_none_or(|handle| handle.is_finished())
    }
}

impl Drop for WatchGuard {
    fn drop(&mut self) {
        // Close the serving gate synchronously, here at the drop site. Dropping
        // the cancel sender below only *requests* the watch task to stop; the
        // task publishes `NotServing` later, on its own timeline. Between this
        // drop and the task's next poll the fast gate would still read `Serving`
        // (and the allocator would still grant), so an RPC could be admitted by a
        // server that has already been told to stop serving. `step_down` clears
        // the allocator and publishes `NotServing` now, before any await; the
        // watch task's later `step_down(None, None)` on cooperative cancel (see
        // `fence::run_leader_watch`) republishes the identical state, so the
        // double-close is harmless and idempotent.
        self.core.step_down(None, None);
        // Dropping the sender (if `shutdown` / `abort` did not already take it)
        // resolves the task's cancel future; the task then publishes
        // `NotServing` and returns. The `JoinHandle` is dropped here too,
        // detaching the task to finish its cooperative shutdown on its own.
        self.cancel_tx.take();
        // Same treatment for the heartbeat task. `Drop` is sync so we cannot
        // await the cooperative stop; instead we drop the cancel sender (the
        // task will observe it at its next select! boundary) and hard-abort
        // the handle so it cannot outlive the guard and publish heartbeats
        // describing a stale view if the runtime keeps the Arc alive.
        self.heartbeat_cancel_tx.take();
        if let Some(hb_handle) = self.heartbeat_handle.take() {
            hb_handle.abort();
        }
    }
}

/// Merge the caller's `shutdown` future with an internal cancellation signal.
///
/// Both [`Server::serve_with_shutdown`] and [`Server::serve_with_listener`]
/// need tonic to stop when EITHER the caller's `shutdown` fires OR the
/// leader-watch task terminates (signalled by firing the returned
/// `oneshot::Sender`). This builds that merged shutdown future and hands back
/// the sender so [`serve_inner`] can trip it from the watch arm.
fn combined_shutdown_with_cancel(
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> (
    impl Future<Output = ()> + Send + 'static,
    tokio::sync::oneshot::Sender<()>,
) {
    let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<()>();
    let combined_shutdown = async move {
        tokio::select! {
            _ = shutdown => {}
            _ = cancel_rx => {}
        }
    };
    (combined_shutdown, cancel_tx)
}

/// Wait for the leader-watch task to stop cooperatively, but no longer than
/// `grace`, then forcibly abort it if it is still running.
///
/// The watch task observes its cancel signal only at the `select!` boundaries
/// in [`crate::fence::run_leader_watch`], never inside a fence attempt. A
/// consensus driver whose `load_high_water` / `persist_high_water` never
/// returns (the trait places no latency bound; see
/// [`tsoracle_consensus::ConsensusDriver`]) therefore parks the task upstream
/// of any cancel-observing await, so dropping the cancel sender cannot stop it.
/// Left unbounded, the shutdown wait would block process exit until the kubelet
/// escalates to SIGKILL on a drain. Bounding the wait by `grace` and aborting
/// on expiry guarantees forward progress: `tokio` tears a suspended task (and
/// the wedged driver future it holds) down at the abort, dropping its
/// drain-barrier guard.
///
/// Returns the task's join result. A clean cooperative stop forwards its real
/// outcome verbatim; an aborted task surfaces as a cancelled `JoinError`, which
/// [`join_to_server_result`] maps to `Ok(())` — the stop was requested, so a
/// forced abort during shutdown is not an error. A `grace` of zero aborts
/// immediately (the `timeout` future is already elapsed on first poll).
async fn await_watch_within_grace(
    watch_handle: &mut tokio::task::JoinHandle<Result<(), ServerError>>,
    grace: Duration,
    reporter: &Arc<crate::reporter::Reporter>,
) -> Result<Result<(), ServerError>, tokio::task::JoinError> {
    match tokio::time::timeout(grace, &mut *watch_handle).await {
        Ok(join_result) => join_result,
        Err(_elapsed) => {
            reporter.shutdown_watch_aborted.increment(1);
            #[cfg(feature = "tracing")]
            tracing::warn!(
                grace_ms = grace.as_millis() as u64,
                "leader-watch task did not stop within the shutdown grace; aborting it (a consensus-driver call likely exceeded its latency bound)"
            );
            watch_handle.abort();
            // Reap the aborted task so its Drop (releasing any held drain-barrier
            // guard) runs before we report shutdown complete. Bounded: an aborted
            // task resolves at its next poll.
            (&mut *watch_handle).await
        }
    }
}

/// Drive the gRPC `serve_future` against the leader-watch task, shared by
/// [`Server::serve_with_shutdown`] and [`Server::serve_with_listener`].
///
/// The two public methods differ only in how `serve_future` is assembled
/// (address-bound via `serve_with_shutdown` vs listener-bound via
/// `serve_with_incoming_shutdown`); everything downstream — the biased select,
/// the cooperative-cancel path, and the drain/translate logic — is identical
/// and lives here so a future change need only be made once.
///
/// `tonic_cancel_tx` is the cancellation half paired with the `serve_future`'s
/// shutdown signal (see [`combined_shutdown_with_cancel`]); firing it begins
/// tonic's graceful drain when the watch task terminates first. `watch_cancel_tx`
/// is the leader-watch task's own cooperative-cancel sender (the same one a
/// [`WatchGuard`] holds for embedders); dropping it stops the task. Taking the
/// raw parts rather than a `WatchGuard` keeps this path free of the guard's
/// `Option` fields — neither the watch handle nor the cancel sender is optional
/// here. `shutdown_grace` bounds the user-shutdown arm's wait for the watch task
/// (see [`await_watch_within_grace`]). `core` is the shared serving core (the
/// same one the watch task and the gRPC service hold): the user-shutdown arm
/// closes the gate on it synchronously so no RPC is admitted in the window
/// before the watch task observes cancellation and publishes `NotServing`.
// Private serve helper. The wide signature is the cost of being the single
// merge point for the two public `serve_*` paths and the leader-watch +
// heartbeat task pair: bundling these into a struct just to placate clippy
// would obscure the lifecycle (every parameter is consumed exactly once and
// has no shared identity worth naming). Keep the arguments visible.
#[allow(clippy::too_many_arguments)]
async fn serve_inner<S>(
    watch_cancel_tx: tokio::sync::oneshot::Sender<()>,
    mut watch_handle: tokio::task::JoinHandle<Result<(), ServerError>>,
    heartbeat_cancel_tx: Option<tokio::sync::oneshot::Sender<()>>,
    heartbeat_handle: Option<tokio::task::JoinHandle<()>>,
    serve_future: S,
    tonic_cancel_tx: tokio::sync::oneshot::Sender<()>,
    shutdown_grace: Duration,
    core: Arc<ServingCore>,
    reporter: Arc<crate::reporter::Reporter>,
) -> Result<(), ServerError>
where
    S: Future<Output = Result<(), tonic::transport::Error>>,
{
    tokio::pin!(serve_future);

    let outcome = tokio::select! {
        // Bias toward the watch arm: if both are ready in the same poll
        // (rare but possible — graceful shutdown completed in the same
        // tick the watch returned), we want to surface the watch error
        // rather than report a clean shutdown.
        biased;

        watch_result = &mut watch_handle => {
            // Watch terminated. State is already poisoned (see watch
            // task body in into_router). Trigger tonic drain, wait for
            // it to finish, then report the watch's outcome — preferring
            // it over any drain error, which surfaces only if the watch
            // itself ended cleanly.
            let _ = tonic_cancel_tx.send(());
            let drain_result = serve_future.await;
            combine_watch_and_drain(watch_result, drain_result)
        }
        serve_result = &mut serve_future => {
            // User shutdown fired (or our cancel — but watch arm has
            // `biased` priority, so reaching here means user shutdown).
            // Prefer a cooperative stop: dropping the cancel sender resolves
            // the task's cancel future so it stops at its next `select!`
            // boundary, having published `NotServing` and never torn down
            // mid-fence while holding `extension_gate.write()`. But a
            // cooperative stop is only observed at those boundaries, never
            // inside a fence attempt — a consensus-driver call that never
            // returns (the trait places no latency bound) would park the task
            // upstream of any cancel point and block process exit until a
            // kubelet SIGKILL. `await_watch_within_grace` therefore bounds the
            // wait by `shutdown_grace` and aborts the task if it overruns. The
            // task's own outcome (a clean `Ok(())` on cooperative stop, a
            // cancelled `JoinError` on abort) is discarded; the user-requested
            // shutdown result wins.
            //
            // Close the serving gate synchronously first: dropping the sender
            // only *requests* the stop, and a task aborted on grace expiry (or
            // simply not yet rescheduled) may never reach its own `step_down`. So
            // a `GetTs` arriving during the drain would still be admitted unless
            // we close the gate here. `step_down` is idempotent with the watch
            // task's own cooperative-cancel publish.
            core.step_down(None, None);
            drop(watch_cancel_tx);
            let _ = await_watch_within_grace(&mut watch_handle, shutdown_grace, &reporter).await;
            serve_result?;
            Ok(())
        }
    };

    // Stop the heartbeat task on every exit path. Done after the watch reap so
    // the watch-arm `combine_watch_and_drain` already saw the watch outcome,
    // and the user-shutdown arm has finished its grace-bounded wait. Dropping
    // the cancel sender breaks the task's `tokio::select! { biased; cancel,
    // sleep }` loop on the next poll; if the task is wedged for any reason we
    // hard-abort on grace overrun. The task's outcome is observability only
    // and cannot influence serving correctness, so its join result is dropped.
    drop(heartbeat_cancel_tx);
    if let Some(mut hb_handle) = heartbeat_handle {
        match tokio::time::timeout(shutdown_grace, &mut hb_handle).await {
            Ok(Ok(())) => {}
            Ok(Err(_join_err)) => {} // panic — already counted via catch_unwind
            Err(_elapsed) => {
                hb_handle.abort();
                let _ = (&mut hb_handle).await;
            }
        }
    }

    outcome
}

/// Convert a `JoinHandle` result into a `ServerError`-typed result.
///
/// - `Ok(Ok(()))` — cooperative cancellation: `run_leader_watch` observed its
///   cancel signal (the `WatchGuard` was dropped, `WatchGuard::shutdown` was
///   called, or `serve_inner` cancelled it on user shutdown), published
///   `NotServing`, and returned cleanly. Forwarded verbatim as `Ok(())`.
/// - `Ok(Err(e))` — task returned an error (including `WatchStreamClosed`
///   from a clean EOF). Forward verbatim.
/// - `Err(JoinError)` — task was aborted or panicked. An abort
///   (`WatchGuard::abort` or `JoinHandle::abort`) maps to Ok (we asked for it);
///   a panic maps to `WatchPanic` with payload.
fn join_to_server_result(
    join_result: Result<Result<(), ServerError>, tokio::task::JoinError>,
) -> Result<(), ServerError> {
    match join_result {
        Ok(inner) => inner,
        Err(join_err) if join_err.is_panic() => {
            let payload = panic_payload_to_string(join_err.into_panic());
            Err(ServerError::WatchPanic {
                payload,
                bt: Bt::capture(),
            })
        }
        Err(_cancelled) => Ok(()),
    }
}

/// Combine the leader-watch outcome with the tonic graceful-drain outcome
/// after the watch arm fired.
///
/// When the watch task terminates first we trigger the drain and then must
/// report a single result. The watch error is the root cause — poisoned
/// serving state was already published before the task returned — so it wins
/// when both fail. A drain error (port stolen, resource exhaustion) is only
/// surfaced when the watch outcome is otherwise `Ok`; previously it was
/// discarded via `let _ = serve.await`, hiding a failed drain behind a clean
/// shutdown report.
///
/// Generic over the drain error so the precedence logic is unit-testable
/// without fabricating a `tonic::transport::Error` (which has no public
/// constructor): production passes `Result<(), tonic::transport::Error>`,
/// tests pass `Result<(), ServerError>` via the reflexive `From` impl.
fn combine_watch_and_drain<E>(
    watch_result: Result<Result<(), ServerError>, tokio::task::JoinError>,
    drain_result: Result<(), E>,
) -> Result<(), ServerError>
where
    ServerError: From<E>,
{
    match join_to_server_result(watch_result) {
        Err(watch_err) => Err(watch_err),
        Ok(()) => drain_result.map_err(ServerError::from),
    }
}

/// Build the gRPC reflection service from an encoded protobuf descriptor set.
///
/// Factored out of [`Server::into_router`] so the decode-failure path is unit
/// testable: production passes [`tsoracle_proto::FILE_DESCRIPTOR_SET`], while
/// tests can feed deliberately corrupt bytes to exercise the error mapping.
/// A decode failure becomes [`ServerError::ReflectionInit`] rather than a panic.
#[cfg(feature = "reflection")]
fn build_reflection_service(
    descriptor_set: &[u8],
) -> Result<
    tonic_reflection::server::v1::ServerReflectionServer<
        impl tonic_reflection::server::v1::ServerReflection,
    >,
    ServerError,
> {
    tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(descriptor_set)
        .build_v1()
        .map_err(ServerError::ReflectionInit)
}

fn panic_payload_to_string(panic: Box<dyn std::any::Any + Send>) -> String {
    if let Some(text) = panic.downcast_ref::<&'static str>() {
        (*text).to_string()
    } else if let Some(text) = panic.downcast_ref::<String>() {
        text.clone()
    } else {
        "watch task panicked with non-string payload".to_string()
    }
}

#[cfg(any(test, feature = "test-fakes"))]
impl Server {
    /// Test-only entry point for the leader-watch loop. Exposed to integration
    /// tests via the `test-fakes` feature; not part of the stable public API.
    #[doc(hidden)]
    pub async fn run_leader_watch_for_tests(self: Arc<Self>) -> Result<(), ServerError> {
        // A never-resolving cancel future: these tests drive termination via
        // leadership events or `JoinHandle::abort`, not cooperative cancel.
        crate::fence::run_leader_watch(self, futures::future::pending()).await
    }

    /// Test-only allocator probe. Issues a window grant against the current
    /// in-memory state without going through the gRPC service. Used by
    /// regression tests that need to observe the behavioral fence (no
    /// timestamp at or below the prior leader's high-water) directly.
    #[doc(hidden)]
    pub fn try_grant_for_tests(&self, count: u32) -> Result<WindowGrant, CoreError> {
        self.core.try_grant(self.clock.now_ms(), count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn panic_payload_to_string_recovers_static_str() {
        // `panic!("literal")` produces a `&'static str` payload; we want the
        // verbatim text so operators see what the watch task said.
        let payload: Box<dyn std::any::Any + Send> = Box::new("watch boom");
        assert_eq!(panic_payload_to_string(payload), "watch boom");
    }

    #[test]
    fn panic_payload_to_string_recovers_owned_string() {
        // `panic!("{var}")` produces a `String` payload (formatted at panic
        // time); the helper must downcast that branch too.
        let payload: Box<dyn std::any::Any + Send> = Box::new(String::from("formatted"));
        assert_eq!(panic_payload_to_string(payload), "formatted");
    }

    #[test]
    fn panic_payload_to_string_falls_back_for_other_types() {
        // Custom payloads (panic!(MyType { .. })) hit the catch-all branch.
        struct Custom;
        let payload: Box<dyn std::any::Any + Send> = Box::new(Custom);
        assert_eq!(
            panic_payload_to_string(payload),
            "watch task panicked with non-string payload",
        );
    }

    #[test]
    fn serving_transitions_publish_through_core() {
        // The Server delegates serving-state transitions to its ServingCore; a
        // step_down on a freshly built Server lands as NotServing with the hint.
        // (The #346 send_replace-with-zero-receivers regression is pinned by the
        // ServingCore unit tests, which can observe `receiver_count` directly.)
        let server = Server::builder()
            .consensus_driver(Arc::new(crate::test_fakes::InMemoryDriver::new()))
            .build()
            .expect("build must succeed");

        let hint = PeerEndpoint::try_from("new-leader:9000").unwrap();
        server.core.step_down(Some(hint.clone()), Some(Epoch(7)));

        match server.core.serving_state() {
            ServingState::NotServing {
                leader_endpoint,
                leader_epoch,
            } => {
                assert_eq!(leader_endpoint, Some(hint));
                assert_eq!(leader_epoch, Some(Epoch(7)));
            }
            ServingState::Serving => panic!("expected NotServing after step_down"),
        }
    }

    #[cfg(any(feature = "tls-rustls", feature = "tls-native"))]
    #[test]
    fn builder_stores_tls_config() {
        // The serve_* paths read `tls_config` from `Server` (not the builder)
        // after `into_router` consumes self — so the field must survive the
        // builder → Server hand-off, not just the builder method.
        use crate::test_fakes::InMemoryDriver;

        let driver = Arc::new(InMemoryDriver::new());
        let cfg = tonic::transport::ServerTlsConfig::new();
        let server = Server::builder()
            .consensus_driver(driver)
            .tls_config(cfg)
            .build()
            .expect("build with tls_config must succeed");
        assert!(server.tls_config.is_some());
    }

    #[tokio::test]
    async fn join_to_server_result_passes_through_clean_outcome() {
        // Ok(Ok(())) — task finished cleanly; forward verbatim.
        let handle = tokio::spawn(async { Ok::<(), ServerError>(()) });
        let join = handle.await;
        assert!(matches!(join_to_server_result(join), Ok(())));
    }

    #[tokio::test]
    async fn join_to_server_result_forwards_inner_error() {
        // Ok(Err(e)) — task returned an error; forward it.
        let handle = tokio::spawn(async {
            Err::<(), ServerError>(ServerError::WatchPanic {
                payload: "synthetic".into(),
                bt: Bt::capture(),
            })
        });
        let join = handle.await;
        match join_to_server_result(join) {
            Err(ServerError::WatchPanic { payload, .. }) => assert_eq!(payload, "synthetic"),
            other => panic!("expected forwarded WatchPanic, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn join_to_server_result_translates_panic_to_watch_panic() {
        // Err(JoinError::is_panic) — task panicked; surface as WatchPanic with
        // the payload stringified by `panic_payload_to_string`.
        let handle = tokio::spawn(async {
            panic!("intentional");
            #[allow(unreachable_code)]
            Ok::<(), ServerError>(())
        });
        let join = handle.await;
        match join_to_server_result(join) {
            Err(ServerError::WatchPanic { payload, .. }) => {
                assert!(payload.contains("intentional"))
            }
            other => panic!("expected WatchPanic, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn join_to_server_result_treats_cancellation_as_clean_exit() {
        // Err(JoinError::is_cancelled) — caller aborted the task; we asked
        // for that, so map to Ok.
        let handle: tokio::task::JoinHandle<Result<(), ServerError>> =
            tokio::spawn(async { futures::future::pending().await });
        handle.abort();
        let join = handle.await;
        assert!(matches!(join_to_server_result(join), Ok(())));
    }

    #[tokio::test]
    async fn combine_watch_and_drain_surfaces_drain_error_when_watch_ok() {
        // Watch ended cleanly but the graceful drain failed (port stolen,
        // resource exhaustion). The drain error must not be swallowed.
        let watch = tokio::spawn(async { Ok::<(), ServerError>(()) }).await;
        let drain: Result<(), ServerError> = Err(ServerError::WatchStreamClosed);
        assert!(matches!(
            combine_watch_and_drain(watch, drain),
            Err(ServerError::WatchStreamClosed)
        ));
    }

    #[tokio::test]
    async fn combine_watch_and_drain_returns_ok_when_both_succeed() {
        // Watch clean, drain clean — the only fully-Ok outcome.
        let watch = tokio::spawn(async { Ok::<(), ServerError>(()) }).await;
        let drain: Result<(), ServerError> = Ok(());
        assert!(matches!(combine_watch_and_drain(watch, drain), Ok(())));
    }

    #[tokio::test]
    async fn combine_watch_and_drain_prefers_watch_error_over_drain_error() {
        // Both failed. The watch error is the root cause (poisoned state was
        // already published), so it wins; the drain error is dropped.
        let watch = tokio::spawn(async {
            Err::<(), ServerError>(ServerError::WatchPanic {
                payload: "watch".into(),
                bt: Bt::capture(),
            })
        })
        .await;
        let drain: Result<(), ServerError> = Err(ServerError::WatchStreamClosed);
        match combine_watch_and_drain(watch, drain) {
            Err(ServerError::WatchPanic { payload, .. }) => assert_eq!(payload, "watch"),
            other => panic!("expected watch error to win, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn combine_watch_and_drain_returns_watch_error_when_drain_ok() {
        // Watch failed, drain succeeded — forward the watch error verbatim.
        let watch = tokio::spawn(async {
            Err::<(), ServerError>(ServerError::WatchPanic {
                payload: "watch".into(),
                bt: Bt::capture(),
            })
        })
        .await;
        let drain: Result<(), ServerError> = Ok(());
        match combine_watch_and_drain(watch, drain) {
            Err(ServerError::WatchPanic { payload, .. }) => assert_eq!(payload, "watch"),
            other => panic!("expected forwarded watch error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn dropping_watch_guard_closes_serving_gate_synchronously() {
        // Regression: dropping the guard must close the serving gate at the
        // drop site, not on the watch task's later timeline. Build a guard whose
        // watch handle never touches the core (so the ONLY thing that can flip
        // the state to NotServing is `Drop`), publish `Serving`, then drop the
        // guard and read the state with NO await in between. On the current-thread
        // test runtime no other task can run between the synchronous `drop` and
        // the synchronous `serving_state` read, so observing `NotServing` proves
        // `Drop` closed the gate synchronously rather than the watch task.
        let core = Arc::new(ServingCore::new(Duration::from_secs(3)));
        core.publish_serving();

        let handle: tokio::task::JoinHandle<Result<(), ServerError>> =
            tokio::spawn(async { Ok(()) });
        let (cancel_tx, _cancel_rx) = tokio::sync::oneshot::channel::<()>();
        let guard = WatchGuard {
            cancel_tx: Some(cancel_tx),
            handle: Some(handle),
            shutdown_grace: Duration::from_secs(10),
            core: core.clone(),
            reporter: Arc::new(crate::reporter::Reporter::new()),
            heartbeat_cancel_tx: None,
            heartbeat_handle: None,
        };

        drop(guard);

        assert!(
            matches!(core.serving_state(), ServingState::NotServing { .. }),
            "dropping the WatchGuard must close the serving gate synchronously",
        );
    }

    #[tokio::test]
    async fn serve_inner_closes_serving_gate_on_user_shutdown() {
        // Regression for the serve path: when the caller's shutdown fires,
        // `serve_inner` drops the watch cancel sender and waits out the grace,
        // forcibly aborting the watch task if it overruns. A task parked upstream
        // of any cancel-observing await (modelled here by a never-resolving
        // future) is aborted before it can publish `NotServing`, so the gate
        // would stay open unless `serve_inner` closes it itself. Seed `Serving`,
        // run the user-shutdown arm with a zero grace (immediate abort), and
        // assert the gate is closed on return.
        let core = Arc::new(ServingCore::new(Duration::from_secs(3)));
        core.publish_serving();

        let watch_handle: tokio::task::JoinHandle<Result<(), ServerError>> =
            tokio::spawn(async { futures::future::pending().await });
        let (watch_cancel_tx, _watch_cancel_rx) = tokio::sync::oneshot::channel::<()>();
        let (tonic_cancel_tx, _tonic_cancel_rx) = tokio::sync::oneshot::channel::<()>();

        // A serve future that is immediately ready models the user's shutdown
        // having fired; with the biased select preferring the (pending) watch arm,
        // control reaches the user-shutdown arm.
        let serve_future = async { Ok::<(), tonic::transport::Error>(()) };

        let result = serve_inner(
            watch_cancel_tx,
            watch_handle,
            None, // heartbeat_cancel_tx — heartbeat disabled in this regression test
            None, // heartbeat_handle
            serve_future,
            tonic_cancel_tx,
            Duration::from_millis(0),
            core.clone(),
            Arc::new(crate::reporter::Reporter::new()),
        )
        .await;

        assert!(
            result.is_ok(),
            "user shutdown must return Ok, got {result:?}"
        );
        assert!(
            matches!(core.serving_state(), ServingState::NotServing { .. }),
            "serve_inner's user-shutdown arm must close the serving gate synchronously",
        );
    }

    #[cfg(feature = "reflection")]
    #[test]
    fn build_reflection_service_accepts_embedded_descriptor_set() {
        // The descriptor set emitted by `tsoracle-proto`'s build.rs must decode
        // cleanly — this is the production happy path that previously sat behind
        // an `expect`.
        assert!(build_reflection_service(tsoracle_proto::FILE_DESCRIPTOR_SET).is_ok());
    }

    #[cfg(feature = "reflection")]
    #[test]
    fn build_reflection_service_maps_corrupt_descriptor_to_typed_error() {
        // A descriptor set that fails to decode (build artifact drift) must
        // surface as a typed `ServerError::ReflectionInit`, not a panic. The
        // bytes below are not a valid encoded `FileDescriptorSet`.
        let corrupt = b"\xff\xff\xff\xff not a descriptor set";
        // The Ok variant wraps a reflection service that is not `Debug`, so map
        // to a unit result before asserting on the error variant.
        match build_reflection_service(corrupt).map(|_| ()) {
            Err(ServerError::ReflectionInit(_)) => {}
            other => panic!("expected ReflectionInit error, got {other:?}"),
        }
    }
}
