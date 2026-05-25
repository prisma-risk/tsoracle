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

use core::time::Duration;
use parking_lot::Mutex;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::watch;
use tonic::service::Routes;
use tonic::transport::Server as TonicServer;
use tsoracle_consensus::ConsensusDriver;
use tsoracle_core::{Allocator, Bt, Clock, Epoch, SystemClock};
#[cfg(any(test, feature = "test-fakes"))]
use tsoracle_core::{CoreError, WindowGrant};
use tsoracle_proto::v1::tso_service_server::TsoServiceServer;

use crate::service::TsoServiceImpl;

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
        leader_endpoint: Option<String>,
        leader_epoch: Option<Epoch>,
    },
    Serving,
}

pub struct ServerBuilder {
    consensus: Option<Arc<dyn ConsensusDriver>>,
    clock: Option<Arc<dyn Clock>>,
    window_ahead: Duration,
    failover_advance: Duration,
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
        // Discard the receiver minted by `watch::channel`: `state_tx` is the
        // single source of truth. It mints fresh receivers on demand via
        // `subscribe()` and reads its current value via `borrow()`, so no parked
        // receiver needs to live on the server. Publish sites use `send_replace`
        // (not `send`) so a transition still mutates and notifies even when no
        // embedder has subscribed and `receiver_count` is zero.
        let (state_tx, _) = watch::channel(ServingState::NotServing {
            leader_endpoint: None,
            leader_epoch: None,
        });
        Ok(Server {
            consensus,
            clock,
            window_ahead: self.window_ahead,
            failover_advance: self.failover_advance,
            allocator: Arc::new(Mutex::new(Allocator::new())),
            state_tx,
            extension_lock: Arc::new(tokio::sync::Mutex::new(())),
            extension_gate: Arc::new(tokio::sync::RwLock::new(())),
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
    pub(crate) allocator: Arc<Mutex<Allocator>>,
    /// Source of truth for serving state. Mints receivers for [`Server::subscribe`]
    /// and is read synchronously via `borrow()` by the service layer; publish
    /// sites use `send_replace` so transitions land even with zero receivers.
    pub(crate) state_tx: watch::Sender<ServingState>,
    /// Serializes window extensions so a stampeding burst of `WindowExhausted`
    /// requests resolves to a single `persist_high_water` round-trip. Acquired
    /// before `extension_gate`; combined with a recheck-after-acquire inside
    /// `extend_window`, latecomers find the window already extended and
    /// return without contacting consensus.
    pub(crate) extension_lock: Arc<tokio::sync::Mutex<()>>,
    /// Read-locked by window-extension calls for the duration of their
    /// prepare → persist → commit dance. The leader-watch task takes a
    /// write lock between clearing serving and calling load_high_water,
    /// which drains all in-flight extensions started under the prior epoch
    /// before the fence proceeds.
    pub(crate) extension_gate: Arc<tokio::sync::RwLock<()>>,
    #[cfg(any(feature = "tls-rustls", feature = "tls-native"))]
    pub(crate) tls_config: Option<tonic::transport::ServerTlsConfig>,
}

/// Raw parts produced by [`Server::into_router_parts`]: the gRPC `Routes`, the
/// leader-watch task's cooperative-cancel sender (dropping it stops the task),
/// and the task's join handle. [`Server::into_router`] wraps these into a
/// [`WatchGuard`]; the `serve_*` methods consume them directly via
/// [`serve_inner`].
type RouterParts = (
    Routes,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<Result<(), ServerError>>,
);

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
        self.state_tx.subscribe()
    }

    /// Single transition API used in response to evidence that the current
    /// epoch is no longer valid: consensus rejection (NotLeader/Fenced
    /// during persist), leader-watch task termination, or other authoritative
    /// signals of leadership loss.
    ///
    /// Clears the allocator BEFORE publishing `NotServing` so a racing
    /// `try_grant` either (a) observes `CoreError::NotLeader` rather than
    /// handing out a stale-epoch grant, or (b) succeeds against a still-valid
    /// epoch and the *next* call observes `NotServing`. Either ordering is
    /// safe; what is not safe is the inverse (publish first, clear later)
    /// because a request between those two steps would see `Serving` AND a
    /// still-leader allocator.
    ///
    /// Does NOT take `extension_gate.write()`. That would deadlock against
    /// in-flight extensions currently holding the read lock and awaiting
    /// `persist_high_water`. Those in-flights will either complete cleanly
    /// (the next `try_grant` then sees `NotServing`) or fail with
    /// NotLeader/Fenced (and reach this helper themselves — it is idempotent).
    ///
    /// `leader_epoch` carries the authoritative new-leader epoch when the
    /// rejection reveals it — e.g. `ConsensusError::Fenced { current, .. }`
    /// names the epoch that fenced us. Publishing it lets the resulting
    /// `NotServing` hint redirect clients with an epoch to validate against;
    /// callers with no epoch to offer (stream closure, panic) pass `None`.
    pub(crate) fn step_down_due_to_consensus_rejection(
        &self,
        leader_endpoint: Option<String>,
        leader_epoch: Option<Epoch>,
    ) {
        self.allocator.lock().on_leadership_lost();
        self.state_tx.send_replace(ServingState::NotServing {
            leader_endpoint,
            leader_epoch,
        });
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
        let (routes, cancel_tx, handle) = self.into_router_parts()?;
        Ok((
            routes,
            WatchGuard {
                cancel_tx: Some(cancel_tx),
                handle: Some(handle),
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
                        watch_server.step_down_due_to_consensus_rejection(None, None);
                        #[cfg(feature = "tracing")]
                        tracing::error!(error = %_e, "leader-watch terminated; serving disabled");
                    }
                    result
                }
                Err(panic_payload) => {
                    // Mirror the Err branch: poison BEFORE re-raising so
                    // guard-dropping embedders still observe NotServing.
                    watch_server.step_down_due_to_consensus_rejection(None, None);
                    #[cfg(feature = "tracing")]
                    tracing::error!("leader-watch panicked; serving disabled");
                    std::panic::resume_unwind(panic_payload);
                }
            }
        });

        let service = TsoServiceImpl { server };
        #[allow(unused_mut)]
        let mut routes = Routes::new(TsoServiceServer::new(service));
        #[cfg(feature = "reflection")]
        {
            routes = routes.add_service(reflection);
        }
        Ok((routes, cancel_tx, watch_handle))
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
    ///    The watch handle is aborted; any error it had been about to return
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

        let (routes, watch_cancel_tx, watch_handle) = self.into_router_parts()?;
        let (combined_shutdown, cancel_tx) = combined_shutdown_with_cancel(shutdown);

        let mut tonic = TonicServer::builder();
        #[cfg(any(feature = "tls-rustls", feature = "tls-native"))]
        if let Some(cfg) = tls_config {
            tonic = tonic.tls_config(cfg).map_err(ServerError::Transport)?;
        }
        let serve = tonic
            .add_routes(routes)
            .serve_with_shutdown(addr, combined_shutdown);

        serve_inner(watch_cancel_tx, watch_handle, serve, cancel_tx).await
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

        let (routes, watch_cancel_tx, watch_handle) = self.into_router_parts()?;
        let (combined_shutdown, cancel_tx) = combined_shutdown_with_cancel(shutdown);

        let incoming = tonic::transport::server::TcpIncoming::from(listener);

        let mut tonic = TonicServer::builder();
        #[cfg(any(feature = "tls-rustls", feature = "tls-native"))]
        if let Some(cfg) = tls_config {
            tonic = tonic.tls_config(cfg).map_err(ServerError::Transport)?;
        }
        let serve = tonic
            .add_routes(routes)
            .serve_with_incoming_shutdown(incoming, combined_shutdown);

        serve_inner(watch_cancel_tx, watch_handle, serve, cancel_tx).await
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
    pub async fn shutdown(mut self) -> Result<(), ServerError> {
        // Dropping the sender fires the task's cancel future.
        self.cancel_tx.take();
        match self.handle.take() {
            Some(handle) => join_to_server_result(handle.await),
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
        // Dropping the sender (if `shutdown` / `abort` did not already take it)
        // resolves the task's cancel future; the task then publishes
        // `NotServing` and returns. The `JoinHandle` is dropped here too,
        // detaching the task to finish its cooperative shutdown on its own.
        self.cancel_tx.take();
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
/// here.
async fn serve_inner<S>(
    watch_cancel_tx: tokio::sync::oneshot::Sender<()>,
    mut watch_handle: tokio::task::JoinHandle<Result<(), ServerError>>,
    serve_future: S,
    tonic_cancel_tx: tokio::sync::oneshot::Sender<()>,
) -> Result<(), ServerError>
where
    S: Future<Output = Result<(), tonic::transport::Error>>,
{
    tokio::pin!(serve_future);

    tokio::select! {
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
            // Stop the watch task cooperatively rather than with a hard
            // abort: dropping its cancel sender resolves the task's cancel
            // future, and we await the task so it never gets torn down
            // mid-fence while holding `extension_gate.write()`. The wait is
            // bounded — the cancel arms in `run_leader_watch` fire even when
            // a fence is stuck retrying a transient error. The task's own
            // outcome is a clean `Ok(())` (we asked it to stop) and is not
            // surfaced; the user-requested shutdown result wins.
            drop(watch_cancel_tx);
            let _ = (&mut watch_handle).await;
            serve_result?;
            Ok(())
        }
    }
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
        self.allocator.lock().try_grant(self.clock.now_ms(), count)
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
    fn serving_transitions_publish_without_an_external_subscriber() {
        // Regression guard for #346. With the parked `state_rx` field gone, the
        // plain `serve(addr)` path holds zero receivers unless an embedder calls
        // `subscribe()`. `watch::Sender::send` is a no-op that leaves the value
        // untouched when `receiver_count == 0`, so the publish sites use
        // `send_replace` (which mutates and notifies unconditionally) and reads
        // go through `state_tx.borrow()`. Were a publish site to use `send`,
        // this transition would be silently dropped and a leaderless-then-leader
        // server could stay NotServing forever.
        //
        // Built without `subscribe()`, so `receiver_count` is zero when the
        // production publish site below fires.
        let server = Server::builder()
            .consensus_driver(Arc::new(crate::test_fakes::InMemoryDriver::new()))
            .build()
            .expect("build must succeed");
        assert_eq!(server.state_tx.receiver_count(), 0);

        server.step_down_due_to_consensus_rejection(
            Some("http://new-leader:9000".into()),
            Some(Epoch(7)),
        );

        match &*server.state_tx.borrow() {
            ServingState::NotServing {
                leader_endpoint,
                leader_epoch,
            } => {
                assert_eq!(leader_endpoint.as_deref(), Some("http://new-leader:9000"));
                assert_eq!(*leader_epoch, Some(Epoch(7)));
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
