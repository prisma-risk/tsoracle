use core::time::Duration;
use parking_lot::Mutex;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::watch;
use tonic::service::Routes;
use tonic::transport::Server as TonicServer;
use tsoracle_consensus::ConsensusDriver;
use tsoracle_core::{Allocator, Clock, SystemClock};
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
    #[error("leader-watch task panicked: {payload}")]
    WatchPanic { payload: String },
}

#[derive(Clone, Debug)]
pub enum ServingState {
    NotServing { leader_endpoint: Option<String> },
    Serving,
}

pub struct ServerBuilder {
    consensus: Option<Arc<dyn ConsensusDriver>>,
    clock: Option<Arc<dyn Clock>>,
    window_ahead: Duration,
    failover_advance: Duration,
}

impl Default for ServerBuilder {
    fn default() -> Self {
        ServerBuilder {
            consensus: None,
            clock: None,
            window_ahead: Duration::from_secs(3),
            failover_advance: Duration::from_secs(1),
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
    pub fn build(self) -> Result<Server, BuildError> {
        let consensus = self.consensus.ok_or(BuildError::MissingConsensusDriver)?;
        let clock = self.clock.unwrap_or_else(|| Arc::new(SystemClock));
        let (state_tx, state_rx) = watch::channel(ServingState::NotServing {
            leader_endpoint: None,
        });
        Ok(Server {
            consensus,
            clock,
            window_ahead: self.window_ahead,
            failover_advance: self.failover_advance,
            allocator: Arc::new(Mutex::new(Allocator::new())),
            state_tx,
            state_rx,
            extension_lock: Arc::new(tokio::sync::Mutex::new(())),
            extension_gate: Arc::new(tokio::sync::RwLock::new(())),
        })
    }
}

pub struct Server {
    pub(crate) consensus: Arc<dyn ConsensusDriver>,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) window_ahead: Duration,
    pub(crate) failover_advance: Duration,
    pub(crate) allocator: Arc<Mutex<Allocator>>,
    pub(crate) state_tx: watch::Sender<ServingState>,
    pub state_rx: watch::Receiver<ServingState>,
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
}

impl Server {
    pub fn builder() -> ServerBuilder {
        ServerBuilder::default()
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
    pub(crate) fn step_down_due_to_consensus_rejection(&self, leader_endpoint: Option<String>) {
        self.allocator.lock().on_leadership_lost();
        let _ = self
            .state_tx
            .send(ServingState::NotServing { leader_endpoint });
    }
}

impl Server {
    /// Return the configured `TsoServiceServer<TsoServiceImpl>` as a tonic
    /// `Routes` value plus a `JoinHandle` for the spawned leader-watch task,
    /// so callers can mount tsoracle's service alongside their own services
    /// on a shared tonic listener instead of binding a dedicated port.
    ///
    /// The `JoinHandle` payload is `Result<(), ServerError>` so embedders
    /// can observe leader-watch termination. Before returning an error, the
    /// task publishes `ServingState::NotServing { leader_endpoint: None }`
    /// so all subsequent RPCs fail fast with `FAILED_PRECONDITION` — even
    /// embedders who never inspect the handle get fail-safe behavior.
    ///
    /// The `Server::serve()` method is a thin wrapper over this — it calls
    /// `into_router`, builds a tonic `Server`, and binds a listener.
    pub fn into_router(self) -> (Routes, tokio::task::JoinHandle<Result<(), ServerError>>) {
        let server = Arc::new(self);

        let watch_server = server.clone();
        let watch_handle = tokio::spawn(async move {
            use futures::FutureExt;
            // catch_unwind so a panic in run_leader_watch still routes through
            // the poisoning path. Without this, embedders who mount into_router
            // directly and never observe the JoinHandle would see
            // ServingState::Serving remain published while the watch task is
            // dead — the inverse of the fail-safe guarantee documented above.
            // The panic is re-raised after poisoning so serve / serve_with_*
            // continue to translate it into ServerError::WatchPanic via
            // join_to_server_result.
            let outcome =
                std::panic::AssertUnwindSafe(crate::fence::run_leader_watch(watch_server.clone()))
                    .catch_unwind()
                    .await;
            match outcome {
                Ok(result) => {
                    if let Err(ref _e) = result {
                        // Poison BEFORE returning so embedders who do not observe
                        // the JoinHandle still get fail-safe behavior.
                        watch_server.step_down_due_to_consensus_rejection(None);
                        #[cfg(feature = "tracing")]
                        tracing::error!(error = %_e, "leader-watch terminated; serving disabled");
                    }
                    result
                }
                Err(panic_payload) => {
                    // Mirror the Err branch: poison BEFORE re-raising so
                    // handle-dropping embedders still observe NotServing.
                    watch_server.step_down_due_to_consensus_rejection(None);
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
            #[expect(
                clippy::expect_used,
                reason = "`FILE_DESCRIPTOR_SET` is generated by `tsoracle-proto`'s `build.rs` from checked-in `.proto` sources; if it ever fails to decode, the build itself is broken. Tracked by #9."
            )]
            let reflection = tonic_reflection::server::Builder::configure()
                .register_encoded_file_descriptor_set(tsoracle_proto::FILE_DESCRIPTOR_SET)
                .build_v1()
                .expect("FILE_DESCRIPTOR_SET emitted by build.rs is always valid");
            routes = routes.add_service(reflection);
        }
        (routes, watch_handle)
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
    ///    timestamps they were allocated; new calls fail fast. Returns `Err(e)`.
    /// 3. Watch task panics → returns `Err(ServerError::WatchPanic{..})`
    ///    with the panic payload stringified. Same drain semantics as (2).
    pub async fn serve_with_shutdown(
        self,
        addr: SocketAddr,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> Result<(), ServerError> {
        let (routes, mut watch_handle) = self.into_router();
        let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<()>();

        // tonic stops when EITHER the user's shutdown fires OR we cancel
        // because the watch task terminated.
        let combined_shutdown = async move {
            tokio::select! {
                _ = shutdown => {}
                _ = cancel_rx => {}
            }
        };

        let serve = TonicServer::builder()
            .add_routes(routes)
            .serve_with_shutdown(addr, combined_shutdown);
        tokio::pin!(serve);

        tokio::select! {
            // Bias toward the watch arm: if both are ready in the same poll
            // (rare but possible — graceful shutdown completed in the same
            // tick the watch returned), we want to surface the watch error
            // rather than report a clean shutdown.
            biased;

            watch_result = &mut watch_handle => {
                // Watch terminated. State is already poisoned (see watch
                // task body in into_router). Trigger tonic drain and wait
                // for it to finish, then report the watch's outcome.
                let _ = cancel_tx.send(());
                let _ = serve.await;
                join_to_server_result(watch_result)
            }
            serve_result = &mut serve => {
                // User shutdown fired (or our cancel — but watch arm has
                // `biased` priority, so reaching here means user shutdown).
                // The watch task may still be running; aborting it loses
                // any error it was about to report, but the process is
                // shutting down so that's acceptable.
                watch_handle.abort();
                serve_result?;
                Ok(())
            }
        }
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
    ///    fail fast. Returns `Err(e)`.
    /// 3. Watch task panics → returns `Err(ServerError::WatchPanic{..})`
    ///    with the panic payload stringified. Same drain semantics as (2).
    pub async fn serve_with_listener(
        self,
        listener: tokio::net::TcpListener,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> Result<(), ServerError> {
        let (routes, mut watch_handle) = self.into_router();
        let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<()>();

        let combined_shutdown = async move {
            tokio::select! {
                _ = shutdown => {}
                _ = cancel_rx => {}
            }
        };

        let incoming = tonic::transport::server::TcpIncoming::from(listener);

        let serve = TonicServer::builder()
            .add_routes(routes)
            .serve_with_incoming_shutdown(incoming, combined_shutdown);
        tokio::pin!(serve);

        tokio::select! {
            biased;

            watch_result = &mut watch_handle => {
                let _ = cancel_tx.send(());
                let _ = serve.await;
                join_to_server_result(watch_result)
            }
            serve_result = &mut serve => {
                watch_handle.abort();
                serve_result?;
                Ok(())
            }
        }
    }
}

/// Convert a `JoinHandle` result into a `ServerError`-typed result.
///
/// - `Ok(Ok(()))` — task ended cleanly (driver stream closed). Caller decides
///   whether this is normal (shutdown) or anomalous.
/// - `Ok(Err(e))` — task returned an error. Forward verbatim.
/// - `Err(JoinError)` — task was cancelled or panicked. Cancellation maps to
///   Ok (we asked for it); panic maps to `WatchPanic` with payload.
fn join_to_server_result(
    join_result: Result<Result<(), ServerError>, tokio::task::JoinError>,
) -> Result<(), ServerError> {
    match join_result {
        Ok(inner) => inner,
        Err(join_err) if join_err.is_panic() => {
            let payload = panic_payload_to_string(join_err.into_panic());
            Err(ServerError::WatchPanic { payload })
        }
        Err(_cancelled) => Ok(()),
    }
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
        crate::fence::run_leader_watch(self).await
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
