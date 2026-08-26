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

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;

/// One-per-node fail-fast signal. Any supervised transport-server task that
/// exits unexpectedly trips it; the bin's run loop selects on
/// [`FatalSignal::tripped`] next to the OS shutdown signal and exits non-zero
/// so an orchestrator restarts the node, instead of leaving a zombie whose
/// peer/admin surface is dead while the consensus driver keeps running.
#[derive(Clone)]
pub struct FatalSignal {
    component: watch::Sender<Option<&'static str>>,
}

impl FatalSignal {
    /// A signal with no failure recorded.
    pub fn new() -> Self {
        Self {
            component: watch::Sender::new(None),
        }
    }

    /// Record that `component`'s server task died. The first trip wins —
    /// later trips keep the original component so the surfaced error names
    /// the root failure, not a teardown cascade.
    pub fn trip(&self, component: &'static str) {
        self.component.send_if_modified(|current| {
            if current.is_none() {
                *current = Some(component);
                true
            } else {
                false
            }
        });
    }

    /// The component whose server died, if any. Non-blocking.
    pub fn check(&self) -> Option<&'static str> {
        *self.component.borrow()
    }

    /// Resolve with the failed component once the signal trips. Pending
    /// forever while the node stays healthy.
    pub async fn tripped(&self) -> &'static str {
        let mut watcher = self.component.subscribe();
        loop {
            if let Some(component) = *watcher.borrow_and_update() {
                return component;
            }
            // `&self` holds a sender, so the channel cannot close while we
            // wait; if it somehow does, "never trips" is the right reading.
            if watcher.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    }
}

impl Default for FatalSignal {
    fn default() -> Self {
        Self::new()
    }
}

/// Owns a spawned peer-transport server and shuts it down cooperatively.
/// The `file` driver has no peer transport, so it uses [`TransportHandle::noop`].
pub struct TransportHandle {
    cancel: Option<oneshot::Sender<()>>,
    join: Option<JoinHandle<()>>,
}

impl TransportHandle {
    /// A handle with nothing to shut down (file driver).
    pub fn noop() -> Self {
        Self {
            cancel: None,
            join: None,
        }
    }

    /// Wrap a spawned server task plus the trigger that asks it to stop.
    pub fn new(cancel: oneshot::Sender<()>, join: JoinHandle<()>) -> Self {
        Self {
            cancel: Some(cancel),
            join: Some(join),
        }
    }

    /// Spawn `serve` under supervision. The closure receives the shutdown
    /// future to hand to `serve_with_incoming_shutdown`, and the returned
    /// handle cancels it cooperatively exactly like [`TransportHandle::new`].
    /// Every other way out of the server — an `Err`, a clean return nobody
    /// asked for, or a panic — trips `fatal` with `component`, so the node
    /// fails fast instead of running on as a zombie with a dead transport.
    pub fn spawn_supervised<F, Fut, E>(
        component: &'static str,
        fatal: FatalSignal,
        serve: F,
    ) -> Self
    where
        F: FnOnce(Pin<Box<dyn Future<Output = ()> + Send>>) -> Fut,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: std::fmt::Debug + Send + 'static,
    {
        let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
        let stop_requested = Arc::new(AtomicBool::new(false));
        let stop_observed = stop_requested.clone();
        let shutdown: Pin<Box<dyn Future<Output = ()> + Send>> = Box::pin(async move {
            // Resolves on explicit cancel AND on handle drop (`RecvError`):
            // both mean stop was requested, so a subsequent `Ok` is clean.
            let _ = cancel_rx.await;
            stop_observed.store(true, Ordering::Release);
        });
        // The server runs in its own task so a panic surfaces here as a
        // `JoinError` instead of killing the supervisor with it.
        let server = tokio::spawn(serve(shutdown));
        let supervisor = tokio::spawn(async move {
            match server.await {
                Ok(Ok(())) if stop_requested.load(Ordering::Acquire) => {}
                Ok(Ok(())) => {
                    tracing::error!(component, "server exited without a shutdown request");
                    fatal.trip(component);
                }
                Ok(Err(err)) => {
                    tracing::error!(error = ?err, component, "server died");
                    fatal.trip(component);
                }
                Err(join_error) => {
                    tracing::error!(error = ?join_error, component, "server task panicked or was aborted");
                    fatal.trip(component);
                }
            }
        });
        Self::new(cancel_tx, supervisor)
    }

    /// Signal the peer server to stop and wait for the task to finish.
    /// Idempotent: calling twice is harmless.
    pub async fn shutdown(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            // Receiver dropped (task already gone) is fine.
            let _ = cancel.send(());
        }
        if let Some(join) = self.join.take()
            && let Err(err) = join.await
        {
            tracing::warn!(error = ?err, "peer transport task join error");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `shutdown` fires the cancel trigger, the task observes it and returns,
    /// and the join completes. A second `shutdown` is a no-op.
    #[tokio::test]
    async fn shutdown_signals_the_task_and_joins() {
        let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
        let join = tokio::spawn(async move {
            // Stand in for the peer server's `serve_with_incoming_shutdown`:
            // park until the cancel trigger fires.
            let _ = cancel_rx.await;
        });
        let mut handle = TransportHandle::new(cancel_tx, join);
        handle.shutdown().await;
        // Idempotent: cancel and join slots are already taken.
        handle.shutdown().await;
    }

    /// The file driver's no-op handle has nothing to signal or join.
    #[tokio::test]
    async fn noop_shutdown_is_harmless() {
        let mut handle = TransportHandle::noop();
        handle.shutdown().await;
    }

    /// If the spawned task has already finished, sending the (now-ignored)
    /// cancel and awaiting the completed join is still clean.
    #[tokio::test]
    async fn shutdown_after_task_already_returned() {
        let (cancel_tx, _cancel_rx) = oneshot::channel::<()>();
        let join = tokio::spawn(async {});
        // Let the empty task finish before we shut down.
        tokio::task::yield_now().await;
        let mut handle = TransportHandle::new(cancel_tx, join);
        handle.shutdown().await;
    }

    /// A fresh signal reports no failure.
    #[test]
    fn fatal_signal_starts_untripped() {
        assert_eq!(FatalSignal::new().check(), None);
    }

    /// The first component to trip wins; later trips do not overwrite it.
    #[tokio::test]
    async fn fatal_trip_records_first_component_only() {
        let fatal = FatalSignal::new();
        fatal.trip("peer server");
        fatal.trip("admin server");
        assert_eq!(fatal.check(), Some("peer server"));
        assert_eq!(fatal.tripped().await, "peer server");
    }

    /// A waiter parked on `tripped()` is woken by a later `trip()`.
    #[tokio::test]
    async fn fatal_tripped_wakes_a_pending_waiter() {
        let fatal = FatalSignal::new();
        let waiter = tokio::spawn({
            let fatal = fatal.clone();
            async move { fatal.tripped().await }
        });
        // Let the waiter park before tripping.
        tokio::task::yield_now().await;
        fatal.trip("peer server");
        assert_eq!(waiter.await.unwrap(), "peer server");
    }

    /// A server future that returns `Err` trips the fatal signal with the
    /// component label — the zombie-node condition from issue #616.
    #[tokio::test]
    async fn supervised_error_exit_trips_fatal() {
        let fatal = FatalSignal::new();
        let mut handle =
            TransportHandle::spawn_supervised("test server", fatal.clone(), |_shutdown| async {
                Err::<(), std::io::Error>(std::io::Error::other("listener torn down"))
            });
        assert_eq!(fatal.tripped().await, "test server");
        handle.shutdown().await;
    }

    /// The graceful path — cancel requested, server drains and returns `Ok`
    /// — must NOT trip the signal.
    #[tokio::test]
    async fn supervised_graceful_cancel_does_not_trip() {
        let fatal = FatalSignal::new();
        let mut handle = TransportHandle::spawn_supervised(
            "test server",
            fatal.clone(),
            |shutdown| async move {
                shutdown.await;
                Ok::<(), std::io::Error>(())
            },
        );
        // Joins the supervisor, so the post-exit bookkeeping has run.
        handle.shutdown().await;
        assert_eq!(fatal.check(), None);
    }

    /// A server that returns `Ok` before anyone asked it to stop is just as
    /// dead as one that errored: trip.
    #[tokio::test]
    async fn supervised_premature_clean_exit_trips_fatal() {
        let fatal = FatalSignal::new();
        let mut handle =
            TransportHandle::spawn_supervised("test server", fatal.clone(), |_shutdown| async {
                Ok::<(), std::io::Error>(())
            });
        assert_eq!(fatal.tripped().await, "test server");
        handle.shutdown().await;
    }

    /// A panic inside the server future must also trip the signal — a flat
    /// task would die with the panic and never report.
    #[tokio::test]
    async fn supervised_panic_trips_fatal() {
        let fatal = FatalSignal::new();
        let mut handle =
            TransportHandle::spawn_supervised("test server", fatal.clone(), |_shutdown| async {
                panic!("server task blew up");
                #[allow(unreachable_code)]
                Ok::<(), std::io::Error>(())
            });
        assert_eq!(fatal.tripped().await, "test server");
        handle.shutdown().await;
    }

    /// Dropping the handle without calling `shutdown()` is intentional
    /// teardown (the cancel sender drop resolves the shutdown future), not a
    /// server death: no trip.
    #[tokio::test]
    async fn supervised_drop_without_shutdown_does_not_trip() {
        let fatal = FatalSignal::new();
        let handle = TransportHandle::spawn_supervised(
            "test server",
            fatal.clone(),
            |shutdown| async move {
                shutdown.await;
                Ok::<(), std::io::Error>(())
            },
        );
        drop(handle);
        for _ in 0..32 {
            tokio::task::yield_now().await;
            assert_eq!(fatal.check(), None);
        }
    }

    /// End-to-end through real tonic: an incoming stream that errors kills
    /// `serve_with_incoming_shutdown`, and supervision converts that into a
    /// fatal trip (whether tonic surfaces it as `Err` or an early `Ok`).
    #[cfg(feature = "openraft")]
    #[tokio::test]
    async fn tonic_incoming_error_propagates_to_fatal() {
        use std::sync::Arc;

        use crate::admin::service::AdminServiceImpl;
        use crate::admin::{MembershipAdmin, MembershipView, UnsupportedAdmin};
        use crate::admin_proto::membership_admin_server::MembershipAdminServer;

        let admin: Arc<dyn MembershipAdmin> = Arc::new(UnsupportedAdmin::new(MembershipView {
            members: Vec::new(),
            leader: None,
        }));
        let service = MembershipAdminServer::new(AdminServiceImpl::new(admin));
        let incoming = futures::stream::iter(vec![Err::<tokio::net::TcpStream, std::io::Error>(
            std::io::Error::other("accept failed"),
        )]);
        let fatal = FatalSignal::new();
        let mut handle =
            TransportHandle::spawn_supervised("admin server", fatal.clone(), move |shutdown| {
                tonic::transport::Server::builder()
                    .add_service(service)
                    .serve_with_incoming_shutdown(incoming, shutdown)
            });
        assert_eq!(fatal.tripped().await, "admin server");
        handle.shutdown().await;
    }
}
