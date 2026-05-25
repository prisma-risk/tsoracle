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

use tokio::sync::oneshot;
use tokio::task::JoinHandle;

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

    /// Signal the peer server to stop and wait for the task to finish.
    /// Idempotent: calling twice is harmless.
    pub async fn shutdown(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            // Receiver dropped (task already gone) is fine.
            let _ = cancel.send(());
        }
        if let Some(join) = self.join.take() {
            if let Err(err) = join.await {
                tracing::warn!(error = ?err, "peer transport task join error");
            }
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
}
