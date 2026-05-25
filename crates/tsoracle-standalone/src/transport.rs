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
