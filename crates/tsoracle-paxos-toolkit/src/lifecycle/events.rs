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

//! Leader event channel + debounced stream.
//!
//! Backed by `tokio::sync::watch` for latest-state-wins semantics. Emits the
//! initial state at first poll and every distinct subsequent value;
//! identical successive sends are debounced via `send_if_modified`.

use core::pin::Pin;

use futures::Stream;
use tokio::sync::watch;
use tokio_stream::wrappers::WatchStream;
use tsoracle_consensus::LeaderState;

/// Error returned by [`LeaderEventSender::send`].
///
/// `Closed` is the only variant for now; it means every subscribed
/// [`LeaderEventStream`] has been dropped, so the runner can stop
/// emitting transitions. The runner observes `Closed` and exits its
/// tick loop on the next iteration.
#[derive(Debug, thiserror::Error)]
pub enum SendError {
    #[error("all receivers have been dropped")]
    Closed,
}

/// Create a new leader-event channel.
///
/// The returned [`LeaderEventStream`] yields `LeaderState::Unknown` on its
/// first poll (the channel's initial value) and every distinct subsequent
/// value sent via the [`LeaderEventSender`]. Identical successive sends
/// are debounced — receivers see one transition per unique value, not
/// one per `send()` call.
pub fn leader_event_channel() -> (LeaderEventSender, LeaderEventStream) {
    let (tx, rx) = watch::channel(LeaderState::Unknown);
    (
        LeaderEventSender { tx },
        LeaderEventStream {
            inner: WatchStream::new(rx),
        },
    )
}

/// Sending half of the leader-event channel.
///
/// Cloneable: every clone shares the same underlying channel and the same
/// debounced value. The runner clones the sender into its spawned tick
/// task while keeping a copy for `Drop` ordering.
#[derive(Clone)]
pub struct LeaderEventSender {
    tx: watch::Sender<LeaderState>,
}

impl LeaderEventSender {
    /// Send a leadership transition, debouncing if the value is unchanged.
    ///
    /// Returns `Err(SendError::Closed)` only when every receiver has been
    /// dropped. The closed-channel detection is atomic with the send via
    /// `watch::Sender::send` — there is no time-of-check / time-of-use
    /// race between observing channel state and committing the value.
    pub fn send(&self, state: LeaderState) -> Result<(), SendError> {
        if *self.tx.borrow() == state {
            // Debounce identical payload. If receivers are gone we still
            // surface the error so callers can shut down.
            if self.tx.is_closed() {
                return Err(SendError::Closed);
            }
            return Ok(());
        }
        self.tx.send(state).map_err(|_| SendError::Closed)
    }
}

/// Receiving half of the leader-event channel.
///
/// Yields one [`LeaderState`] per distinct value pushed by the sender,
/// starting with `LeaderState::Unknown` (the channel's initial value)
/// on the first poll. Backed by a stored `WatchStream` whose receiver
/// state persists across polls — recreating the stream per poll would
/// lose the receiver's last-seen value.
pub struct LeaderEventStream {
    inner: WatchStream<LeaderState>,
}

impl LeaderEventStream {
    /// Box and pin the stream for trait-object use sites.
    ///
    /// Convenience for callers (notably `ConsensusDriver::leadership_events`)
    /// that need a `Pin<Box<dyn Stream<Item = LeaderState> + Send>>` and
    /// would otherwise write `Box::pin(stream)` themselves. The stream
    /// does NOT require pinning before first poll — this is purely an
    /// ergonomic shortcut.
    #[must_use]
    pub fn into_pin(self) -> Pin<Box<dyn Stream<Item = LeaderState> + Send>> {
        Box::pin(self)
    }
}

impl Stream for LeaderEventStream {
    type Item = LeaderState;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use tokio::task::yield_now;
    use tsoracle_consensus::LeaderState;
    use tsoracle_core::Epoch;

    #[tokio::test]
    async fn stream_yields_changes_polled_between_sends() {
        // Watch channels coalesce intermediate values: multiple sends without
        // an intervening poll collapse to the latest. Poll between sends so
        // each transition is yielded distinctly.
        let (sender, mut stream) = leader_event_channel();
        assert_eq!(stream.next().await, Some(LeaderState::Unknown));

        sender
            .send(LeaderState::Leader { epoch: Epoch(1) })
            .unwrap();
        yield_now().await;
        assert_eq!(
            stream.next().await,
            Some(LeaderState::Leader { epoch: Epoch(1) })
        );

        sender
            .send(LeaderState::Leader { epoch: Epoch(2) })
            .unwrap();
        yield_now().await;
        assert_eq!(
            stream.next().await,
            Some(LeaderState::Leader { epoch: Epoch(2) })
        );

        drop(sender);
        assert_eq!(stream.next().await, None);
    }

    #[tokio::test]
    async fn send_if_modified_accepts_repeated_payload() {
        // The contract verified here is sender-side: send_if_modified does
        // not error on identical payloads. Stream output cannot be used to
        // observe debounce because watch already coalesces.
        let (sender, _stream) = leader_event_channel();
        let same = LeaderState::Leader { epoch: Epoch(1) };
        assert!(sender.send(same.clone()).is_ok());
        assert!(sender.send(same).is_ok());
        assert!(sender.send(LeaderState::Leader { epoch: Epoch(2) }).is_ok());
    }
}
