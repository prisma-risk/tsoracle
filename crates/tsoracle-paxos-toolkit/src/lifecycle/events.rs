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
/// Returns a [`LeaderEventSender`] (the runner's emit half) and a
/// [`LeaderEventSubscriber`] (the consumer's subscribe half). The
/// subscriber mints a fresh [`LeaderEventStream`] on every
/// [`LeaderEventSubscriber::subscribe`] call; each stream yields the
/// channel's *current* value (`LeaderState::Unknown` until the first
/// transition is sent) on its first poll and every distinct subsequent
/// value. Identical successive sends are debounced — a stream sees one
/// transition per unique value, not one per `send()` call.
pub fn leader_event_channel() -> (LeaderEventSender, LeaderEventSubscriber) {
    let (tx, rx) = watch::channel(LeaderState::Unknown);
    (LeaderEventSender { tx }, LeaderEventSubscriber { rx })
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

/// Subscribe half of the leader-event channel.
///
/// Holds a `watch::Receiver` and mints a fresh [`LeaderEventStream`] on every
/// [`Self::subscribe`] call — the analogue of cloning an openraft metrics watch
/// receiver. Two properties make this the right handle to hand a consumer that
/// may subscribe more than once (e.g. a driver re-derived across an in-process
/// restart):
///
/// - **Re-subscribable:** every `subscribe()` returns an independent live
///   stream whose first poll yields the channel's *current* value, so a second
///   (or third) subscription is never a silent blackout.
/// - **Keeps the channel open:** the retained receiver counts toward the
///   channel's receiver count, so while a subscriber is held the sender's
///   `send` cannot observe a closed channel. The runner's drop-based shutdown
///   (all receivers gone ⇒ [`SendError::Closed`]) therefore fires only once
///   this subscriber *and* every stream it minted have been dropped.
///
/// Cloneable: each clone is an independent receiver over the same channel.
#[derive(Clone)]
pub struct LeaderEventSubscriber {
    rx: watch::Receiver<LeaderState>,
}

impl LeaderEventSubscriber {
    /// Mint a fresh [`LeaderEventStream`] over this channel.
    ///
    /// The returned stream yields the channel's current [`LeaderState`] on its
    /// first poll (`WatchStream::new` reads the held value rather than waiting
    /// for the next change) and every distinct subsequent value. Calling this
    /// repeatedly is well-defined: each call hands back a new, independent
    /// stream, so a late or second subscriber is never blacked out.
    #[must_use]
    pub fn subscribe(&self) -> LeaderEventStream {
        LeaderEventStream {
            inner: WatchStream::new(self.rx.clone()),
        }
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
        let (sender, subscriber) = leader_event_channel();
        let mut stream = subscriber.subscribe();
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
        // The contract verified here is sender-side: send does not error
        // on identical payloads while a receiver is alive. Stream output
        // cannot be used to observe debounce because watch already
        // coalesces.
        let (sender, _subscriber) = leader_event_channel();
        let same = LeaderState::Leader { epoch: Epoch(1) };
        assert!(sender.send(same.clone()).is_ok());
        assert!(sender.send(same).is_ok());
        assert!(sender.send(LeaderState::Leader { epoch: Epoch(2) }).is_ok());
    }

    #[tokio::test]
    async fn send_returns_closed_after_subscriber_dropped() {
        // Atomicity contract: dropping the last receiver (here the subscriber,
        // which has minted no stream) makes the next send return
        // SendError::Closed immediately. Exercises the change-path in `send`
        // (the watch::Sender::send arm).
        let (sender, subscriber) = leader_event_channel();
        drop(subscriber);
        let result = sender.send(LeaderState::Leader { epoch: Epoch(1) });
        assert!(matches!(result, Err(SendError::Closed)));
    }

    #[tokio::test]
    async fn send_returns_closed_for_debounced_payload_after_drop() {
        // Exercises the debounce-with-closed-channel arm: an unchanged
        // payload still surfaces SendError::Closed when every receiver is
        // gone, so the runner can shut down even when nothing changed.
        let (sender, subscriber) = leader_event_channel();
        sender
            .send(LeaderState::Leader { epoch: Epoch(1) })
            .expect("first send succeeds while subscriber alive");
        drop(subscriber);
        let result = sender.send(LeaderState::Leader { epoch: Epoch(1) });
        assert!(matches!(result, Err(SendError::Closed)));
    }

    #[tokio::test]
    async fn send_stays_open_while_a_minted_stream_outlives_the_subscriber() {
        // A stream minted from a subscriber is itself a receiver, so dropping
        // the subscriber while a stream is still alive must NOT close the
        // channel — the runner keeps emitting to the surviving stream.
        let (sender, subscriber) = leader_event_channel();
        let _stream = subscriber.subscribe();
        drop(subscriber);
        assert!(
            sender.send(LeaderState::Leader { epoch: Epoch(1) }).is_ok(),
            "channel must stay open while a minted stream survives the subscriber",
        );
    }

    #[tokio::test]
    async fn into_pin_stream_dropped_closes_channel() {
        // The into_pin'd trait-object stream is still a watch receiver: once it
        // and the subscriber are dropped, the channel closes. Guards the boxed
        // path the driver actually hands the server.
        let (sender, subscriber) = leader_event_channel();
        let pinned = subscriber.subscribe().into_pin();
        drop(subscriber);
        drop(pinned);
        assert!(matches!(
            sender.send(LeaderState::Leader { epoch: Epoch(1) }),
            Err(SendError::Closed)
        ));
    }

    #[tokio::test]
    async fn subscriber_mints_independent_streams_each_yielding_current_state() {
        // A subscriber re-derives a fresh stream on every call, and each fresh
        // stream yields the channel's *current* value on its first poll — the
        // synchronous-first-item contract `ConsensusDriver::leadership_events`
        // requires. Crucially a *second* subscription is not a blackout: both
        // streams below see the post-send leader state, not an empty stream.
        let (sender, subscriber) = leader_event_channel();
        sender
            .send(LeaderState::Leader { epoch: Epoch(1) })
            .unwrap();

        let mut first = subscriber.subscribe();
        let mut second = subscriber.subscribe();
        assert_eq!(
            first.next().await,
            Some(LeaderState::Leader { epoch: Epoch(1) })
        );
        assert_eq!(
            second.next().await,
            Some(LeaderState::Leader { epoch: Epoch(1) })
        );
    }

    #[tokio::test]
    async fn subscriber_streams_observe_transitions_after_subscribe() {
        // A stream minted before a transition still observes the transition:
        // proves subscribe() returns a live WatchStream, not a one-shot
        // snapshot of the value at subscription time.
        let (sender, subscriber) = leader_event_channel();
        let mut stream = subscriber.subscribe();
        assert_eq!(stream.next().await, Some(LeaderState::Unknown));

        sender
            .send(LeaderState::Leader { epoch: Epoch(2) })
            .unwrap();
        yield_now().await;
        assert_eq!(
            stream.next().await,
            Some(LeaderState::Leader { epoch: Epoch(2) })
        );
    }

    #[tokio::test]
    async fn into_pin_yields_stream_with_initial_and_changes() {
        // Confirms that `into_pin` produces a stream functionally
        // equivalent to the underlying LeaderEventStream — yields the
        // initial value and subsequent changes via the boxed-pinned
        // trait object path consumers use.
        let (sender, subscriber) = leader_event_channel();
        let mut pinned = subscriber.subscribe().into_pin();
        assert_eq!(pinned.next().await, Some(LeaderState::Unknown));

        sender
            .send(LeaderState::Leader { epoch: Epoch(7) })
            .unwrap();
        yield_now().await;
        assert_eq!(
            pinned.next().await,
            Some(LeaderState::Leader { epoch: Epoch(7) }),
        );
    }
}
