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

#[derive(Debug, thiserror::Error)]
pub enum SendError {
    #[error("receiver closed")]
    Closed,
}

pub fn leader_event_channel() -> (LeaderEventSender, LeaderEventStream) {
    let (tx, rx) = watch::channel(LeaderState::Unknown);
    (
        LeaderEventSender { tx },
        LeaderEventStream {
            inner: WatchStream::new(rx),
        },
    )
}

#[derive(Clone)]
pub struct LeaderEventSender {
    tx: watch::Sender<LeaderState>,
}

impl LeaderEventSender {
    pub fn send(&self, state: LeaderState) -> Result<(), SendError> {
        self.tx.send_if_modified(|prev| {
            if *prev == state {
                false
            } else {
                *prev = state;
                true
            }
        });
        if self.tx.is_closed() {
            return Err(SendError::Closed);
        }
        Ok(())
    }
}

pub struct LeaderEventStream {
    inner: WatchStream<LeaderState>,
}

impl LeaderEventStream {
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
