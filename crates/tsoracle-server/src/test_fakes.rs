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

//! In-memory `ConsensusDriver` for integration tests.

use core::pin::Pin;
use futures::{Stream, StreamExt};
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::watch;
use tokio_stream::wrappers::WatchStream;
use tsoracle_consensus::{ConsensusDriver, ConsensusError, LeaderState};
use tsoracle_core::Epoch;

#[derive(Clone)]
pub struct InMemoryDriver {
    state: Arc<Mutex<u64>>,
    tx: watch::Sender<LeaderState>,
    rx: watch::Receiver<LeaderState>,
}

impl Default for InMemoryDriver {
    fn default() -> Self {
        let (tx, rx) = watch::channel(LeaderState::Unknown);
        InMemoryDriver {
            state: Arc::new(Mutex::new(0)),
            tx,
            rx,
        }
    }
}

impl InMemoryDriver {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn become_leader(&self, epoch: Epoch) {
        let _ = self.tx.send(LeaderState::Leader { epoch });
    }

    pub fn become_follower(&self, hint: Option<String>) {
        let _ = self.tx.send(LeaderState::Follower {
            leader_endpoint: hint,
        });
    }

    pub fn current_high_water(&self) -> u64 {
        *self.state.lock()
    }
}

#[async_trait::async_trait]
impl ConsensusDriver for InMemoryDriver {
    fn leadership_events(&self) -> Pin<Box<dyn Stream<Item = LeaderState> + Send>> {
        Box::pin(WatchStream::new(self.rx.clone()).boxed())
    }

    async fn load_high_water(&self) -> Result<u64, ConsensusError> {
        Ok(*self.state.lock())
    }

    async fn persist_high_water(
        &self,
        at_least: u64,
        _epoch: Epoch,
    ) -> Result<u64, ConsensusError> {
        let mut high_water = self.state.lock();
        if at_least > *high_water {
            *high_water = at_least;
        }
        Ok(*high_water)
    }
}

/// Wraps an [`InMemoryDriver`] with a knob that holds `persist_high_water`
/// for tests that need to simulate a slow consensus driver.
///
/// Stall semantics are indexed by call order. Every `persist_high_water`
/// invocation reads its zero-based call index from a monotonic counter and
/// blocks while that index is at or above the configured threshold; the
/// threshold defaults to `u64::MAX` (no stalling).
///
/// Use [`Self::stall_from`] to mark every persist call with index >=
/// `threshold` as stalled, and [`Self::release`] to unblock all waiting
/// calls and disable stalling for future calls. [`Self::persist_call_count`]
/// returns the number of persist calls that have started so far.
#[derive(Clone)]
pub struct StallableDriver {
    inner: InMemoryDriver,
    stall_threshold_tx: watch::Sender<u64>,
    // Held purely so the `watch::Sender` always has at least one live
    // receiver — without this, `send` returns an error and the value is
    // never observed by future `subscribe()` calls.
    _stall_threshold_keepalive_rx: watch::Receiver<u64>,
    persist_calls: Arc<AtomicU64>,
}

impl Default for StallableDriver {
    fn default() -> Self {
        let (stall_threshold_tx, _stall_threshold_keepalive_rx) = watch::channel(u64::MAX);
        StallableDriver {
            inner: InMemoryDriver::new(),
            stall_threshold_tx,
            _stall_threshold_keepalive_rx,
            persist_calls: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl StallableDriver {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn become_leader(&self, epoch: Epoch) {
        self.inner.become_leader(epoch);
    }

    pub fn become_follower(&self, hint: Option<String>) {
        self.inner.become_follower(hint);
    }

    pub fn current_high_water(&self) -> u64 {
        self.inner.current_high_water()
    }

    /// Cause every `persist_high_water` invocation whose call index is at
    /// or above `threshold` to block until [`Self::release`] is called.
    /// Calls with index `< threshold` (whether already in flight or yet to
    /// arrive) are unaffected.
    pub fn stall_from(&self, threshold: u64) {
        // `send` only fails when every receiver has been dropped, which
        // cannot happen here — `_stall_threshold_keepalive_rx` is owned by
        // this struct and lives as long as the sender does. Drop the
        // result rather than `.expect()`-ing it so the strict
        // `clippy::expect_used` lint stays clean.
        let _ = self.stall_threshold_tx.send(threshold);
    }

    /// Unblock every pending stalled persist call and disable stalling for
    /// future calls.
    pub fn release(&self) {
        let _ = self.stall_threshold_tx.send(u64::MAX);
    }

    /// Number of `persist_high_water` calls that have started so far,
    /// including any that are currently stalled.
    pub fn persist_call_count(&self) -> u64 {
        self.persist_calls.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl ConsensusDriver for StallableDriver {
    fn leadership_events(&self) -> Pin<Box<dyn Stream<Item = LeaderState> + Send>> {
        self.inner.leadership_events()
    }

    async fn load_high_water(&self) -> Result<u64, ConsensusError> {
        self.inner.load_high_water().await
    }

    async fn persist_high_water(&self, at_least: u64, epoch: Epoch) -> Result<u64, ConsensusError> {
        let call_idx = self.persist_calls.fetch_add(1, Ordering::SeqCst);
        let mut threshold_rx = self.stall_threshold_tx.subscribe();
        let mut was_stalled = false;
        loop {
            let threshold = *threshold_rx.borrow_and_update();
            if call_idx < threshold {
                break;
            }
            was_stalled = true;
            threshold_rx.changed().await.map_err(|err| {
                ConsensusError::TransientDriver(Box::<dyn std::error::Error + Send + Sync>::from(
                    format!("stall sender dropped: {err}"),
                ))
            })?;
        }
        // The documented persist contract lets a driver commit to more
        // than `at_least`. When a call has been stalled, wall-clock time
        // has advanced past the value the server asked for, and serving
        // the in-flight GetTs against the original `at_least` would fail
        // the allocator's post-extension window check. Bump the persist
        // target to "now + 60 s" so the allocator has fresh window once
        // the call returns — this is what a real driver that just
        // recovered from a transient slowdown would do.
        let effective_at_least = if was_stalled {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(at_least);
            core::cmp::max(at_least, now_ms.saturating_add(60_000))
        } else {
            at_least
        };
        self.inner
            .persist_high_water(effective_at_least, epoch)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn persist_is_monotonic() {
        let driver = InMemoryDriver::new();
        assert_eq!(driver.persist_high_water(100, Epoch(1)).await.unwrap(), 100);
        assert_eq!(driver.persist_high_water(50, Epoch(1)).await.unwrap(), 100); // ignored
        assert_eq!(driver.persist_high_water(200, Epoch(1)).await.unwrap(), 200);
        assert_eq!(driver.load_high_water().await.unwrap(), 200);
    }

    #[tokio::test]
    async fn stallable_driver_holds_persist_until_released() {
        use std::time::Duration;
        use tokio::time::timeout;

        let driver = StallableDriver::new();
        // Stall every call from index 1 onward. Call 0 must proceed.
        driver.stall_from(1);

        // Index 0: passes through.
        let first = driver.persist_high_water(50, Epoch(1)).await.unwrap();
        assert_eq!(first, 50);

        // Index 1: blocks. `timeout(short)` must elapse without completion.
        let stalled = driver.persist_high_water(100, Epoch(1));
        assert!(
            timeout(Duration::from_millis(50), stalled).await.is_err(),
            "stalled persist must not complete before release"
        );

        // Release: a fresh call (index 2) must complete promptly.
        driver.release();
        let released = timeout(
            Duration::from_millis(500),
            driver.persist_high_water(200, Epoch(1)),
        )
        .await
        .expect("released persist must complete")
        .unwrap();
        assert_eq!(released, 200);

        // Both stall_from / release left an audit trail in the call counter.
        assert!(driver.persist_call_count() >= 3);
    }

    #[tokio::test]
    async fn leadership_events_observe_transitions() {
        let driver = InMemoryDriver::new();
        let mut events = driver.leadership_events();
        driver.become_leader(Epoch(1));
        // The initial Unknown may or may not be observed depending on stream
        // timing; loop until we see Leader.
        loop {
            match events.next().await.unwrap() {
                LeaderState::Leader { epoch } => {
                    assert_eq!(epoch, Epoch(1));
                    break;
                }
                _ => continue,
            }
        }
    }
}
