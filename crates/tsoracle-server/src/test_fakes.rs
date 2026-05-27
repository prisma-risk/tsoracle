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

//! In-memory `ConsensusDriver` for integration tests.

use core::pin::Pin;
use futures::{Stream, StreamExt};
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{Notify, watch};
use tokio_stream::wrappers::WatchStream;
use tsoracle_consensus::{ConsensusDriver, ConsensusError, LeaderState};
use tsoracle_core::{Epoch, PeerEndpoint};

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

    pub fn become_follower(&self, hint: Option<PeerEndpoint>) {
        let _ = self.tx.send(LeaderState::Follower {
            leader_endpoint: hint,
            leader_epoch: None,
        });
    }

    /// Emit a `Follower` transition carrying both the leader endpoint hint and
    /// the leader's epoch, so tests can exercise epoch propagation.
    pub fn become_follower_with_epoch(&self, hint: Option<PeerEndpoint>, epoch: Option<Epoch>) {
        let _ = self.tx.send(LeaderState::Follower {
            leader_endpoint: hint,
            leader_epoch: epoch,
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
    stall_threshold: Arc<AtomicU64>,
    threshold_wake: Arc<Notify>,
    persist_calls: Arc<AtomicU64>,
}

impl Default for StallableDriver {
    fn default() -> Self {
        StallableDriver {
            inner: InMemoryDriver::new(),
            stall_threshold: Arc::new(AtomicU64::new(u64::MAX)),
            threshold_wake: Arc::new(Notify::new()),
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

    /// Cause every `persist_high_water` invocation whose call index is at
    /// or above `threshold` to block until [`Self::release`] is called.
    /// Calls with index `< threshold` (whether already in flight or yet to
    /// arrive) are unaffected.
    pub fn stall_from(&self, threshold: u64) {
        self.stall_threshold.store(threshold, Ordering::SeqCst);
    }

    /// Unblock every pending stalled persist call and disable stalling for
    /// future calls.
    pub fn release(&self) {
        self.stall_threshold.store(u64::MAX, Ordering::SeqCst);
        self.threshold_wake.notify_waiters();
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
        let mut was_stalled = false;
        loop {
            // Race-free `Notify` pattern: register the wake future *before*
            // checking the threshold. If `release` fires between the check
            // and the await, the future is already armed and resolves
            // immediately at `.await` instead of missing the wake.
            let notified = self.threshold_wake.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if call_idx < self.stall_threshold.load(Ordering::SeqCst) {
                break;
            }
            was_stalled = true;
            notified.as_mut().await;
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

/// Kinds of [`ConsensusError`] a [`FaultyDriver`] injects, each mapped to the
/// variant the real openraft/paxos drivers produce for the matching failure.
#[derive(Clone, Copy, Debug)]
pub enum FaultKind {
    /// `ConsensusError::NotLeader` — what drivers return when a
    /// `ForwardToLeader` is observed (leadership moved during the fence).
    NotLeader,
    /// `ConsensusError::TransientDriver` — the explicitly retryable class
    /// (momentary quorum loss or transport flap during a failover).
    Transient,
    /// `ConsensusError::PermanentDriver` — the must-not-retry class
    /// (corruption, gone storage); the fence treats it as fatal.
    Permanent,
}

impl FaultKind {
    fn into_error(self) -> ConsensusError {
        match self {
            FaultKind::NotLeader => ConsensusError::NotLeader { observed: None },
            FaultKind::Transient => ConsensusError::TransientDriver(Box::new(
                std::io::Error::other("injected transient fault"),
            )),
            FaultKind::Permanent => ConsensusError::PermanentDriver(Box::new(
                std::io::Error::other("injected permanent fault"),
            )),
        }
    }
}

/// Wraps an [`InMemoryDriver`] with a queue of faults applied to upcoming
/// `persist_high_water` calls, for tests that exercise the fence's
/// transient-vs-permanent error handling.
///
/// Each `persist_high_water` call pops one fault off the front of the queue
/// and returns it; once the queue is empty, calls fall through to the inner
/// driver and succeed. Leadership events and the stored high-water are
/// delegated to the inner [`InMemoryDriver`].
#[derive(Clone)]
pub struct FaultyDriver {
    inner: InMemoryDriver,
    persist_faults: Arc<Mutex<VecDeque<FaultKind>>>,
    persist_calls: Arc<AtomicU64>,
}

impl Default for FaultyDriver {
    fn default() -> Self {
        FaultyDriver {
            inner: InMemoryDriver::new(),
            persist_faults: Arc::new(Mutex::new(VecDeque::new())),
            persist_calls: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl FaultyDriver {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn become_leader(&self, epoch: Epoch) {
        self.inner.become_leader(epoch);
    }

    pub fn become_follower(&self, hint: Option<PeerEndpoint>) {
        self.inner.become_follower(hint);
    }

    pub fn current_high_water(&self) -> u64 {
        self.inner.current_high_water()
    }

    /// Number of `persist_high_water` calls that have started so far,
    /// including any that returned an injected fault.
    pub fn persist_call_count(&self) -> u64 {
        self.persist_calls.load(Ordering::SeqCst)
    }

    /// Enqueue `count` faults of `kind`. The next `count` `persist_high_water`
    /// calls return that error in order; later calls succeed.
    pub fn fail_next_persists(&self, count: usize, kind: FaultKind) {
        let mut queue = self.persist_faults.lock();
        for _ in 0..count {
            queue.push_back(kind);
        }
    }
}

#[async_trait::async_trait]
impl ConsensusDriver for FaultyDriver {
    fn leadership_events(&self) -> Pin<Box<dyn Stream<Item = LeaderState> + Send>> {
        self.inner.leadership_events()
    }

    async fn load_high_water(&self) -> Result<u64, ConsensusError> {
        self.inner.load_high_water().await
    }

    async fn persist_high_water(&self, at_least: u64, epoch: Epoch) -> Result<u64, ConsensusError> {
        self.persist_calls.fetch_add(1, Ordering::SeqCst);
        if let Some(kind) = self.persist_faults.lock().pop_front() {
            return Err(kind.into_error());
        }
        self.inner.persist_high_water(at_least, epoch).await
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
