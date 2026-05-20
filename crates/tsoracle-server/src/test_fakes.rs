//! In-memory `ConsensusDriver` for integration tests.

use core::pin::Pin;
use futures::{Stream, StreamExt};
use parking_lot::Mutex;
use std::sync::Arc;
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
