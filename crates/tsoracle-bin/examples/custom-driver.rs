//! Sketch of a `ConsensusDriver` impl against an external KV store.
//!
//! This is illustrative: shows the trait shape so users planning an openraft,
//! raft-rs, or etcd integration can see what they need to implement. It does
//! NOT provide HA — `leadership_events` returns a single Leader event, the
//! "KV" is an in-memory `Mutex<u64>`, and there is no replication.
//!
//! Run: `cargo run --example custom-driver -p tsoracle`

use core::pin::Pin;
use futures::{Stream, StreamExt};
use parking_lot::Mutex;
use std::sync::Arc;
use tokio::sync::watch;
use tokio_stream::wrappers::WatchStream;
use tsoracle_consensus::{ConsensusDriver, ConsensusError, LeaderState};
use tsoracle_core::Epoch;
use tsoracle_server::Server;

struct InMemoryKv {
    state: Arc<Mutex<u64>>,
    rx: watch::Receiver<LeaderState>,
    _tx: watch::Sender<LeaderState>,
}

impl InMemoryKv {
    fn new() -> Arc<Self> {
        let (tx, rx) = watch::channel(LeaderState::Leader { epoch: Epoch::ZERO });
        Arc::new(InMemoryKv {
            state: Arc::new(Mutex::new(0)),
            rx,
            _tx: tx,
        })
    }
}

#[async_trait::async_trait]
impl ConsensusDriver for InMemoryKv {
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let driver = InMemoryKv::new();
    let server = Server::builder().consensus_driver(driver).build()?;
    let addr = "127.0.0.1:50551".parse().unwrap();
    println!("Custom-driver tsoracle on http://{addr}");
    server.serve(addr).await?;
    Ok(())
}
