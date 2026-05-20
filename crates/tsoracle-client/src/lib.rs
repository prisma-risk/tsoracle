//! gRPC client for tsoracle.
//!
//! **The client never retains pre-fetched timestamps.** Every timestamp returned
//! to a caller was allocated by the server after that caller's request entered
//! the client driver. RPC efficiency comes from request coalescing (multiple
//! concurrent waiters batch into one outgoing GetTs), not pre-fetching.

mod driver;
mod error;
mod leader_resolved;
mod response;
mod retry;

pub use error::ClientError;

use std::sync::Arc;
use std::time::Duration;
use tsoracle_core::{LOGICAL_MAX, Timestamp};

/// The server's per-call cap on requested timestamps, fixed by the 18-bit
/// logical width. Callers asking for more than this can't be served by any
/// single RPC; the client rejects them up-front rather than burning a queue
/// slot and round-trip to learn the same thing from the server.
pub(crate) const MAX_TIMESTAMPS_PER_RPC: u32 = LOGICAL_MAX + 1;

use crate::leader_resolved::ChannelPool;

pub struct ClientBuilder {
    endpoints: Vec<String>,
    flush_interval: Duration,
}

impl ClientBuilder {
    pub fn endpoints(endpoints: Vec<String>) -> Self {
        ClientBuilder {
            endpoints,
            flush_interval: Duration::from_millis(1),
        }
    }

    pub fn batch_flush_interval(mut self, d: Duration) -> Self {
        self.flush_interval = d;
        self
    }

    pub async fn build(self) -> Result<Client, ClientError> {
        if self.endpoints.is_empty() {
            return Err(ClientError::NoReachableEndpoints);
        }
        let pool = Arc::new(ChannelPool::new(self.endpoints));
        let pool_for_rpc = pool.clone();
        let driver = driver::Driver::spawn(
            move |count| {
                let pool = pool_for_rpc.clone();
                Box::pin(async move { retry::issue_rpc(&pool, count).await })
            },
            self.flush_interval,
        );
        Ok(Client { pool, driver })
    }
}

pub struct Client {
    #[allow(dead_code)]
    pool: Arc<ChannelPool>,
    driver: driver::Driver,
}

impl Client {
    pub async fn connect(endpoints: Vec<String>) -> Result<Self, ClientError> {
        ClientBuilder::endpoints(endpoints).build().await
    }

    pub async fn get_ts(&self) -> Result<Timestamp, ClientError> {
        Ok(self.driver.request(1).await?[0])
    }

    pub async fn get_ts_batch(&self, count: u32) -> Result<Vec<Timestamp>, ClientError> {
        if count == 0 || count > MAX_TIMESTAMPS_PER_RPC {
            return Err(ClientError::InvalidCount(count));
        }
        self.driver.request(count).await
    }
}
