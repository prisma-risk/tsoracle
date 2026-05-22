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

//! gRPC client for tsoracle.
//!
//! **The client never retains pre-fetched timestamps.** Every timestamp returned
//! to a caller was allocated by the server after that caller's request entered
//! the client driver. RPC efficiency comes from request coalescing (multiple
//! concurrent waiters batch into one outgoing GetTs), not pre-fetching.

// Panic policy (see CONTRIBUTING.md). `cfg_attr(not(test), ...)` skips the lint
// for the lib's own unit tests; integration tests are separate compilation units.
#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used))]

mod driver;
mod error;
mod leader_resolved;
mod response;
mod retry;
mod transport;

pub use error::ClientError;
pub use transport::BoxError;

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
    connector: Option<Arc<crate::transport::ChannelConnector>>,
}

impl ClientBuilder {
    pub fn endpoints(endpoints: Vec<String>) -> Self {
        ClientBuilder {
            endpoints,
            flush_interval: Duration::from_millis(1),
            connector: None,
        }
    }

    pub fn batch_flush_interval(mut self, flush_interval: Duration) -> Self {
        self.flush_interval = flush_interval;
        self
    }

    /// Replace the default plaintext channel construction with a caller-owned
    /// closure. The closure is invoked on first use of each endpoint —
    /// configured endpoints and leader-hint redirects alike. Errors returned
    /// from the closure surface as [`ClientError::Connector`].
    ///
    /// See module docs for the interaction with [`Self::tls_config`]
    /// (last-wins) and the scheme matrix.
    pub fn channel_connector<F, Fut>(mut self, connector: F) -> Self
    where
        F: Fn(&str) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<tonic::transport::Channel, crate::BoxError>>
            + Send
            + 'static,
    {
        let wrapped: Arc<crate::transport::ChannelConnector> = Arc::new(move |endpoint: &str| {
            let fut = connector(endpoint);
            Box::pin(async move { fut.await.map_err(ClientError::Connector) })
        });
        self.connector = Some(wrapped);
        self
    }

    pub async fn build(self) -> Result<Client, ClientError> {
        if self.endpoints.is_empty() {
            return Err(ClientError::NoReachableEndpoints);
        }
        let pool = Arc::new(ChannelPool::new(self.endpoints, self.connector));
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn build_rejects_empty_endpoint_list() {
        // Validation prevents a Client whose `pool` has no endpoints to try;
        // every RPC would fail-fast with `NoReachableEndpoints` and burn no
        // network roundtrips at all, so reject up-front instead.
        match ClientBuilder::endpoints(Vec::new()).build().await {
            Err(ClientError::NoReachableEndpoints) => {}
            Err(other) => panic!("expected NoReachableEndpoints, got {other:?}"),
            Ok(_) => panic!("expected Err, got Ok(Client)"),
        }
    }

    #[tokio::test]
    async fn channel_connector_error_surfaces_as_connector_variant() {
        let builder = ClientBuilder::endpoints(vec!["a:1".into()]).channel_connector(
            |_endpoint: &str| async move {
                Err::<tonic::transport::Channel, crate::BoxError>(
                    std::io::Error::other("boom").into(),
                )
            },
        );
        let client = builder.build().await.expect("build must not fail");
        let result = client.get_ts().await;
        match result {
            Err(ClientError::Connector(inner)) => {
                assert!(inner.to_string().contains("boom"));
            }
            other => panic!("expected ClientError::Connector, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn batch_flush_interval_overrides_default() {
        // The builder's `batch_flush_interval` knob feeds the driver's
        // coalescing window; without a test it could silently revert to the
        // default and no-one would notice from black-box behavior. We
        // confirm the override path by reaching into the builder fields.
        let custom = Duration::from_millis(25);
        let builder = ClientBuilder::endpoints(vec!["http://127.0.0.1:1".into()])
            .batch_flush_interval(custom);
        assert_eq!(builder.flush_interval, custom);
    }
}
