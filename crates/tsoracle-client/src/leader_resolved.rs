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

//! Channel pool with leader-cache and NOT_LEADER redirect handling.

use parking_lot::Mutex;
use prost::Message;
use std::collections::HashMap;
use tonic::Status;
use tonic::metadata::MetadataKey;
use tonic::transport::{Channel, Endpoint};
use tsoracle_proto::v1::LeaderHint;
use tsoracle_proto::v1::tso_service_client::TsoServiceClient;

use crate::error::ClientError;

const LEADER_HINT_KEY: &str = "tsoracle-leader-hint-bin";

pub fn decode_leader_hint(status: &Status) -> Option<LeaderHint> {
    let key = MetadataKey::from_bytes(LEADER_HINT_KEY.as_bytes()).ok()?;
    let value = status.metadata().get_bin(key)?;
    let bytes = value.to_bytes().ok()?;
    LeaderHint::decode(bytes.as_ref()).ok()
}

pub struct ChannelPool {
    configured: Vec<String>,
    channels: Mutex<HashMap<String, Channel>>,
    leader: Mutex<Option<String>>,
    connector: Option<std::sync::Arc<crate::transport::ChannelConnector>>,
    /// Set by `ClientBuilder::tls_config`; cleared by `channel_connector`.
    /// Tells the retry loop to drop wire-supplied `http://` leader hints so
    /// a contacted peer cannot downgrade the transport. Has no effect on
    /// operator-supplied endpoints; those use the documented scheme rule
    /// ("explicit beats configured") unchanged.
    tls_required: bool,
}

impl ChannelPool {
    pub fn new(
        endpoints: Vec<String>,
        connector: Option<std::sync::Arc<crate::transport::ChannelConnector>>,
        tls_required: bool,
    ) -> Self {
        ChannelPool {
            configured: endpoints,
            channels: Mutex::new(HashMap::new()),
            leader: Mutex::new(None),
            connector,
            tls_required,
        }
    }

    /// True when the built-in TLS connector is in use. The retry loop uses
    /// this to refuse wire-supplied `http://` leader hints; see
    /// `crate::retry::issue_rpc`.
    pub fn tls_required(&self) -> bool {
        self.tls_required
    }

    pub fn cached_leader(&self) -> Option<String> {
        self.leader.lock().clone()
    }

    pub fn set_leader(&self, endpoint: String) {
        *self.leader.lock() = Some(endpoint);
    }

    pub fn clear_leader(&self) {
        *self.leader.lock() = None;
    }

    /// Returns a tonic client for `endpoint`, opening the channel on first use.
    pub async fn client(&self, endpoint: &str) -> Result<TsoServiceClient<Channel>, ClientError> {
        if let Some(channel) = self.channels.lock().get(endpoint).cloned() {
            return Ok(TsoServiceClient::new(channel));
        }
        let channel = match &self.connector {
            Some(connector) => connector(endpoint).await?,
            None => {
                let uri = crate::transport::normalize_uri(endpoint, false);
                let transport_endpoint: Endpoint = uri
                    .parse()
                    .map_err(|_| ClientError::InvalidEndpoint(endpoint.into()))?;
                transport_endpoint.connect().await?
            }
        };
        self.channels
            .lock()
            .insert(endpoint.to_string(), channel.clone());
        Ok(TsoServiceClient::new(channel))
    }

    pub fn iter_round_robin(&self) -> Vec<String> {
        let leader = self.cached_leader();
        let mut endpoints = Vec::with_capacity(self.configured.len());
        if let Some(leader_endpoint) = &leader {
            endpoints.push(leader_endpoint.clone());
        }
        for endpoint in &self.configured {
            if Some(endpoint) != leader.as_ref() {
                endpoints.push(endpoint.clone());
            }
        }
        endpoints
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn iter_starts_with_cached_leader() {
        let pool = ChannelPool::new(vec!["a:1".into(), "b:1".into(), "c:1".into()], None, false);
        pool.set_leader("b:1".into());
        let order = pool.iter_round_robin();
        assert_eq!(order, vec!["b:1", "a:1", "c:1"]);
    }

    #[test]
    fn iter_without_cache_is_configured_order() {
        let pool = ChannelPool::new(vec!["a:1".into(), "b:1".into(), "c:1".into()], None, false);
        let order = pool.iter_round_robin();
        assert_eq!(order, vec!["a:1", "b:1", "c:1"]);
    }

    #[test]
    fn clear_leader_drops_cached_leader() {
        let pool = ChannelPool::new(vec!["a:1".into(), "b:1".into()], None, false);
        pool.set_leader("b:1".into());
        assert_eq!(pool.cached_leader().as_deref(), Some("b:1"));
        pool.clear_leader();
        // With the cache cleared, round-robin order falls back to the
        // configured order — the cleared leader is not re-prepended.
        assert!(pool.cached_leader().is_none());
        assert_eq!(pool.iter_round_robin(), vec!["a:1", "b:1"]);
    }

    #[tokio::test]
    async fn pool_with_custom_connector_invokes_closure_per_endpoint() {
        let captured = Arc::new(parking_lot::Mutex::new(Vec::<String>::new()));
        let captured_for_closure = captured.clone();
        let connector: Arc<crate::transport::ChannelConnector> = Arc::new(move |endpoint: &str| {
            captured_for_closure.lock().push(endpoint.to_string());
            let endpoint_owned = endpoint.to_string();
            Box::pin(async move { Err(crate::error::ClientError::InvalidEndpoint(endpoint_owned)) })
        });
        let pool = ChannelPool::new(vec!["a:1".into(), "b:1".into()], Some(connector), false);
        let _ = pool.client("a:1").await;
        let _ = pool.client("b:1").await;
        let seen = captured.lock().clone();
        assert_eq!(seen, vec!["a:1".to_string(), "b:1".to_string()]);
    }

    #[tokio::test]
    async fn pool_caches_channel_from_custom_connector() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_for_closure = call_count.clone();
        let connector: Arc<crate::transport::ChannelConnector> =
            Arc::new(move |_endpoint: &str| {
                let n = call_count_for_closure.fetch_add(1, Ordering::SeqCst);
                assert_eq!(n, 0, "connector must only be invoked once per endpoint");
                Box::pin(async {
                    let channel = tonic::transport::Endpoint::from_static("http://127.0.0.1:1")
                        .connect_lazy();
                    Ok(channel)
                })
            });
        let pool = ChannelPool::new(vec!["a:1".into()], Some(connector), false);
        let _ = pool
            .client("a:1")
            .await
            .expect("first client() must succeed");
        let _ = pool
            .client("a:1")
            .await
            .expect("second client() must hit cache");
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn leader_hint_endpoint_goes_through_same_connector() {
        let captured = Arc::new(parking_lot::Mutex::new(Vec::<String>::new()));
        let captured_for_closure = captured.clone();
        let connector: Arc<crate::transport::ChannelConnector> = Arc::new(move |endpoint: &str| {
            captured_for_closure.lock().push(endpoint.to_string());
            Box::pin(async { Err(crate::error::ClientError::InvalidEndpoint("x".into())) })
        });
        let pool = ChannelPool::new(vec!["a:1".into()], Some(connector), false);
        let _ = pool.client("hinted:1").await;
        let seen = captured.lock().clone();
        assert_eq!(seen, vec!["hinted:1".to_string()]);
    }
}
