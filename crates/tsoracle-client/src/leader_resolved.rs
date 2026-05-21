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
}

impl ChannelPool {
    pub fn new(endpoints: Vec<String>) -> Self {
        ChannelPool {
            configured: endpoints,
            channels: Mutex::new(HashMap::new()),
            leader: Mutex::new(None),
        }
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
        let uri = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
            endpoint.to_string()
        } else {
            format!("http://{endpoint}")
        };
        let transport_endpoint: Endpoint = uri
            .parse()
            .map_err(|_| ClientError::InvalidEndpoint(endpoint.into()))?;
        let channel = transport_endpoint.connect().await?;
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

    #[test]
    fn iter_starts_with_cached_leader() {
        let pool = ChannelPool::new(vec!["a:1".into(), "b:1".into(), "c:1".into()]);
        pool.set_leader("b:1".into());
        let order = pool.iter_round_robin();
        assert_eq!(order, vec!["b:1", "a:1", "c:1"]);
    }

    #[test]
    fn iter_without_cache_is_configured_order() {
        let pool = ChannelPool::new(vec!["a:1".into(), "b:1".into(), "c:1".into()]);
        let order = pool.iter_round_robin();
        assert_eq!(order, vec!["a:1", "b:1", "c:1"]);
    }

    #[test]
    fn clear_leader_drops_cached_leader() {
        let pool = ChannelPool::new(vec!["a:1".into(), "b:1".into()]);
        pool.set_leader("b:1".into());
        assert_eq!(pool.cached_leader().as_deref(), Some("b:1"));
        pool.clear_leader();
        assert!(pool.cached_leader().is_none());
        // With the cache cleared, round-robin order falls back to the
        // configured order — the cleared leader is not re-prepended.
        assert_eq!(pool.iter_round_robin(), vec!["a:1", "b:1"]);
    }
}
