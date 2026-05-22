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

//! Transport plumbing for the channel pool.
//!
//! `normalize_uri` enforces the scheme rule: bare `host:port` becomes
//! `http://host:port` or `https://host:port` depending on whether a TLS
//! transport is configured; explicit schemes are always preserved.
//!
//! "Explicit beats configured" governs *operator-supplied* endpoint
//! strings — those passed to `ClientBuilder::endpoints`. The
//! `crate::retry::issue_rpc` loop applies a tighter rule to *wire-supplied*
//! `tsoracle-leader-hint-bin` trailers under `tls_config`: explicit
//! `http://...` hints are dropped so a contacted peer cannot downgrade the
//! transport. See `crate::retry::rejects_plaintext_hint`.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use tonic::transport::Channel;

use crate::RetryPolicy;
use crate::error::ClientError;

/// Boxed error returned by user-supplied connector closures.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// HTTP/2 keepalive ping interval. Hardcoded rather than exposed on
/// [`RetryPolicy`] because no realistic deployment needs to tune it
/// independently of the per-attempt deadline — 30 s is well below the
/// idle-connection cull window of common L4 load balancers and NATs
/// (AWS NLB: 350 s, AWS ALB: 60 s default, GCP: 600 s, conntrack:
/// 432 000 s but earlier eviction under pressure). Callers needing a
/// different value can use [`crate::ClientBuilder::channel_connector`]
/// and build the `Endpoint` themselves.
pub(crate) const HTTP2_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);

/// Apply the four `Endpoint` knobs the [`RetryPolicy`] dictates:
/// `connect_timeout`, `timeout` (both seeded from
/// `per_attempt_deadline`), `keep_alive_while_idle(true)`, and
/// `http2_keep_alive_interval`. Called from the built-in default and
/// built-in TLS paths so a blackholed peer surfaces a tonic transport
/// error within `per_attempt_deadline` instead of parking on the
/// OS-default TCP timeout. User-supplied
/// [`crate::ClientBuilder::channel_connector`] closures own their own
/// `Endpoint` config and do not go through this helper.
pub(crate) fn apply_endpoint_config(
    endpoint: tonic::transport::Endpoint,
    policy: &RetryPolicy,
) -> tonic::transport::Endpoint {
    endpoint
        .connect_timeout(policy.per_attempt_deadline)
        .timeout(policy.per_attempt_deadline)
        .keep_alive_while_idle(true)
        .http2_keep_alive_interval(HTTP2_KEEPALIVE_INTERVAL)
}

/// Stored channel-construction strategy, shared by the built-in TLS path
/// and any user-supplied closure. Errors are normalized to `ClientError`
/// before storage so the pool's execution path is a single `await?`.
pub(crate) type ChannelConnector = dyn Fn(&str) -> Pin<Box<dyn Future<Output = Result<Channel, ClientError>> + Send>>
    + Send
    + Sync;

/// Apply the scheme rule to an endpoint string.
///
/// - Explicit `http://...` and `https://...` are returned verbatim.
/// - Bare `host:port` becomes `http://host:port` when `tls` is false,
///   `https://host:port` when `tls` is true.
///
/// "Explicit beats configured" is universal: callers wanting plaintext on a
/// per-endpoint basis even when a TLS transport is configured can pass
/// `http://host:port` and the rule returns it untouched.
pub(crate) fn normalize_uri(endpoint: &str, tls: bool) -> String {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else if tls {
        format!("https://{endpoint}")
    } else {
        format!("http://{endpoint}")
    }
}

/// Construct the built-in TLS-aware channel connector.
///
/// Bare endpoints are rewritten to `https://` via [`normalize_uri`].
/// Explicit `http://...` endpoints supplied via
/// `ClientBuilder::endpoints` are honored as plaintext even when this
/// connector is in use ("explicit beats configured"). The TLS config is
/// attached only when the resolved URI uses the `https` scheme.
///
/// Explicit `http://...` endpoints arriving via the
/// `tsoracle-leader-hint-bin` trailer are filtered out one layer up in
/// `crate::retry::issue_rpc` and never reach this connector — they would
/// otherwise dial plaintext here, downgrading a TLS-configured client.
#[cfg(any(feature = "tls-rustls", feature = "tls-native"))]
pub(crate) fn tls_connector(
    cfg: tonic::transport::ClientTlsConfig,
    policy: RetryPolicy,
) -> std::sync::Arc<ChannelConnector> {
    use tonic::transport::Endpoint;
    std::sync::Arc::new(move |endpoint: &str| {
        let uri = normalize_uri(endpoint, true);
        let cfg = cfg.clone();
        let policy = policy.clone();
        let endpoint_owned = endpoint.to_string();
        Box::pin(async move {
            let ep: Endpoint = uri
                .parse()
                .map_err(|_| ClientError::InvalidEndpoint(endpoint_owned))?;
            let ep = if ep.uri().scheme_str() == Some("https") {
                ep.tls_config(cfg).map_err(ClientError::from)?
            } else {
                ep
            };
            let ep = apply_endpoint_config(ep, &policy);
            let channel = ep.connect().await.map_err(ClientError::from)?;
            Ok(channel)
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bare_endpoint_without_tls_yields_http() {
        assert_eq!(normalize_uri("host:1", false), "http://host:1");
    }

    #[test]
    fn bare_endpoint_with_tls_yields_https() {
        assert_eq!(normalize_uri("host:1", true), "https://host:1");
    }

    #[test]
    fn explicit_http_preserved_under_tls() {
        assert_eq!(normalize_uri("http://host:1", true), "http://host:1");
    }

    #[test]
    fn explicit_https_preserved_without_tls() {
        assert_eq!(normalize_uri("https://host:1", false), "https://host:1");
    }
}
