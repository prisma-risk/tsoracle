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

use std::future::Future;
use std::pin::Pin;
use tonic::transport::Channel;

use crate::error::ClientError;

/// Boxed error returned by user-supplied connector closures.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

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
/// Explicit `http://...` endpoints are honored as plaintext even when this
/// connector is in use ("explicit beats configured"). The TLS config is
/// attached only when the resolved URI uses the `https` scheme.
#[cfg(any(feature = "tls-rustls", feature = "tls-native"))]
pub(crate) fn tls_connector(
    cfg: tonic::transport::ClientTlsConfig,
) -> std::sync::Arc<ChannelConnector> {
    use tonic::transport::Endpoint;
    std::sync::Arc::new(move |endpoint: &str| {
        let uri = normalize_uri(endpoint, true);
        let cfg = cfg.clone();
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
