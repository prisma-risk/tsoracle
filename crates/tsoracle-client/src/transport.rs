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

/// Apply the scheme rule to an endpoint string.
///
/// - Explicit `http://...` and `https://...` are returned verbatim.
/// - Bare `host:port` becomes `http://host:port` when `tls` is false,
///   `https://host:port` when `tls` is true.
///
/// "Explicit beats configured" is universal: callers wanting plaintext on a
/// per-endpoint basis even when a TLS transport is configured can pass
/// `http://host:port` and the rule returns it untouched.
#[allow(dead_code)]
pub(crate) fn normalize_uri(endpoint: &str, tls: bool) -> String {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else if tls {
        format!("https://{endpoint}")
    } else {
        format!("http://{endpoint}")
    }
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
