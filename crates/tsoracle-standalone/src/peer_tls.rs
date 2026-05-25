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

//! Builds and eagerly validates the TLS configs for the peer transport.

use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity, ServerTlsConfig};

use crate::config::PeerTlsConfig;
use crate::error::StandaloneError;

/// Validated TLS material for one node's peer transport. The same node identity
/// is used both as the peer-server identity and (for mTLS) the peer-client
/// identity when dialing other nodes.
pub(crate) struct PeerTlsMaterial {
    pub server: ServerTlsConfig,
    pub client: ClientTlsConfig,
}

fn read(path: &std::path::Path) -> Result<Vec<u8>, StandaloneError> {
    std::fs::read(path).map_err(|source| StandaloneError::Tls {
        path: path.to_path_buf(),
        source: Box::new(source),
    })
}

/// Read the PEM trio and build the peer mTLS server + client configs, eagerly
/// validating BOTH so a bad cert/key/CA fails at build() rather than on first
/// dial. `tonic`'s `Identity/Certificate::from_pem` are lazy (just wrap bytes);
/// the cryptographic parse happens when the acceptor/connector is materialized,
/// so we force that here via `Server`/`Endpoint` dry-runs (no socket touched).
pub(crate) fn build_peer_tls(cfg: &PeerTlsConfig) -> Result<PeerTlsMaterial, StandaloneError> {
    let cert = read(&cfg.cert)?;
    let key = read(&cfg.key)?;
    let ca = read(&cfg.ca)?;

    let identity = Identity::from_pem(&cert, &key);
    let ca_cert = Certificate::from_pem(&ca);

    let server = ServerTlsConfig::new()
        .identity(identity.clone())
        .client_ca_root(ca_cert.clone());
    let client = ClientTlsConfig::new()
        .ca_certificate(ca_cert)
        .identity(identity);

    // Eager validation (force the lazy parse before bind/spawn):
    // server side — building the acceptor parses the identity + client CA.
    tonic::transport::Server::builder()
        .tls_config(server.clone())
        .map_err(|source| StandaloneError::Tls {
            path: cfg.cert.clone(),
            source: Box::new(source),
        })?;
    // client side — building the connector parses the CA + client identity.
    Endpoint::from_static("https://127.0.0.1:65535")
        .tls_config(client.clone())
        .map_err(|source| StandaloneError::Tls {
            path: cfg.cert.clone(),
            source: Box::new(source),
        })?;

    Ok(PeerTlsMaterial { server, client })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PeerTlsConfig;

    // rcgen 0.13 API (mirrors examples/tls-mtls/src/certs.rs): KeyPair::generate,
    // CertificateParams::{self_signed, signed_by}, cert.pem(), key.serialize_pem().
    fn write_node_certs(dir: &std::path::Path) -> PeerTlsConfig {
        use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
        let ca_key = KeyPair::generate().unwrap();
        let mut ca_params = CertificateParams::new(vec!["tso-ca".to_string()]).unwrap();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();

        let node_key = KeyPair::generate().unwrap();
        let node_params = CertificateParams::new(vec!["127.0.0.1".to_string()]).unwrap();
        let node_cert = node_params.signed_by(&node_key, &ca_cert, &ca_key).unwrap();

        let cert = dir.join("node.crt");
        let key = dir.join("node.key");
        let ca = dir.join("ca.crt");
        std::fs::write(&cert, node_cert.pem()).unwrap();
        std::fs::write(&key, node_key.serialize_pem()).unwrap();
        std::fs::write(&ca, ca_cert.pem()).unwrap();
        PeerTlsConfig { cert, key, ca }
    }

    #[tokio::test]
    async fn valid_trio_builds_both_configs() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = write_node_certs(dir.path());
        let mat = build_peer_tls(&cfg).expect("valid trio must build");
        // Confirm the material fields are populated (used by Tasks 3–6).
        let _ = mat.server;
        let _ = mat.client;
    }

    #[tokio::test]
    async fn missing_file_is_a_tls_error() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = write_node_certs(dir.path());
        cfg.cert = dir.path().join("does-not-exist.crt");
        // PeerTlsMaterial is not Debug, so don't use unwrap_err().
        let err = match build_peer_tls(&cfg) {
            Ok(_) => panic!("expected a Tls error"),
            Err(e) => e,
        };
        assert!(matches!(err, StandaloneError::Tls { .. }), "got {err:?}");
    }

    #[tokio::test]
    async fn invalid_pem_is_a_tls_error() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = write_node_certs(dir.path());
        let garbage = dir.path().join("garbage.crt");
        std::fs::write(&garbage, b"not a pem").unwrap();
        cfg.cert = garbage;
        let err = match build_peer_tls(&cfg) {
            Ok(_) => panic!("expected a Tls error"),
            Err(e) => e,
        };
        assert!(matches!(err, StandaloneError::Tls { .. }), "got {err:?}");
    }
}
