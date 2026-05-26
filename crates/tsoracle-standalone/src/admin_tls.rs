//
//  ░▀█▀░█▀▀░█▀█░█▀▄░█▀█░█▀▀░█░░░█▀▀
//  ░░█░░▀▀█░█░█░█▀▄░█▀█░█░░░█░░░█▀▀
//  ░░▀░░▀▀▀░▀▀▀░▀░▀░▀░▀░▀▀▀░▀▀▀░▀▀▀
//
//  tsoracle — Distributed Timestamp Oracle
//  https://www.tsoracle.rs
//
//  Copyright (c) 2026 Prisma Risk
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

//! Builds and eagerly validates the TLS config for the membership-admin
//! gRPC server. Mirrors `peer_tls.rs` but produces only the server side;
//! the `tsoracle admin` CLI assembles its own `ClientTlsConfig` in the bin.

use tonic::transport::{Certificate, Identity, ServerTlsConfig};

use crate::config::AdminTlsConfig;
use crate::error::StandaloneError;

/// Validated TLS material for one node's admin gRPC server.
pub(crate) struct AdminTlsMaterial {
    pub server: ServerTlsConfig,
}

fn read(path: &std::path::Path) -> Result<Vec<u8>, StandaloneError> {
    std::fs::read(path).map_err(|source| StandaloneError::Tls {
        path: path.to_path_buf(),
        source: Box::new(source),
    })
}

/// Read the PEM trio and build the admin mTLS server config, eagerly
/// validating it so a bad cert/key/CA fails at build() rather than on
/// first client connect. `tonic`'s `Identity/Certificate::from_pem` are
/// lazy (just wrap bytes); the cryptographic parse happens when the
/// acceptor is materialized, so we force that here via a Server dry-run.
pub(crate) fn build_admin_tls(cfg: &AdminTlsConfig) -> Result<AdminTlsMaterial, StandaloneError> {
    let cert = read(&cfg.cert)?;
    let key = read(&cfg.key)?;
    let ca = read(&cfg.ca)?;

    let identity = Identity::from_pem(&cert, &key);
    let ca_cert = Certificate::from_pem(&ca);

    let server = ServerTlsConfig::new()
        .identity(identity)
        .client_ca_root(ca_cert);

    // Force the lazy parse before bind/spawn.
    tonic::transport::Server::builder()
        .tls_config(server.clone())
        .map_err(|source| StandaloneError::Tls {
            path: cfg.cert.clone(),
            source: Box::new(source),
        })?;

    Ok(AdminTlsMaterial { server })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AdminTlsConfig;

    fn write_admin_certs(dir: &std::path::Path) -> AdminTlsConfig {
        use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
        let ca_key = KeyPair::generate().unwrap();
        let mut ca_params = CertificateParams::new(vec!["tso-admin-ca".to_string()]).unwrap();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();

        let node_key = KeyPair::generate().unwrap();
        let node_params = CertificateParams::new(vec!["127.0.0.1".to_string()]).unwrap();
        let node_cert = node_params.signed_by(&node_key, &ca_cert, &ca_key).unwrap();

        let cert = dir.join("admin.crt");
        let key = dir.join("admin.key");
        let ca = dir.join("admin-ca.crt");
        std::fs::write(&cert, node_cert.pem()).unwrap();
        std::fs::write(&key, node_key.serialize_pem()).unwrap();
        std::fs::write(&ca, ca_cert.pem()).unwrap();
        AdminTlsConfig { cert, key, ca }
    }

    #[tokio::test]
    async fn valid_trio_builds_admin_server_tls() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = write_admin_certs(dir.path());
        let mat = build_admin_tls(&cfg).expect("valid trio must build");
        let _ = mat.server;
    }

    #[tokio::test]
    async fn missing_file_is_a_tls_error() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = write_admin_certs(dir.path());
        cfg.cert = dir.path().join("does-not-exist.crt");
        let err = match build_admin_tls(&cfg) {
            Ok(_) => panic!("expected a Tls error"),
            Err(e) => e,
        };
        assert!(matches!(err, StandaloneError::Tls { .. }), "got {err:?}");
    }

    #[tokio::test]
    async fn invalid_pem_is_a_tls_error() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = write_admin_certs(dir.path());
        let garbage = dir.path().join("garbage.crt");
        std::fs::write(&garbage, b"not a pem").unwrap();
        cfg.cert = garbage;
        let err = match build_admin_tls(&cfg) {
            Ok(_) => panic!("expected a Tls error"),
            Err(e) => e,
        };
        assert!(matches!(err, StandaloneError::Tls { .. }), "got {err:?}");
    }
}
