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

//! Mint a test CA + a single fan-SAN leaf cert covering every replica's pod
//! FQDN and the client/headless Service DNS. Used by the kube-e2e TLS cell
//! (issue #483) to populate the chart's `tls.secretName` Secret before
//! `helm install`.
//!
//! ## Why one shared cert
//!
//! The chart from #452 mounts a single Secret cluster-wide at `/etc/tsoracle/tls`
//! and PR #445's peer authorization is CA-based: any cert chaining to the
//! configured peer CA is accepted. So every pod presenting one shared leaf
//! that has SANs for *all* pod FQDNs is sufficient — when pod A dials pod B,
//! SNI on the dial is B's FQDN, B's cert has B's FQDN in its SAN list, the
//! handshake succeeds.
//!
//! Per-pod identity (Vault/cert-manager-style) is out of scope for the kind
//! e2e fixture; this binary exists only to give the lane real TLS material
//! against which the chart's secure-by-default render guard's happy path can
//! be exercised under realistic kube DNS rotation.
//!
//! ## Output
//!
//! Writes three files into `--out`, named so they line up with the chart's
//! expected Secret keys (tls.crt / tls.key / ca.crt — see
//! `deploy/entrypoint.sh`):
//!
//!   - `ca.crt`  — CA certificate (trust anchor mounted on every pod)
//!   - `tls.crt` — leaf cert with SANs covering every pod FQDN + service DNS
//!   - `tls.key` — leaf private key
//!
//! The CA private key is intentionally discarded once the leaf is signed —
//! this is a one-shot fixture mint with no follow-up signing.

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};

#[derive(Parser, Debug)]
#[command(
    name = "gen-certs",
    about = "Mint a test CA + fan-SAN leaf cert for the kube-e2e TLS cell (issue #483)"
)]
struct Cli {
    /// Output directory; created if it does not exist.
    #[arg(long)]
    out: PathBuf,

    /// Helm release name. Per-pod FQDNs are
    /// `<release>-<i>.<release>-peer.<namespace>.svc.cluster.local`.
    #[arg(long, default_value = "tsoracle")]
    release: String,

    /// Kubernetes namespace into which the chart will be installed.
    #[arg(long, default_value = "default")]
    namespace: String,

    /// Number of replicas in the StatefulSet (ordinals 0..N go into the SAN list).
    #[arg(long, default_value_t = 3)]
    replicas: u32,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let sans = build_sans(&cli.release, &cli.namespace, cli.replicas);

    let ca_name = format!("tsoracle-e2e-{}-ca", cli.release);
    let ca_key = KeyPair::generate().context("CA keypair")?;
    let mut ca_params = CertificateParams::new(vec![ca_name.clone()]).context("CA params")?;
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    ca_params
        .distinguished_name
        .push(DnType::CommonName, ca_name);
    let ca_cert = ca_params.self_signed(&ca_key).context("CA self-sign")?;

    let leaf_key = KeyPair::generate().context("leaf keypair")?;
    let mut leaf_params = CertificateParams::new(sans).context("leaf params")?;
    leaf_params.distinguished_name.push(
        DnType::CommonName,
        format!("{}.{}", cli.release, cli.namespace),
    );
    // The shared leaf cert serves three roles simultaneously: peer-as-server
    // when a sibling dials in, peer-as-client when this pod dials a sibling,
    // and the client-API server cert that the driver Job validates. Hence
    // both ServerAuth and ClientAuth EKUs.
    leaf_params.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ServerAuth,
        ExtendedKeyUsagePurpose::ClientAuth,
    ];
    let leaf_cert = leaf_params
        .signed_by(&leaf_key, &ca_cert, &ca_key)
        .context("leaf sign")?;

    std::fs::create_dir_all(&cli.out)
        .with_context(|| format!("create_dir_all({})", cli.out.display()))?;
    let ca_path = cli.out.join("ca.crt");
    let cert_path = cli.out.join("tls.crt");
    let key_path = cli.out.join("tls.key");
    std::fs::write(&ca_path, ca_cert.pem())
        .with_context(|| format!("write {}", ca_path.display()))?;
    std::fs::write(&cert_path, leaf_cert.pem())
        .with_context(|| format!("write {}", cert_path.display()))?;
    std::fs::write(&key_path, leaf_key.serialize_pem())
        .with_context(|| format!("write {}", key_path.display()))?;

    println!(
        "gen-certs: wrote ca.crt + tls.crt + tls.key to {}",
        cli.out.display()
    );
    Ok(())
}

/// SANs the leaf cert must cover for the chart-rendered topology.
///
/// The chart exposes consensus + client + DNS surfaces:
///   * peer dial inside the cluster: `<release>-<i>.<release>-peer.<ns>.svc.cluster.local`
///   * headless peer Service DNS:    `<release>-peer.<ns>.svc.cluster.local`
///   * client Service VIP DNS:       `<release>.<ns>.svc.cluster.local`
///
/// `localhost`/`127.0.0.1` are added so loopback probes (e.g. the chart's
/// admin port bound at `127.0.0.1`) can use the same fixture if a future
/// operator adds admin TLS material to the entrypoint.
fn build_sans(release: &str, namespace: &str, replicas: u32) -> Vec<String> {
    let mut sans = Vec::with_capacity((replicas as usize) + 4);
    for i in 0..replicas {
        sans.push(format!(
            "{release}-{i}.{release}-peer.{namespace}.svc.cluster.local"
        ));
    }
    sans.push(format!("{release}-peer.{namespace}.svc.cluster.local"));
    sans.push(format!("{release}.{namespace}.svc.cluster.local"));
    sans.push("localhost".to_string());
    sans.push("127.0.0.1".to_string());
    sans
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sans_cover_each_pod_fqdn_and_services() {
        let sans = build_sans("tsoracle-tls", "e2e-tls", 3);
        for i in 0..3 {
            let want = format!("tsoracle-tls-{i}.tsoracle-tls-peer.e2e-tls.svc.cluster.local");
            assert!(sans.contains(&want), "missing pod FQDN SAN {want}");
        }
        assert!(sans.contains(&"tsoracle-tls-peer.e2e-tls.svc.cluster.local".to_string()));
        assert!(sans.contains(&"tsoracle-tls.e2e-tls.svc.cluster.local".to_string()));
        assert!(sans.contains(&"localhost".to_string()));
        assert!(sans.contains(&"127.0.0.1".to_string()));
    }

    #[test]
    fn sans_scale_with_replicas() {
        let sans = build_sans("foo", "bar", 5);
        for i in 0..5 {
            let want = format!("foo-{i}.foo-peer.bar.svc.cluster.local");
            assert!(sans.contains(&want), "missing {want}");
        }
        // No phantom ordinals beyond `replicas`.
        let phantom = "foo-5.foo-peer.bar.svc.cluster.local".to_string();
        assert!(!sans.contains(&phantom), "unexpected SAN {phantom}");
    }
}
