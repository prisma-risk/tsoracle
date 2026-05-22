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

//! End-to-end TLS and mTLS tests. Each test boots a real
//! `tsoracle_server::Server` with a self-signed CA minted in-process,
//! then exercises the client side `tls_config` / `channel_connector`
//! plumbing against it.

#![cfg(any(feature = "tls-rustls", feature = "tls-native"))]

use std::sync::Arc;
use std::time::Duration;
use tonic::transport::{Certificate, ClientTlsConfig, Identity, ServerTlsConfig};
use tsoracle_core::Epoch;
use tsoracle_server::Server;
use tsoracle_server::test_fakes::InMemoryDriver;
use tsoracle_server::test_support::{
    boot_server, wait_for_grpc_handshake, wait_for_grpc_handshake_tls, wait_until_serving,
};

struct CertBundle {
    ca_pem: String,
    server_cert_pem: String,
    server_key_pem: String,
    client_cert_pem: String,
    client_key_pem: String,
}

fn mint_certs() -> CertBundle {
    use rcgen::{
        BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair,
        KeyUsagePurpose,
    };

    let ca_key = KeyPair::generate().expect("ca keypair");
    let mut ca_params = CertificateParams::new(vec!["tsoracle-test-ca".into()]).expect("ca params");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let ca_cert = ca_params.self_signed(&ca_key).expect("ca self-sign");

    let server_key = KeyPair::generate().expect("server keypair");
    let mut server_params = CertificateParams::new(vec!["localhost".into(), "127.0.0.1".into()])
        .expect("server params");
    server_params
        .distinguished_name
        .push(DnType::CommonName, "localhost");
    server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let server_cert = server_params
        .signed_by(&server_key, &ca_cert, &ca_key)
        .expect("server sign");

    let client_key = KeyPair::generate().expect("client keypair");
    let mut client_params = CertificateParams::new(Vec::<String>::new()).expect("client params");
    client_params
        .distinguished_name
        .push(DnType::CommonName, "tsoracle-test-client");
    client_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    let client_cert = client_params
        .signed_by(&client_key, &ca_cert, &ca_key)
        .expect("client sign");

    CertBundle {
        ca_pem: ca_cert.pem(),
        server_cert_pem: server_cert.pem(),
        server_key_pem: server_key.serialize_pem(),
        client_cert_pem: client_cert.pem(),
        client_key_pem: client_key.serialize_pem(),
    }
}

fn server_tls_config(bundle: &CertBundle) -> ServerTlsConfig {
    let identity = Identity::from_pem(&bundle.server_cert_pem, &bundle.server_key_pem);
    ServerTlsConfig::new().identity(identity)
}

fn mtls_server_tls_config(bundle: &CertBundle) -> ServerTlsConfig {
    let identity = Identity::from_pem(&bundle.server_cert_pem, &bundle.server_key_pem);
    let ca = Certificate::from_pem(&bundle.ca_pem);
    ServerTlsConfig::new().identity(identity).client_ca_root(ca)
}

fn client_tls_config_with_ca(bundle: &CertBundle) -> ClientTlsConfig {
    ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(&bundle.ca_pem))
        .domain_name("localhost")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_handshake_succeeds_with_self_signed_ca() {
    let bundle = mint_certs();
    let driver = Arc::new(InMemoryDriver::new());
    driver.become_leader(Epoch(1));

    let server = Server::builder()
        .consensus_driver(driver.clone())
        .tls_config(server_tls_config(&bundle))
        .build()
        .expect("server build");

    let mut booted = boot_server(server).await;
    wait_until_serving(&mut booted.state_rx).await;
    wait_for_grpc_handshake_tls(
        booted.addr,
        client_tls_config_with_ca(&bundle),
        Duration::from_secs(5),
    )
    .await
    .expect("tonic must accept TLS handshake");

    let client = tsoracle_client::ClientBuilder::endpoints(vec![format!(
        "127.0.0.1:{}",
        booted.addr.port()
    )])
    .tls_config(client_tls_config_with_ca(&bundle))
    .build()
    .await
    .expect("client connect");

    let ts = client.get_ts().await.expect("get_ts");
    assert!(ts.physical_ms() > 1_700_000_000_000);

    drop(client);
    booted.shutdown().await.expect("server exit");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_handshake_fails_without_ca() {
    let bundle = mint_certs();
    let driver = Arc::new(InMemoryDriver::new());
    driver.become_leader(Epoch(1));
    let server = Server::builder()
        .consensus_driver(driver.clone())
        .tls_config(server_tls_config(&bundle))
        .build()
        .expect("server build");
    let mut booted = boot_server(server).await;
    wait_until_serving(&mut booted.state_rx).await;
    let result = tsoracle_client::ClientBuilder::endpoints(vec![format!(
        "127.0.0.1:{}",
        booted.addr.port()
    )])
    .tls_config(ClientTlsConfig::new().domain_name("localhost"))
    .build()
    .await
    .expect("build does not connect")
    .get_ts()
    .await;
    match result {
        Err(tsoracle_client::ClientError::Transport(_))
        | Err(tsoracle_client::ClientError::NoReachableEndpoints) => {}
        other => panic!("expected Transport/NoReachableEndpoints, got {other:?}"),
    }
    booted.shutdown().await.expect("server exit");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn channel_connector_parity_with_tls_config() {
    let bundle = mint_certs();
    let driver = Arc::new(InMemoryDriver::new());
    driver.become_leader(Epoch(1));
    let server = Server::builder()
        .consensus_driver(driver.clone())
        .tls_config(server_tls_config(&bundle))
        .build()
        .expect("server build");
    let mut booted = boot_server(server).await;
    wait_until_serving(&mut booted.state_rx).await;
    wait_for_grpc_handshake_tls(
        booted.addr,
        client_tls_config_with_ca(&bundle),
        Duration::from_secs(5),
    )
    .await
    .expect("tls ready");

    let tls = client_tls_config_with_ca(&bundle);
    let client = tsoracle_client::ClientBuilder::endpoints(vec![format!(
        "127.0.0.1:{}",
        booted.addr.port()
    )])
    .channel_connector(move |endpoint: &str| {
        let tls = tls.clone();
        let uri = format!("https://{endpoint}");
        async move {
            let ep = tonic::transport::Endpoint::from_shared(uri)?.tls_config(tls)?;
            Ok(ep.connect().await?)
        }
    })
    .build()
    .await
    .expect("client connect");
    let ts = client.get_ts().await.expect("get_ts");
    assert!(ts.physical_ms() > 1_700_000_000_000);
    booted.shutdown().await.expect("server exit");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_http_endpoint_stays_plaintext_under_tls_config() {
    let bundle = mint_certs();
    let driver = Arc::new(InMemoryDriver::new());
    driver.become_leader(Epoch(1));
    let server = Server::builder()
        .consensus_driver(driver.clone())
        .build()
        .expect("server build");
    let mut booted = boot_server(server).await;
    wait_until_serving(&mut booted.state_rx).await;
    wait_for_grpc_handshake(booted.addr, Duration::from_secs(5))
        .await
        .expect("plaintext ready");

    let client = tsoracle_client::ClientBuilder::endpoints(vec![format!(
        "http://127.0.0.1:{}",
        booted.addr.port()
    )])
    .tls_config(client_tls_config_with_ca(&bundle))
    .build()
    .await
    .expect("client connect");
    let ts = client.get_ts().await.expect("get_ts");
    assert!(ts.physical_ms() > 1_700_000_000_000);
    booted.shutdown().await.expect("server exit");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bare_endpoint_becomes_tls_under_tls_config() {
    let bundle = mint_certs();
    let driver = Arc::new(InMemoryDriver::new());
    driver.become_leader(Epoch(1));
    let server = Server::builder()
        .consensus_driver(driver.clone())
        .tls_config(server_tls_config(&bundle))
        .build()
        .expect("server build");
    let mut booted = boot_server(server).await;
    wait_until_serving(&mut booted.state_rx).await;
    wait_for_grpc_handshake_tls(
        booted.addr,
        client_tls_config_with_ca(&bundle),
        Duration::from_secs(5),
    )
    .await
    .expect("tls ready");

    let client = tsoracle_client::ClientBuilder::endpoints(vec![format!(
        "127.0.0.1:{}",
        booted.addr.port()
    )])
    .tls_config(client_tls_config_with_ca(&bundle))
    .build()
    .await
    .expect("client connect");
    let ts = client.get_ts().await.expect("get_ts");
    assert!(ts.physical_ms() > 1_700_000_000_000);
    booted.shutdown().await.expect("server exit");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_hint_redirect_under_tls() {
    let bundle = mint_certs();

    let leader_driver = Arc::new(InMemoryDriver::new());
    leader_driver.become_leader(Epoch(1));
    let leader = Server::builder()
        .consensus_driver(leader_driver.clone())
        .tls_config(server_tls_config(&bundle))
        .build()
        .expect("leader build");
    let mut booted_leader = boot_server(leader).await;
    wait_until_serving(&mut booted_leader.state_rx).await;
    wait_for_grpc_handshake_tls(
        booted_leader.addr,
        client_tls_config_with_ca(&bundle),
        Duration::from_secs(5),
    )
    .await
    .expect("leader ready");

    let leader_endpoint = format!("127.0.0.1:{}", booted_leader.addr.port());
    let follower_driver = Arc::new(InMemoryDriver::new());
    follower_driver.become_follower(Some(leader_endpoint.clone()));
    let follower = Server::builder()
        .consensus_driver(follower_driver.clone())
        .tls_config(server_tls_config(&bundle))
        .build()
        .expect("follower build");
    let booted_follower = boot_server(follower).await;
    // The follower stays NotServing until it transitions to leader, which
    // it never does — so wait_until_serving would block. Wait for the TLS
    // port to be open instead.
    wait_for_grpc_handshake_tls(
        booted_follower.addr,
        client_tls_config_with_ca(&bundle),
        Duration::from_secs(5),
    )
    .await
    .expect("follower port ready");

    let client = tsoracle_client::ClientBuilder::endpoints(vec![
        format!("127.0.0.1:{}", booted_follower.addr.port()),
        format!("127.0.0.1:{}", booted_leader.addr.port()),
    ])
    .tls_config(client_tls_config_with_ca(&bundle))
    .build()
    .await
    .expect("client connect");

    let ts = client.get_ts().await.expect("get_ts");
    assert!(ts.physical_ms() > 1_700_000_000_000);

    booted_leader.shutdown().await.expect("leader exit");
    booted_follower.shutdown().await.expect("follower exit");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mtls_handshake_succeeds_with_client_identity() {
    let bundle = mint_certs();
    let driver = Arc::new(InMemoryDriver::new());
    driver.become_leader(Epoch(1));
    let server = Server::builder()
        .consensus_driver(driver.clone())
        .tls_config(mtls_server_tls_config(&bundle))
        .build()
        .expect("server build");
    let mut booted = boot_server(server).await;
    wait_until_serving(&mut booted.state_rx).await;

    let client_identity = Identity::from_pem(&bundle.client_cert_pem, &bundle.client_key_pem);
    let tls = client_tls_config_with_ca(&bundle).identity(client_identity);
    // Skip the readiness probe — it would need the same client identity.
    // The first real client call exercises that.
    let client = tsoracle_client::ClientBuilder::endpoints(vec![format!(
        "127.0.0.1:{}",
        booted.addr.port()
    )])
    .tls_config(tls)
    .build()
    .await
    .expect("client connect");
    let ts = client.get_ts().await.expect("get_ts");
    assert!(ts.physical_ms() > 1_700_000_000_000);
    booted.shutdown().await.expect("server exit");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serve_with_shutdown_applies_tls_config() {
    // `boot_server` exercises `serve_with_listener`; this scenario covers the
    // sibling `serve_with_shutdown` path so the TLS-config application inside
    // it is reached end-to-end. Bind-then-drop probes a free port for the
    // server to re-bind itself, since serve_with_shutdown takes a SocketAddr.
    let bundle = mint_certs();
    let driver = Arc::new(InMemoryDriver::new());
    driver.become_leader(Epoch(1));

    let probe = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind probe");
    let addr = probe.local_addr().expect("probe addr");
    drop(probe);

    let server = Server::builder()
        .consensus_driver(driver.clone())
        .tls_config(server_tls_config(&bundle))
        .build()
        .expect("server build");

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server_handle = tokio::spawn(async move {
        server
            .serve_with_shutdown(addr, async move {
                let _ = shutdown_rx.await;
            })
            .await
    });

    wait_for_grpc_handshake_tls(
        addr,
        client_tls_config_with_ca(&bundle),
        Duration::from_secs(5),
    )
    .await
    .expect("tls ready");

    let client =
        tsoracle_client::ClientBuilder::endpoints(vec![format!("127.0.0.1:{}", addr.port())])
            .tls_config(client_tls_config_with_ca(&bundle))
            .build()
            .await
            .expect("client connect");
    let ts = client.get_ts().await.expect("get_ts");
    assert!(ts.physical_ms() > 1_700_000_000_000);

    drop(client);
    let _ = shutdown_tx.send(());
    server_handle
        .await
        .expect("server task panicked")
        .expect("server exit");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mtls_handshake_fails_without_client_identity() {
    let bundle = mint_certs();
    let driver = Arc::new(InMemoryDriver::new());
    driver.become_leader(Epoch(1));
    let server = Server::builder()
        .consensus_driver(driver.clone())
        .tls_config(mtls_server_tls_config(&bundle))
        .build()
        .expect("server build");
    let mut booted = boot_server(server).await;
    wait_until_serving(&mut booted.state_rx).await;
    let result = tsoracle_client::ClientBuilder::endpoints(vec![format!(
        "127.0.0.1:{}",
        booted.addr.port()
    )])
    .tls_config(client_tls_config_with_ca(&bundle))
    .build()
    .await
    .expect("build does not connect")
    .get_ts()
    .await;
    // The mTLS failure mode is tonic-version dependent: the channel may open
    // lazily and the handshake error surface during the RPC as `Rpc` with
    // code `Unknown` ("transport error"), or the retry loop may exhaust to
    // `NoReachableEndpoints`, or the eager handshake may fail with `Transport`.
    // All three are valid signals of "server rejected the connection."
    match result {
        Err(tsoracle_client::ClientError::Transport(_))
        | Err(tsoracle_client::ClientError::NoReachableEndpoints)
        | Err(tsoracle_client::ClientError::Rpc(_)) => {}
        other => panic!("expected Transport/NoReachableEndpoints/Rpc, got {other:?}"),
    }
    booted.shutdown().await.expect("server exit");
}
