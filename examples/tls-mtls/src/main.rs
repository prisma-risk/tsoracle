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

//! Four-step TLS / mTLS demo:
//!   1. Plain TLS (server identity, client verifies)
//!   2. mTLS (server also verifies client identity)
//!   3. Custom connector against the mTLS server (transport escape hatch)
//!   4. mTLS misconfiguration (no client identity) — expected to fail
//!
//! Each step boots a fresh server, runs three GetTs calls, and prints the
//! results. All certs are minted in process via `rcgen`; nothing is read
//! from disk.

mod certs;

use anyhow::Result;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tonic::transport::{Certificate, ClientTlsConfig, Identity, ServerTlsConfig};
use tsoracle_client::{ClientBuilder, ClientError};
use tsoracle_driver_file::FileDriver;
use tsoracle_server::{Server, ServingState};

async fn wait_until_serving(rx: &mut watch::Receiver<ServingState>) {
    loop {
        if matches!(*rx.borrow_and_update(), ServingState::Serving) {
            return;
        }
        if rx.changed().await.is_err() {
            return;
        }
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let bundle = certs::mint()?;

    step_plain_tls(&bundle).await?;
    step_mtls(&bundle).await?;
    step_custom_connector_against_mtls(&bundle).await?;
    step_mtls_misconfigured(&bundle).await?;

    Ok(())
}

async fn step_plain_tls(bundle: &certs::CertBundle) -> Result<()> {
    println!("\n=== Step 1: plain TLS ===");
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let driver = boot_file_driver("tls")?;

    let server = Server::builder()
        .consensus_driver(driver)
        .tls_config(ServerTlsConfig::new().identity(server_identity(bundle)))
        .build()?;

    let mut state_rx = server.subscribe();
    let server_handle = tokio::spawn(async move {
        server
            .serve_with_listener(listener, futures::future::pending())
            .await
    });
    wait_until_serving(&mut state_rx).await;

    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(&bundle.ca_pem))
        .domain_name("localhost");
    let client = ClientBuilder::endpoints(vec![format!("127.0.0.1:{}", addr.port())])
        .tls_config(tls)
        .build()
        .await?;

    for i in 0..3 {
        let ts = client.get_ts().await?;
        println!(
            "  [tls] call {i} -> physical_ms={} logical={}",
            ts.physical_ms(),
            ts.logical()
        );
    }

    drop(client);
    server_handle.abort();
    Ok(())
}

async fn step_mtls(bundle: &certs::CertBundle) -> Result<()> {
    println!("\n=== Step 2: mTLS ===");
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let driver = boot_file_driver("mtls")?;

    let server = Server::builder()
        .consensus_driver(driver)
        .tls_config(
            ServerTlsConfig::new()
                .identity(server_identity(bundle))
                .client_ca_root(Certificate::from_pem(&bundle.ca_pem)),
        )
        .build()?;

    let mut state_rx = server.subscribe();
    let server_handle = tokio::spawn(async move {
        server
            .serve_with_listener(listener, futures::future::pending())
            .await
    });
    wait_until_serving(&mut state_rx).await;

    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(&bundle.ca_pem))
        .identity(Identity::from_pem(
            &bundle.client_cert_pem,
            &bundle.client_key_pem,
        ))
        .domain_name("localhost");
    let client = ClientBuilder::endpoints(vec![format!("127.0.0.1:{}", addr.port())])
        .tls_config(tls)
        .build()
        .await?;

    for i in 0..3 {
        let ts = client.get_ts().await?;
        println!(
            "  [mtls] call {i} -> physical_ms={} logical={}",
            ts.physical_ms(),
            ts.logical()
        );
    }

    drop(client);
    server_handle.abort();
    Ok(())
}

async fn step_custom_connector_against_mtls(bundle: &certs::CertBundle) -> Result<()> {
    println!("\n=== Step 3: custom connector against mTLS server ===");
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let driver = boot_file_driver("connector")?;

    let server = Server::builder()
        .consensus_driver(driver)
        .tls_config(
            ServerTlsConfig::new()
                .identity(server_identity(bundle))
                .client_ca_root(Certificate::from_pem(&bundle.ca_pem)),
        )
        .build()?;

    let mut state_rx = server.subscribe();
    let server_handle = tokio::spawn(async move {
        server
            .serve_with_listener(listener, futures::future::pending())
            .await
    });
    wait_until_serving(&mut state_rx).await;

    // `.channel_connector(...)` is the escape hatch when you need transport
    // knobs not exposed on the builder (keepalive here; concurrency,
    // proxies, service-mesh integrations elsewhere).
    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(&bundle.ca_pem))
        .identity(Identity::from_pem(
            &bundle.client_cert_pem,
            &bundle.client_key_pem,
        ))
        .domain_name("localhost");
    let client = ClientBuilder::endpoints(vec![format!("127.0.0.1:{}", addr.port())])
        .channel_connector(move |endpoint: &str| {
            let tls = tls.clone();
            let uri = format!("https://{endpoint}");
            async move {
                let ep = tonic::transport::Endpoint::from_shared(uri)?
                    .tls_config(tls)?
                    .keep_alive_while_idle(true);
                Ok(ep.connect().await?)
            }
        })
        .build()
        .await?;

    for i in 0..3 {
        let ts = client.get_ts().await?;
        println!(
            "  [connector] call {i} -> physical_ms={} logical={}",
            ts.physical_ms(),
            ts.logical()
        );
    }

    drop(client);
    server_handle.abort();
    Ok(())
}

async fn step_mtls_misconfigured(bundle: &certs::CertBundle) -> Result<()> {
    println!("\n=== Step 4: mTLS misconfigured (no client identity, expected failure) ===");
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let driver = boot_file_driver("misconfigured")?;

    let server = Server::builder()
        .consensus_driver(driver)
        .tls_config(
            ServerTlsConfig::new()
                .identity(server_identity(bundle))
                .client_ca_root(Certificate::from_pem(&bundle.ca_pem)),
        )
        .build()?;

    let mut state_rx = server.subscribe();
    let server_handle = tokio::spawn(async move {
        server
            .serve_with_listener(listener, futures::future::pending())
            .await
    });
    wait_until_serving(&mut state_rx).await;

    // No `.identity(...)` on the client TLS config.
    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(&bundle.ca_pem))
        .domain_name("localhost");
    let client = ClientBuilder::endpoints(vec![format!("127.0.0.1:{}", addr.port())])
        .tls_config(tls)
        .build()
        .await?;

    match client.get_ts().await {
        Err(ClientError::Transport(err)) => {
            println!("  [expected] mTLS without identity -> ClientError::Transport: {err}");
        }
        Err(ClientError::TransportFanout(message)) => {
            println!(
                "  [expected] mTLS without identity -> ClientError::TransportFanout: {message}"
            );
        }
        Err(ClientError::NoReachableEndpoints) => {
            println!("  [expected] mTLS without identity -> NoReachableEndpoints");
        }
        Err(ClientError::Rpc(status)) => {
            println!(
                "  [expected] mTLS without identity -> ClientError::Rpc: code={:?} message={}",
                status.code(),
                status.message()
            );
        }
        other => println!("  [unexpected] {other:?}"),
    }

    drop(client);
    server_handle.abort();
    Ok(())
}

fn server_identity(bundle: &certs::CertBundle) -> Identity {
    Identity::from_pem(&bundle.server_cert_pem, &bundle.server_key_pem)
}

fn boot_file_driver(tag: &str) -> Result<Arc<FileDriver>> {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let dir = std::env::temp_dir().join(format!("tsoracle-example-tls-{tag}-{nanos}"));
    std::fs::create_dir_all(&dir)?;
    Ok(FileDriver::open_or_init(&dir)?)
}
