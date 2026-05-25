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

//! Deterministic-simulation-testing harness: run the real gRPC server (and,
//! optionally, a tonic client) over [`turmoil`]'s simulated network and clock,
//! so timing-sensitive flows — window extension, fence retry/backoff,
//! leadership churn — execute deterministically and instantly (sleeps advance
//! simulated time).
//!
//! The transport glue (`Accepted` / connector) is shared so individual DST
//! tests don't re-implement it. [`into_sim_parts`] spawns the leader-watch task
//! on the calling host's executor, so the fence runs under simulated time as
//! soon as it returns; tests then drive the in-memory driver and observe the
//! returned `state_rx`.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::future::Future;
use std::net::{IpAddr, Ipv4Addr};
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::watch;
use tonic::service::Routes;
use tonic::transport::server::{Connected, TcpConnectInfo};
use tonic::transport::{Channel, Endpoint, Server as TonicServer};
use tsoracle_proto::v1::tso_service_client::TsoServiceClient;
use turmoil::net::{TcpListener, TcpStream};

use tsoracle_server::{Server, ServerError, ServingState, WatchGuard};

// Matches turmoil's `Result` error type so `?` works directly in host/client
// closures.
type BoxError = Box<dyn std::error::Error>;
type ConnFut = Pin<
    Box<dyn Future<Output = Result<hyper_util::rt::TokioIo<TcpStream>, std::io::Error>> + Send>,
>;

/// The pieces needed to drive a server inside a turmoil host: the gRPC
/// `Routes` (serve them with [`serve`]), the leader-watch [`WatchGuard`]
/// (keep it alive for as long as the host should serve — dropping it
/// cooperatively stops the task), and a clone of the observable
/// `ServingState` channel.
pub struct SimParts {
    pub routes: Routes,
    pub watch: WatchGuard,
    pub state_rx: watch::Receiver<ServingState>,
}

/// Capture a built [`Server`]'s sim parts. `into_router` spawns the
/// leader-watch task on the calling host's executor, so the fence begins
/// running under simulated time as soon as this returns.
pub fn into_sim_parts(server: Server) -> Result<SimParts, ServerError> {
    let state_rx = server.subscribe();
    let (routes, watch) = server.into_router()?;
    Ok(SimParts {
        routes,
        watch,
        state_rx,
    })
}

/// Serve `routes` over a `turmoil` listener on `port` until the sim ends.
/// Run inside a host closure (directly or via `tokio::spawn`).
pub async fn serve(routes: Routes, port: u16) -> Result<(), BoxError> {
    TonicServer::builder()
        .add_routes(routes)
        .serve_with_incoming(async_stream::stream! {
            let listener = TcpListener::bind((IpAddr::from(Ipv4Addr::UNSPECIFIED), port)).await?;
            loop {
                yield listener.accept().await.map(|(stream, _)| Accepted(stream));
            }
        })
        .await?;
    Ok(())
}

/// A lazy tonic client that dials `host:port` over the turmoil network.
pub fn client(host: &str, port: u16) -> Result<TsoServiceClient<Channel>, BoxError> {
    let channel =
        Endpoint::new(format!("http://{host}:{port}"))?.connect_with_connector_lazy(connector());
    Ok(TsoServiceClient::new(channel))
}

/// Build a lazy tonic [`Channel`] that dials `endpoint` over the turmoil
/// network. Bare `host:port` is rewritten to `http://host:port`.
///
/// This is the per-endpoint building block for driving the *high-level*
/// [`tsoracle_client::Client`] over turmoil: pass a closure delegating to
/// it into `ClientBuilder::channel_connector`, and the client's pool —
/// including leader-hint redirects — dials every endpoint through the
/// simulated network.
pub fn sim_channel(endpoint: &str) -> Result<Channel, tonic::transport::Error> {
    let uri = if endpoint.contains("://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    };
    Ok(Endpoint::new(uri)?.connect_with_connector_lazy(connector()))
}

/// Server-side transport glue: wrap an accepted turmoil stream so tonic
/// treats it as a connected transport.
struct Accepted(TcpStream);

impl Connected for Accepted {
    type ConnectInfo = TcpConnectInfo;
    fn connect_info(&self) -> Self::ConnectInfo {
        TcpConnectInfo {
            local_addr: self.0.local_addr().ok(),
            remote_addr: self.0.peer_addr().ok(),
        }
    }
}

impl AsyncRead for Accepted {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl AsyncWrite for Accepted {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

/// Client-side transport glue: dial over the turmoil network and hand tonic
/// a `TokioIo`-wrapped stream.
fn connector() -> impl tower::Service<
    hyper::Uri,
    Response = hyper_util::rt::TokioIo<TcpStream>,
    Error = std::io::Error,
    Future = ConnFut,
> + Clone {
    tower::service_fn(|uri: hyper::Uri| {
        Box::pin(async move {
            let conn = TcpStream::connect(uri.authority().unwrap().as_str()).await?;
            Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(conn))
        }) as ConnFut
    })
}
