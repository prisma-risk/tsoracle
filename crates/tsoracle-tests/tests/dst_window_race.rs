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

//! Deterministic-simulation-testing (DST) trial via `turmoil`.
//!
//! The CI `coverage` job intermittently fails `client_backpressure`'s
//! `first_chunk_delivers_before_slow_second_chunk_e2e` with
//! `Status { code: Internal, message: "window exhausted" }`. That flake is a
//! wall-clock race: with `window_ahead = 0` the committed bound has zero slack,
//! and the server reads `now_ms()` twice per `get_ts` extend cycle (once in
//! `extend_window`'s `would_grant` recheck, once in the retry `try_grant`). If
//! the clock ticks between those reads, the retry exhausts. On a fast machine
//! the gap stays sub-millisecond, so the bug is unreproducible locally — it only
//! surfaces under the slow, contended CI runner.
//!
//! This test removes the dependency on real time entirely. It runs the *real*
//! tsoracle gRPC server and a tonic client inside `turmoil`'s single-threaded,
//! deterministic simulation, and injects a `Clock` that advances by 1ms on every
//! read. That makes the two-read skew happen on every run from a fixed seed:
//! "1-in-1000 on slow CI" becomes "100%, here, now".
//!
//! It is written as a regression test for the deterministic fix: the client
//! asserts that `get_ts` *succeeds*. Against the current (unfixed) server it is
//! RED — the simulation reproduces the race as a deterministic
//! `Internal "window exhausted"`. Once the server reads `now_ms()` once per
//! `get_ts` and reuses it, the test goes GREEN.
//!
//! Opt in: `cargo test -p tsoracle-tests --features dst --test dst_window_race`.

use std::error::Error;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tonic::transport::{Endpoint, Server as TonicServer};
use tsoracle_core::{Clock, Epoch};
use tsoracle_proto::v1::{GetTsRequest, tso_service_client::TsoServiceClient};
use tsoracle_server::Server;
use tsoracle_server::test_fakes::StallableDriver;
use turmoil::Builder;
use turmoil::net::TcpListener;

const PORT: u16 = 9999;

/// A clock that advances by 1ms on *every* read, modelling "the wall clock
/// ticked between the server's two `now_ms()` reads" deterministically. The
/// starting value is irrelevant; only the forward movement matters.
struct TickingClock {
    ms: AtomicU64,
}

impl TickingClock {
    fn new(start_ms: u64) -> Self {
        TickingClock {
            ms: AtomicU64::new(start_ms),
        }
    }
}

impl Clock for TickingClock {
    fn now_ms(&self) -> u64 {
        // fetch_add returns the pre-increment value, so successive reads yield
        // strictly increasing, distinct milliseconds.
        self.ms.fetch_add(1, Ordering::Relaxed)
    }
}

#[test]
fn window_extension_race_is_deterministic_under_dst() {
    let mut sim = Builder::new().build();

    // The server host: the real tsoracle gRPC stack, served over turmoil's
    // simulated network. `window_ahead = 0` (zero slack) + the ticking clock
    // make the two-read skew deterministic.
    let driver = Arc::new(StallableDriver::new());
    sim.host("server", move || {
        let driver = driver.clone();
        async move {
            let server = Server::builder()
                .consensus_driver(driver.clone())
                .clock(Arc::new(TickingClock::new(1_000)))
                .window_ahead(Duration::ZERO)
                .failover_advance(Duration::ZERO)
                .build()
                .expect("build server");

            // `into_router` spawns the leader-watch task that seeds the
            // allocator once the driver reports leadership.
            let (routes, _watch) = server.into_router().expect("into_router");
            driver.become_leader(Epoch(1));

            TonicServer::builder()
                .add_routes(routes)
                .serve_with_incoming(async_stream::stream! {
                    let listener =
                        TcpListener::bind((IpAddr::from(Ipv4Addr::UNSPECIFIED), PORT)).await?;
                    loop {
                        yield listener.accept().await.map(|(stream, _)| incoming::Accepted(stream));
                    }
                })
                .await?;
            Ok(())
        }
    });

    // The client: polls `get_ts` until leadership is established, then asserts
    // it succeeds. NOT_LEADER (FailedPrecondition) / Unavailable mean "not ready
    // yet" — retry. An Internal reply is terminal: it is the reproduced race.
    sim.client("client", async move {
        let channel = Endpoint::new(format!("http://server:{PORT}"))?
            .connect_with_connector_lazy(connector::connector());
        let mut client = TsoServiceClient::new(channel);

        for _ in 0..100 {
            match client.get_ts(GetTsRequest { count: 1 }).await {
                Ok(_) => return Ok(()), // GREEN: succeeds once the fix lands.
                Err(status) => match status.code() {
                    tonic::Code::FailedPrecondition | tonic::Code::Unavailable => {
                        // Leadership not yet seeded / still connecting — retry.
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    _ => {
                        // Terminal. Against the unfixed server this is the
                        // deterministically-reproduced race.
                        return Err(Box::<dyn Error>::from(format!(
                            "get_ts must succeed under a forward-ticking clock with \
                             window_ahead=0, but the window-extension race surfaced: {status:?}"
                        )));
                    }
                },
            }
        }
        Err(Box::<dyn Error + Send + Sync>::from(
            "server never became leader within the poll budget",
        ))
    });

    sim.run().expect(
        "get_ts should succeed under deterministic simulation; \
         the two-now_ms-read window-extension race was reproduced",
    );
}

/// Server-side glue: wrap an accepted `turmoil` stream so tonic can treat it as
/// a connected transport. Mirrors the upstream turmoil gRPC example.
mod incoming {
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
    use tonic::transport::server::{Connected, TcpConnectInfo};
    use turmoil::net::TcpStream;

    pub struct Accepted(pub TcpStream);

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
        ) -> Poll<Result<(), std::io::Error>> {
            Pin::new(&mut self.0).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for Accepted {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<Result<usize, std::io::Error>> {
            Pin::new(&mut self.0).poll_write(cx, buf)
        }

        fn poll_flush(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), std::io::Error>> {
            Pin::new(&mut self.0).poll_flush(cx)
        }

        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), std::io::Error>> {
            Pin::new(&mut self.0).poll_shutdown(cx)
        }
    }
}

/// Client-side glue: a tower service that dials over turmoil's simulated network
/// and hands tonic a `TokioIo`-wrapped stream. Mirrors the upstream example.
mod connector {
    use std::future::Future;
    use std::pin::Pin;

    use hyper::Uri;
    use hyper_util::rt::TokioIo;
    use tower::Service;
    use turmoil::net::TcpStream;

    type Fut = Pin<Box<dyn Future<Output = Result<TokioIo<TcpStream>, std::io::Error>> + Send>>;

    pub fn connector()
    -> impl Service<Uri, Response = TokioIo<TcpStream>, Error = std::io::Error, Future = Fut> + Clone
    {
        tower::service_fn(|uri: Uri| {
            Box::pin(async move {
                let conn = TcpStream::connect(uri.authority().unwrap().as_str()).await?;
                Ok::<_, std::io::Error>(TokioIo::new(conn))
            }) as Fut
        })
    }
}
