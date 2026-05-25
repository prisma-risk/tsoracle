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

//! Deterministic-simulation-testing (DST) regression for the `get_ts`
//! window-extension clock race.
//!
//! With `window_ahead = 0` the committed bound has zero slack, and the server
//! read `now_ms()` more than once per `get_ts` extend cycle; any clock tick
//! between those reads exhausted the retry. On fast hardware the gap stays
//! sub-millisecond, so the bug only flaked under the slow, contended CI runner.
//!
//! This test removes real time entirely: the real tsoracle gRPC server + a tonic
//! client run inside `turmoil`'s single-threaded simulation, with a `Clock` that
//! advances 1ms on every read — making the two-read skew happen on every run
//! from a fixed seed. RED before the read-once fix, GREEN after. The turmoil
//! transport glue is shared from `tsoracle_server_testkit`.
//!
//! Opt in: `cargo test -p tsoracle-tests --features dst --test dst_window_race`.

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tsoracle_core::{Clock, Epoch};
use tsoracle_proto::v1::GetTsRequest;
use tsoracle_server::Server;
use tsoracle_server::test_fakes::StallableDriver;
use tsoracle_server_testkit::{client, into_sim_parts, serve};
use turmoil::Builder;

const PORT: u16 = 9_999;

/// A clock that advances by 1ms on *every* read, modelling "the wall clock
/// ticked between the server's two `now_ms()` reads" deterministically.
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
        self.ms.fetch_add(1, Ordering::Relaxed)
    }
}

#[test]
fn window_extension_race_is_deterministic_under_dst() {
    let mut sim = Builder::new().build();

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
            let parts = into_sim_parts(server)?;
            driver.become_leader(Epoch(1));
            serve(parts.routes, PORT).await?;
            Ok(())
        }
    });

    sim.client("client", async move {
        let mut grpc = client("server", PORT)?;
        for _ in 0..100 {
            match grpc.get_ts(GetTsRequest { count: 1 }).await {
                Ok(_) => return Ok(()), // GREEN: succeeds with the read-once fix.
                Err(status) => match status.code() {
                    // Leadership not yet seeded / still connecting — retry.
                    tonic::Code::FailedPrecondition | tonic::Code::Unavailable => {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    // Terminal: against the unfixed server this is the
                    // deterministically-reproduced race.
                    _ => {
                        return Err(Box::<dyn Error>::from(format!(
                            "get_ts must succeed under a forward-ticking clock with \
                             window_ahead=0, but the window-extension race surfaced: {status:?}"
                        )));
                    }
                },
            }
        }
        Err(Box::<dyn Error>::from(
            "server never became leader within the poll budget".to_string(),
        ))
    });

    sim.run().expect(
        "get_ts should succeed under deterministic simulation; \
         the two-now_ms-read window-extension race was reproduced",
    );
}
