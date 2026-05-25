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

//! Deterministic-simulation (turmoil) variants of the end-to-end client tests.
//!
//! These drive the *high-level* `tsoracle_client::Client` — its coalescing
//! driver, retry loop, leader-hint redirect, and channel pool — over turmoil's
//! simulated network, by handing `ClientBuilder::channel_connector` a closure
//! that dials each endpoint through `tsoracle_server_testkit::sim_channel`. No change
//! to the published client crate is needed: the connector hook already exists,
//! and tonic's lazy `Channel` keeps the `!Send` turmoil IO on the sim's
//! connection task while exposing a `Send` handle to the driver.
//!
//! Opt in: `cargo test -p tsoracle-tests --features dst --test client_e2e_dst`.

use std::sync::Arc;
use std::time::Duration;

use tsoracle_client::{Client, ClientBuilder, ClientError};
use tsoracle_core::{Clock, Epoch};
use tsoracle_server::Server;
use tsoracle_server::test_fakes::InMemoryDriver;
use tsoracle_server_testkit::{into_sim_parts, serve, sim_channel};

const PORT: u16 = 9_999;
const EPOCH_BASE_MS: u64 = 1_700_000_000_001;
const EPOCH_THRESHOLD: u64 = 1_700_000_000_000;

/// A fixed `Clock` seeded near a real wall-clock epoch so granted timestamps
/// clear the `> 1.7e12` epoch assertions. (`tsoracle_core::testing::MockClock`
/// is gated behind `test-clock`, which this crate doesn't pull, so DST tests
/// supply their own `Clock`.)
struct FixedClock(u64);

impl Clock for FixedClock {
    fn now_ms(&self) -> u64 {
        self.0
    }
}

/// Build a high-level `Client` whose channel pool dials every endpoint
/// (configured or leader-hint) over the turmoil network.
async fn sim_client(endpoints: Vec<String>) -> Result<Client, ClientError> {
    ClientBuilder::endpoints(endpoints)
        .channel_connector(|ep: &str| {
            let ep = ep.to_string();
            async move {
                sim_channel(&ep)
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            }
        })
        .build()
        .await
}

fn sim() -> turmoil::Sim<'static> {
    turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(60))
        .build()
}

/// Run `body` on a leader server (host `name`, port `PORT`). The driver becomes
/// leader before `serve` binds the listener, so a client cannot connect until
/// the server is logically ready.
fn leader_host(sim: &mut turmoil::Sim<'static>, name: &'static str, driver: Arc<InMemoryDriver>) {
    sim.host(name, move || {
        let driver = driver.clone();
        async move {
            let server = Server::builder()
                .consensus_driver(driver.clone())
                .clock(Arc::new(FixedClock(EPOCH_BASE_MS)))
                .build()
                .unwrap();
            let parts = into_sim_parts(server)?;
            driver.become_leader(Epoch(1));
            serve(parts.routes, PORT).await?;
            Ok(())
        }
    });
}

#[test]
fn client_gets_timestamps_against_leader() {
    let driver = Arc::new(InMemoryDriver::new());
    let mut sim = sim();
    leader_host(&mut sim, "server", driver);
    sim.client("client", async move {
        let client = sim_client(vec![format!("server:{PORT}")]).await?;
        for _ in 0..100 {
            match client.get_ts().await {
                Ok(ts) => {
                    assert!(ts.physical_ms() > EPOCH_THRESHOLD);
                    return Ok(());
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(25)).await,
            }
        }
        Err("client never became responsive".into())
    });
    sim.run().unwrap();
}

#[test]
fn client_follows_leader_hint_on_first_call() {
    // Server A is a follower hinting at B; B is the leader. The client knows
    // only A. The first get_ts hits A, gets NOT_LEADER with a hint→B, and must
    // follow it to B within the same call.
    let driver_a = Arc::new(InMemoryDriver::new());
    let driver_b = Arc::new(InMemoryDriver::new());
    let mut sim = sim();

    {
        let driver_a = driver_a.clone();
        sim.host("server-a", move || {
            let driver_a = driver_a.clone();
            async move {
                let server = Server::builder()
                    .consensus_driver(driver_a.clone())
                    .clock(Arc::new(FixedClock(EPOCH_BASE_MS)))
                    .build()
                    .unwrap();
                let parts = into_sim_parts(server)?;
                driver_a.become_follower(Some(format!("server-b:{PORT}")));
                serve(parts.routes, PORT).await?;
                Ok(())
            }
        });
    }
    leader_host(&mut sim, "server-b", driver_b);

    sim.client("client", async move {
        // Client only knows about A.
        let client = sim_client(vec![format!("server-a:{PORT}")]).await?;
        for _ in 0..100 {
            match client.get_ts().await {
                Ok(ts) => {
                    assert!(ts.physical_ms() > EPOCH_THRESHOLD);
                    return Ok(());
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(25)).await,
            }
        }
        Err("client never followed the hint to a timestamp".into())
    });
    sim.run().unwrap();
}

#[test]
fn client_surfaces_error_when_only_endpoint_is_a_hintless_follower() {
    // A follower with no known leader replies FailedPrecondition with an empty
    // hint. With nothing actionable to follow, the client must surface the RPC
    // error rather than loop forever.
    let driver = Arc::new(InMemoryDriver::new());
    let mut sim = sim();

    {
        let driver = driver.clone();
        sim.host("server", move || {
            let driver = driver.clone();
            async move {
                let server = Server::builder()
                    .consensus_driver(driver.clone())
                    .clock(Arc::new(FixedClock(EPOCH_BASE_MS)))
                    .build()
                    .unwrap();
                let parts = into_sim_parts(server)?;
                driver.become_follower(None);
                serve(parts.routes, PORT).await?;
                Ok(())
            }
        });
    }

    sim.client("client", async move {
        let client = sim_client(vec![format!("server:{PORT}")]).await?;
        // Retry only past connect-time transport errors (listener not up yet);
        // the hintless-follower verdict is FailedPrecondition.
        for _ in 0..100 {
            match client.get_ts().await {
                Ok(_) => panic!("hintless follower must not yield a timestamp"),
                Err(ClientError::Rpc(status)) => {
                    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
                    return Ok(());
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(25)).await,
            }
        }
        Err("never observed the hintless-follower FAILED_PRECONDITION".into())
    });
    sim.run().unwrap();
}

#[test]
fn concurrent_requests_coalesce() {
    let driver = Arc::new(InMemoryDriver::new());
    let mut sim = sim();
    leader_host(&mut sim, "server", driver);

    sim.client("client", async move {
        let client = Arc::new(sim_client(vec![format!("server:{PORT}")]).await?);
        // Warm up until responsive, then fire 32 logically-concurrent requests;
        // the driver's flush window coalesces many onto one RPC, but every
        // returned timestamp must be unique.
        for _ in 0..100 {
            if client.get_ts().await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let requests = (0..32).map(|_| {
            let client = client.clone();
            async move { client.get_ts().await }
        });
        let results = futures::future::join_all(requests).await;
        let mut timestamps = Vec::with_capacity(32);
        for result in results {
            timestamps.push(result?);
        }
        timestamps.sort();
        timestamps.dedup();
        assert_eq!(timestamps.len(), 32, "all 32 timestamps must be unique");
        Ok(())
    });
    sim.run().unwrap();
}
