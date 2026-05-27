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

//! Deterministic-simulation (turmoil) variant of the freshness property: every
//! timestamp has `physical_ms >= the clock time at which its caller enqueued`.
//!
//! The real-time original compares the granted `physical_ms` against
//! `SystemTime::now()` with a 2ms tolerance — a wall-clock race. Here the server
//! grants from a `SharedClock` that the test also reads for "enqueue time", so
//! the contract holds exactly (no tolerance) and deterministically: the grant
//! samples the clock no earlier than the enqueue read, and the allocator floors
//! `physical_ms` at the clock. Advancing the clock between iterations exercises
//! the floor tracking the clock forward.
//!
//! Opt in: `cargo test -p tsoracle-tests --features dst --test client_freshness_dst`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tsoracle_client::{Client, ClientBuilder, ClientError};
use tsoracle_core::Epoch;
use tsoracle_server::test_fakes::InMemoryDriver;
use tsoracle_server::{Clock, Server};
use tsoracle_server_testkit::{into_sim_parts, serve, sim_channel};

const PORT: u16 = 9_999;
const EPOCH_BASE_MS: u64 = 1_700_000_000_001;

/// A hand-driven clock shared between the server (which grants from it) and the
/// test (which reads it as "enqueue time" and advances it). Monotonic.
#[derive(Clone)]
struct SharedClock(Arc<AtomicU64>);

impl SharedClock {
    fn new(start_ms: u64) -> Self {
        SharedClock(Arc::new(AtomicU64::new(start_ms)))
    }
    fn now(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
    fn advance(&self, by_ms: u64) {
        self.0.fetch_add(by_ms, Ordering::SeqCst);
    }
}

impl Clock for SharedClock {
    fn now_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

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

#[test]
fn timestamps_are_at_or_after_enqueue_time() {
    let driver = Arc::new(InMemoryDriver::new());
    let clock = SharedClock::new(EPOCH_BASE_MS);

    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(120))
        .build();

    {
        let driver = driver.clone();
        let clock = clock.clone();
        sim.host("server", move || {
            let driver = driver.clone();
            let clock = clock.clone();
            async move {
                let server = Server::builder()
                    .consensus_driver(driver.clone())
                    .clock(Arc::new(clock))
                    .build()
                    .unwrap();
                let parts = into_sim_parts(server)?;
                driver.become_leader(Epoch(1));
                serve(parts.routes, PORT).await?;
                Ok(())
            }
        });
    }

    sim.client("client", async move {
        let client = sim_client(vec![format!("server:{PORT}")]).await?;
        // Wait until responsive.
        for _ in 0..100 {
            if client.get_ts().await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        for _ in 0..200 {
            let enqueue_ms = clock.now();
            let ts = client.get_ts().await?;
            assert!(
                ts.physical_ms() >= enqueue_ms,
                "freshness violated: ts.physical_ms={} < enqueue_ms={}",
                ts.physical_ms(),
                enqueue_ms
            );
            // Move wall time forward so the next grant must track it.
            clock.advance(1);
        }
        Ok(())
    });

    sim.run().unwrap();
}
