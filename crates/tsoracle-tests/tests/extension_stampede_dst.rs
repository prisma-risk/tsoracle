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

//! Deterministic-simulation (turmoil) regression for the window-extension
//! stampede single-flight.
//!
//! When `committed_high_water` is exceeded, every concurrent `get_ts` that hits
//! `WindowExhausted` enters `extend_window`. The `extension_lock` mutex plus a
//! recheck-after-acquire must coalesce a burst into exactly one
//! `persist_high_water` round-trip. The original test reproduced the burst with
//! 50 OS-thread-concurrent clients and a real 50ms persist delay to make them
//! pile up; that ordering is timing-dependent. Here the burst is 50 logically
//! concurrent requests driven by `join_all` under turmoil's single-threaded
//! deterministic scheduler — the `extension_lock` contention (and so the
//! coalescing) is exercised identically, but deterministically and instantly.
//!
//! Opt in: `cargo test -p tsoracle-tests --features dst --test extension_stampede_dst`.

use core::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::Stream;
use futures::future::join_all;
use tsoracle_consensus::{ConsensusDriver, ConsensusError, LeaderState};
use tsoracle_core::{Epoch, testing::MockClock};
use tsoracle_proto::v1::GetTsRequest;
use tsoracle_server::Server;
use tsoracle_server::test_fakes::InMemoryDriver;
use tsoracle_server::test_support::wait_until_serving;
use tsoracle_server_testkit::{client, into_sim_parts, serve};

const PORT: u16 = 9_999;
const STAMPEDE_SIZE: usize = 50;

/// Wraps `InMemoryDriver` to count `persist_high_water` calls and inject a
/// delay so concurrent stampeders pile up inside the extension path. Under
/// turmoil the delay is simulated time (instant in wall-clock terms).
struct StampedeProbeDriver {
    inner: Arc<InMemoryDriver>,
    persist_calls: AtomicUsize,
    persist_delay: Duration,
}

impl StampedeProbeDriver {
    fn new(inner: Arc<InMemoryDriver>, persist_delay: Duration) -> Self {
        Self {
            inner,
            persist_calls: AtomicUsize::new(0),
            persist_delay,
        }
    }

    fn persist_calls(&self) -> usize {
        self.persist_calls.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl ConsensusDriver for StampedeProbeDriver {
    fn leadership_events(&self) -> Pin<Box<dyn Stream<Item = LeaderState> + Send>> {
        self.inner.leadership_events()
    }

    async fn load_high_water(&self) -> Result<u64, ConsensusError> {
        self.inner.load_high_water().await
    }

    async fn persist_high_water(&self, at_least: u64, epoch: Epoch) -> Result<u64, ConsensusError> {
        self.persist_calls.fetch_add(1, Ordering::SeqCst);
        if !self.persist_delay.is_zero() {
            tokio::time::sleep(self.persist_delay).await;
        }
        self.inner.persist_high_water(at_least, epoch).await
    }
}

#[test]
fn extension_stampede_coalesces_to_single_persist() {
    let in_memory = Arc::new(InMemoryDriver::new());
    let probe = Arc::new(StampedeProbeDriver::new(
        in_memory.clone(),
        Duration::from_millis(50),
    ));
    let clock = Arc::new(MockClock::new(1_000));
    // Filled in by the server host once the fence's seed persist has settled.
    let baseline = Arc::new(AtomicUsize::new(0));

    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(300))
        .build();

    {
        let in_memory = in_memory.clone();
        let probe = probe.clone();
        let clock = clock.clone();
        let baseline = baseline.clone();
        sim.host("server", move || {
            let in_memory = in_memory.clone();
            let probe = probe.clone();
            let clock = clock.clone();
            let baseline = baseline.clone();
            async move {
                let server = Server::builder()
                    .consensus_driver(probe.clone())
                    .clock(clock.clone())
                    .window_ahead(Duration::from_millis(50))
                    .failover_advance(Duration::from_millis(50))
                    .build()
                    .unwrap();
                let mut parts = into_sim_parts(server)?;
                in_memory.become_leader(Epoch(1));
                wait_until_serving(&mut parts.state_rx).await;

                // Account for the seed persist done by the fence, then push the
                // clock past the seeded committed_high_water so every grant
                // attempt hits WindowExhausted. Both happen before `serve` binds
                // the listener, so the client cannot connect (and thus cannot
                // issue any request) until the window is already exhausted.
                baseline.store(probe.persist_calls(), Ordering::SeqCst);
                clock.set(10_000);

                serve(parts.routes, PORT).await?;
                Ok(())
            }
        });
    }

    sim.client("client", async move {
        let grpc = client("server", PORT)?;
        // The listener only binds after the server pushes the clock, so early
        // batches fail at connect (contacting the server zero times — no
        // persist). Retry the whole burst until it lands; the landing batch is
        // the only one that reaches the server, and it must coalesce to one
        // persist.
        for _ in 0..50 {
            let requests = (0..STAMPEDE_SIZE).map(|_| {
                let mut grpc = grpc.clone();
                async move { grpc.get_ts(GetTsRequest { count: 1 }).await }
            });
            let results = join_all(requests).await;
            if results.iter().all(|r| r.is_ok()) {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Err("stampede never fully succeeded within the poll budget".into())
    });

    sim.run().unwrap();

    let extensions = probe.persist_calls() - baseline.load(Ordering::SeqCst);
    assert_eq!(
        extensions, 1,
        "expected exactly one persist_high_water call for {STAMPEDE_SIZE} concurrent \
         stampeders, observed {extensions} — single-flight is not engaged"
    );
}
