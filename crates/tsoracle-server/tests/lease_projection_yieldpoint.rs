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

//! A lease mutation that loses leadership between its window extension and its lease-set projection must not persist a set projected from the cleared table.
//!
//! The acquire parks at `server::lease_flow::before_projection` after its extension commits, still holding the drain barrier. The test then steps the node down, which clears the lease table, and re-elects it at a higher epoch, which parks the fence behind that barrier. Released, an unguarded projection would read the empty table and persist a set holding only the new record, dropping the other holder's live lease from durable state before the fence loads it. The guarded projection refuses instead. Lives in its own binary because the yield-point registry is process-global.

#![cfg(feature = "yieldpoints")]

use std::sync::Arc;
use std::time::Duration;

use tonic::Code;
use tsoracle_core::Epoch;
use tsoracle_proto::v1::{AcquireLeaseRequest, RenewLeaseRequest};
use tsoracle_server::Server;
use tsoracle_server::test_fakes::{InMemoryDriver, MockClock};
use tsoracle_server::test_support::{
    boot_leader_server, wait_until_not_serving, wait_until_serving,
};
use tsoracle_yieldpoint as yieldpoint;

const PROJECTION_GATE: &str = "server::lease_flow::before_projection";
const START_MS: u64 = 1_000_000;
const TTL_MS: u64 = 10_000;

fn acquire_req(holder: &[u8]) -> AcquireLeaseRequest {
    AcquireLeaseRequest {
        holder: holder.to_vec(),
        holder_epoch: 1,
        ttl_ms: TTL_MS,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lease_projection_after_a_leadership_flap_keeps_other_live_leases() {
    let driver = Arc::new(InMemoryDriver::new());
    let clock = Arc::new(MockClock::new(START_MS));
    let server = Server::builder()
        .consensus_driver(driver.clone())
        .clock(clock.clone())
        .window_ahead(Duration::from_millis(500))
        .failover_advance(Duration::from_millis(200))
        .build()
        .unwrap();
    let (mut booted, mut client) =
        boot_leader_server(server, || driver.become_leader(Epoch(1))).await;

    let held = client
        .acquire_lease(acquire_req(b"group-a"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(driver.current_leases().len(), 1);

    // A later clock gives the second acquire a strictly higher extension bound, which is how the test sees it reach the gate.
    clock.advance(1_000);
    let high_water_before = driver.current_high_water();
    let gate = yieldpoint::cfg(PROJECTION_GATE);
    let mut racing_client = client.clone();
    let racing =
        tokio::spawn(async move { racing_client.acquire_lease(acquire_req(b"group-b")).await });

    tokio::time::timeout(Duration::from_secs(5), async {
        while driver.current_high_water() <= high_water_before {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the racing acquire must commit its extension and reach the projection gate");

    // Step down (clears the lease table), then win leadership again at a higher epoch. The fence parks behind the drain barrier the racing acquire still holds.
    driver.become_follower(None);
    wait_until_not_serving(&mut booted.state_rx).await;
    driver.become_leader(Epoch(2));

    gate.notify_one();
    let refused = tokio::time::timeout(Duration::from_secs(5), racing)
        .await
        .expect("the racing acquire must finish once released")
        .expect("the racing acquire task must not panic")
        .expect_err("an acquire that lost its epoch before projecting must fail");
    assert_eq!(refused.code(), Code::FailedPrecondition);
    yieldpoint::remove(PROJECTION_GATE);

    assert!(
        driver
            .current_leases()
            .iter()
            .any(|record| record.lease_id == held.lease_id),
        "the other holder's live lease must remain durable across the flap"
    );

    wait_until_serving(&mut booted.state_rx).await;
    client
        .renew_lease(RenewLeaseRequest {
            lease_id: held.lease_id,
        })
        .await
        .expect("the surviving lease must renew under the new epoch");

    booted.shutdown().await.unwrap();
}
