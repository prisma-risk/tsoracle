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

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures::Stream;
use tonic::Code;
use tsoracle_consensus::{ConsensusDriver, ConsensusError, LeaderState};
use tsoracle_core::{Epoch, LeaseRecord, PeerEndpoint};
use tsoracle_proto::v1::{
    AcquireLeaseRequest, GetTsRequest, LeaderHintLookup, ReleaseLeaseRequest, RenewLeaseRequest,
    decode_leader_hint, tso_service_client::TsoServiceClient,
};
use tsoracle_server::Server;
use tsoracle_server::test_fakes::{InMemoryDriver, MockClock};
use tsoracle_server::test_support::{
    boot_leader_server, wait_until_not_serving, wait_until_serving,
};

const START_MS: u64 = 1_000_000;
const TTL_MS: u64 = 10_000;

async fn boot_leader(
    driver: Arc<InMemoryDriver>,
    clock: Arc<MockClock>,
    epoch: Epoch,
) -> (
    tsoracle_server::test_support::BootedServer,
    TsoServiceClient<tonic::transport::Channel>,
) {
    let server = Server::builder()
        .consensus_driver(driver.clone())
        .clock(clock)
        .window_ahead(Duration::from_millis(500))
        .failover_advance(Duration::from_millis(200))
        .build()
        .unwrap();
    boot_leader_server(server, || driver.become_leader(epoch)).await
}

fn acquire_req(holder: &[u8], holder_epoch: u64, ttl_ms: u64) -> AcquireLeaseRequest {
    AcquireLeaseRequest {
        holder: holder.to_vec(),
        holder_epoch,
        ttl_ms,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acquire_grants_durable_lease_before_returning() {
    let driver = Arc::new(InMemoryDriver::new());
    let clock = Arc::new(MockClock::new(START_MS));
    let (booted, mut client) = boot_leader(driver.clone(), clock, Epoch(1)).await;

    let resp = client
        .acquire_lease(acquire_req(b"group-a", 1, TTL_MS))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.expires_at_ms, START_MS + TTL_MS);
    assert_eq!(resp.ts_upper_bound, driver.current_high_water());
    assert_eq!(
        resp.epoch.unwrap(),
        tsoracle_proto::v1::EpochWire { hi: 0, lo: 1 }
    );
    let leases = driver.current_leases();
    assert_eq!(leases.len(), 1);
    assert_eq!(leases[0].lease_id, resp.lease_id);
    assert!(!leases[0].superseded);

    booted.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lease_bound_and_direct_windows_share_one_authority() {
    let driver = Arc::new(InMemoryDriver::new());
    let clock = Arc::new(MockClock::new(START_MS));
    let (booted, mut client) = boot_leader(driver.clone(), clock.clone(), Epoch(1)).await;

    let lease = client
        .acquire_lease(acquire_req(b"group-a", 1, TTL_MS))
        .await
        .unwrap()
        .into_inner();
    let bound = lease.ts_upper_bound;
    assert_eq!(driver.current_high_water(), bound);

    client.get_ts(GetTsRequest { count: 1 }).await.unwrap();
    assert_eq!(driver.current_high_water(), bound);

    clock.set(bound + 1_000);
    let ts = client
        .get_ts(GetTsRequest { count: 1 })
        .await
        .unwrap()
        .into_inner();
    assert!(ts.physical_ms > bound);
    assert!(driver.current_high_water() > bound);
    assert_eq!(driver.current_leases()[0].ts_upper_bound, bound);

    booted.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idempotent_supersede_renew_release_and_ttl_errors() {
    let driver = Arc::new(InMemoryDriver::new());
    let clock = Arc::new(MockClock::new(START_MS));
    let (booted, mut client) = boot_leader(driver.clone(), clock.clone(), Epoch(1)).await;

    let first = client
        .acquire_lease(acquire_req(b"group-a", 1, TTL_MS))
        .await
        .unwrap()
        .into_inner();
    let repeat = client
        .acquire_lease(acquire_req(b"group-a", 1, TTL_MS))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(repeat.lease_id, first.lease_id);
    assert_eq!(repeat.ts_upper_bound, first.ts_upper_bound);

    let second = client
        .acquire_lease(acquire_req(b"group-a", 2, TTL_MS))
        .await
        .unwrap()
        .into_inner();
    assert_ne!(second.lease_id, first.lease_id);
    let leases = driver.current_leases();
    assert!(
        leases
            .iter()
            .any(|r| r.lease_id == first.lease_id && r.superseded)
    );
    assert_eq!(
        client
            .renew_lease(RenewLeaseRequest {
                lease_id: first.lease_id
            })
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    assert_eq!(
        client
            .acquire_lease(acquire_req(b"group-a", 1, TTL_MS))
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );

    clock.advance(3_000);
    let renewed = client
        .renew_lease(RenewLeaseRequest {
            lease_id: second.lease_id,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(renewed.ts_upper_bound > second.ts_upper_bound);
    assert_eq!(renewed.expires_at_ms, START_MS + 3_000 + TTL_MS);
    assert_eq!(
        client
            .renew_lease(RenewLeaseRequest { lease_id: 999 })
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );

    client
        .release_lease(ReleaseLeaseRequest {
            lease_id: second.lease_id,
        })
        .await
        .unwrap();
    client
        .release_lease(ReleaseLeaseRequest {
            lease_id: second.lease_id,
        })
        .await
        .unwrap();
    client
        .release_lease(ReleaseLeaseRequest { lease_id: 999 })
        .await
        .unwrap();

    assert_eq!(
        client
            .acquire_lease(acquire_req(b"group-b", 1, 4_999))
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    assert_eq!(
        client
            .acquire_lease(acquire_req(b"group-b", 1, 300_001))
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    assert_eq!(
        client
            .acquire_lease(acquire_req(b"", 1, TTL_MS))
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );

    booted.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expired_lease_cannot_renew_and_reacquire_gets_fresh_grant() {
    let driver = Arc::new(InMemoryDriver::new());
    let clock = Arc::new(MockClock::new(START_MS));
    let (booted, mut client) = boot_leader(driver, clock.clone(), Epoch(1)).await;

    let first = client
        .acquire_lease(acquire_req(b"group-a", 1, TTL_MS))
        .await
        .unwrap()
        .into_inner();
    clock.advance(TTL_MS);
    assert_eq!(
        client
            .renew_lease(RenewLeaseRequest {
                lease_id: first.lease_id
            })
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    let fresh = client
        .acquire_lease(acquire_req(b"group-a", 1, TTL_MS))
        .await
        .unwrap()
        .into_inner();
    assert_ne!(fresh.lease_id, first.lease_id);

    booted.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lease_rpcs_are_leader_only_with_hint() {
    let driver = Arc::new(InMemoryDriver::new());
    let clock = Arc::new(MockClock::new(START_MS));
    let (mut booted, mut client) = boot_leader(driver.clone(), clock, Epoch(1)).await;

    driver.become_follower(Some(PeerEndpoint::try_from("10.9.8.7:50551").unwrap()));
    wait_until_not_serving(&mut booted.state_rx).await;

    let acquire = client
        .acquire_lease(acquire_req(b"group-a", 1, TTL_MS))
        .await
        .unwrap_err();
    assert_eq!(acquire.code(), Code::FailedPrecondition);
    assert!(matches!(
        decode_leader_hint(&acquire),
        LeaderHintLookup::Decoded(_)
    ));

    let renew = client
        .renew_lease(RenewLeaseRequest { lease_id: 1 })
        .await
        .unwrap_err();
    assert_eq!(renew.code(), Code::FailedPrecondition);
    assert!(matches!(
        decode_leader_hint(&renew),
        LeaderHintLookup::Decoded(_)
    ));

    let release = client
        .release_lease(ReleaseLeaseRequest { lease_id: 1 })
        .await
        .unwrap_err();
    assert_eq!(release.code(), Code::FailedPrecondition);
    assert!(matches!(
        decode_leader_hint(&release),
        LeaderHintLookup::Decoded(_)
    ));

    booted.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leases_survive_failover() {
    let driver = Arc::new(InMemoryDriver::new());
    let clock = Arc::new(MockClock::new(START_MS));
    let (mut booted, mut client) = boot_leader(driver.clone(), clock, Epoch(1)).await;

    let lease = client
        .acquire_lease(acquire_req(b"group-a", 1, TTL_MS))
        .await
        .unwrap()
        .into_inner();
    let pre_failover_high_water = driver.current_high_water();

    driver.become_follower(None);
    wait_until_not_serving(&mut booted.state_rx).await;
    driver.become_leader(Epoch(2));
    wait_until_serving(&mut booted.state_rx).await;

    let renewed = client
        .renew_lease(RenewLeaseRequest {
            lease_id: lease.lease_id,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(renewed.ts_upper_bound > pre_failover_high_water);

    booted.shutdown().await.unwrap();
}

#[derive(Clone)]
struct FailingLeaseDriver {
    inner: InMemoryDriver,
    fail_leases: Arc<AtomicBool>,
}

impl FailingLeaseDriver {
    fn new() -> Self {
        Self {
            inner: InMemoryDriver::new(),
            fail_leases: Arc::new(AtomicBool::new(false)),
        }
    }

    fn become_leader(&self, epoch: Epoch) {
        self.inner.become_leader(epoch);
    }

    fn fail_leases(&self, fail: bool) {
        self.fail_leases.store(fail, Ordering::SeqCst);
    }
}

#[async_trait::async_trait]
impl ConsensusDriver for FailingLeaseDriver {
    fn leadership_events(&self) -> Pin<Box<dyn Stream<Item = LeaderState> + Send>> {
        self.inner.leadership_events()
    }

    async fn load_high_water(&self) -> Result<u64, ConsensusError> {
        self.inner.load_high_water().await
    }

    async fn persist_high_water(&self, at_least: u64, epoch: Epoch) -> Result<u64, ConsensusError> {
        self.inner.persist_high_water(at_least, epoch).await
    }

    async fn load_leases(&self) -> Result<Vec<LeaseRecord>, ConsensusError> {
        self.inner.load_leases().await
    }

    async fn persist_leases(
        &self,
        live: &[LeaseRecord],
        epoch: Epoch,
    ) -> Result<(), ConsensusError> {
        if self.fail_leases.load(Ordering::SeqCst) {
            return Err(ConsensusError::TransientDriver(Box::new(
                std::io::Error::other("injected lease persist failure"),
            )));
        }
        self.inner.persist_leases(live, epoch).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_lease_persist_fails_the_rpc_without_committing() {
    let driver = Arc::new(FailingLeaseDriver::new());
    let clock = Arc::new(MockClock::new(START_MS));
    let server = Server::builder()
        .consensus_driver(driver.clone())
        .clock(clock)
        .window_ahead(Duration::from_millis(500))
        .failover_advance(Duration::from_millis(200))
        .build()
        .unwrap();
    let (booted, mut client) = boot_leader_server(server, || driver.become_leader(Epoch(1))).await;

    driver.fail_leases(true);
    assert_eq!(
        client
            .acquire_lease(acquire_req(b"group-a", 1, TTL_MS))
            .await
            .unwrap_err()
            .code(),
        Code::Unavailable
    );
    driver.fail_leases(false);
    assert_eq!(
        client
            .renew_lease(RenewLeaseRequest { lease_id: 1 })
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    client
        .acquire_lease(acquire_req(b"group-a", 1, TTL_MS))
        .await
        .unwrap();

    booted.shutdown().await.unwrap();
}
