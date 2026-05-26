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

#![cfg(feature = "openraft")]

use std::collections::BTreeMap;
use std::time::Duration;

use tempfile::tempdir;
use tokio_stream::StreamExt;
use tsoracle_consensus::LeaderState;
use tsoracle_standalone::{
    DriverConfig, MemberAddr, MemberRole, NewMember, OpenraftConfig, RaftTuning, build,
};

#[test]
fn admin_proto_types_exist() {
    // Compiles only if the generated module is present and named as expected.
    let _ = tsoracle_standalone::admin_proto::ChangeResponse::default();
    let _ = tsoracle_standalone::admin_proto::MemberRole::Voter;
}

/// Bind an ephemeral port, read its address, then release it so the node can
/// rebind it on boot. TOCTOU-tolerant in practice (same trick the build and
/// smoke tests use): the window is tiny and the test owns the loopback range.
async fn lease_port() -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

fn fast_tuning() -> RaftTuning {
    RaftTuning {
        heartbeat_ms: 50,
        election_min_ms: 150,
        election_max_ms: 300,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn add_learner_promote_then_remove() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    let raft1 = lease_port().await;
    let raft2 = lease_port().await;

    let mut members = BTreeMap::new();
    members.insert(
        1,
        MemberAddr {
            raft_addr: raft1.to_string(),
            service_endpoint: "127.0.0.1:1".into(),
            admin_endpoint: "127.0.0.1:11".into(),
        },
    );
    let node1 = build(DriverConfig::Openraft(OpenraftConfig {
        id: 1,
        raft_addr: raft1,
        raft_dir: dir1.path().join("raft"),
        bootstrap: true,
        initial_membership: Some(members),
        tuning: fast_tuning(),
        peer_tls: None,
        admin_listen: None,
    }))
    .await
    .expect("build node 1");

    let mut events = node1.driver.leadership_events();
    tokio::time::timeout(Duration::from_secs(15), async {
        while let Some(state) = events.next().await {
            if matches!(state, LeaderState::Leader { .. }) {
                return;
            }
        }
    })
    .await
    .expect("node 1 elected");
    drop(events);

    let node2 = build(DriverConfig::Openraft(OpenraftConfig {
        id: 2,
        raft_addr: raft2,
        raft_dir: dir2.path().join("raft"),
        bootstrap: false,
        initial_membership: None,
        tuning: fast_tuning(),
        peer_tls: None,
        admin_listen: None,
    }))
    .await
    .expect("build node 2");

    node1
        .admin
        .add_learner(NewMember {
            id: 2,
            raft_addr: raft2.to_string(),
            service_endpoint: "127.0.0.1:2".into(),
            admin_endpoint: "127.0.0.1:22".into(),
        })
        .await
        .expect("add_learner");
    node1.admin.promote(2).await.expect("promote");

    let view = node1.admin.list_members().await.expect("list");
    assert_eq!(view.members.len(), 2, "two members after promote");
    assert!(
        view.members
            .iter()
            .all(|member| member.role == MemberRole::Voter),
        "both members are voters after promote"
    );
    assert_eq!(
        view.members
            .iter()
            .find(|member| member.id == 2)
            .unwrap()
            .admin_endpoint,
        "127.0.0.1:22",
        "admin_endpoint replicated through the membership log"
    );

    node1.admin.remove(2).await.expect("remove");

    // `remove` returns once change_membership commits; metrics update asynchronously
    // after the log entry is applied. Poll until the view reflects the removal.
    let removed = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let view = node1.admin.list_members().await.expect("list after remove");
            if view.members.iter().all(|member| member.id != 2) {
                return view;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("node 2 did not disappear from membership within the budget");
    assert_eq!(removed.members.len(), 1, "only node 1 remains");

    node2.shutdown().await;
    node1.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admin_grpc_list_and_add_learner() {
    use tsoracle_standalone::admin_proto::membership_admin_client::MembershipAdminClient;
    use tsoracle_standalone::admin_proto::{AddLearnerRequest, ListMembersRequest};

    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    let raft1 = lease_port().await;
    let raft2 = lease_port().await;
    let admin_addr = lease_port().await;

    let mut members = BTreeMap::new();
    members.insert(
        1,
        MemberAddr {
            raft_addr: raft1.to_string(),
            service_endpoint: "127.0.0.1:1".into(),
            admin_endpoint: admin_addr.to_string(),
        },
    );
    let node1 = build(DriverConfig::Openraft(OpenraftConfig {
        id: 1,
        raft_addr: raft1,
        raft_dir: dir1.path().join("raft"),
        bootstrap: true,
        initial_membership: Some(members),
        tuning: fast_tuning(),
        peer_tls: None,
        admin_listen: Some(admin_addr),
    }))
    .await
    .expect("build node 1");

    let mut events = node1.driver.leadership_events();
    tokio::time::timeout(Duration::from_secs(15), async {
        while let Some(state) = events.next().await {
            if matches!(state, LeaderState::Leader { .. }) {
                return;
            }
        }
    })
    .await
    .expect("elected");
    drop(events);

    let node2 = build(DriverConfig::Openraft(OpenraftConfig {
        id: 2,
        raft_addr: raft2,
        raft_dir: dir2.path().join("raft"),
        bootstrap: false,
        initial_membership: None,
        tuning: fast_tuning(),
        peer_tls: None,
        admin_listen: None,
    }))
    .await
    .expect("build node 2");

    let mut client = MembershipAdminClient::connect(format!("http://{admin_addr}"))
        .await
        .expect("connect admin");

    let view = client
        .list_members(ListMembersRequest {})
        .await
        .expect("list")
        .into_inner();
    assert_eq!(view.members.len(), 1);

    let resp = client
        .add_learner(AddLearnerRequest {
            id: 2,
            raft_addr: raft2.to_string(),
            service_endpoint: "127.0.0.1:2".into(),
            admin_endpoint: "127.0.0.1:22".into(),
        })
        .await
        .expect("add_learner rpc")
        .into_inner();
    assert!(resp.ok, "add_learner ok, got error kind {}", resp.error);

    let view = client
        .list_members(ListMembersRequest {})
        .await
        .expect("list2")
        .into_inner();
    assert_eq!(view.members.len(), 2);

    node2.shutdown().await;
    node1.shutdown().await;
}

/// Regression for the staged-rollout workflow: `promote` (and `remove`) rebuild
/// the voter set with openraft `change_membership(.., retain = false)`, which
/// deletes only *demoted voters* — a standing learner that is not part of the
/// change must survive. Stage two learners, promote one, and assert the other
/// is still in the membership as a learner.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn coexisting_learner_survives_a_promote() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    let dir3 = tempdir().unwrap();
    let raft1 = lease_port().await;
    let raft2 = lease_port().await;
    let raft3 = lease_port().await;

    let mut members = BTreeMap::new();
    members.insert(
        1,
        MemberAddr {
            raft_addr: raft1.to_string(),
            service_endpoint: "127.0.0.1:1".into(),
            admin_endpoint: "127.0.0.1:11".into(),
        },
    );
    let node1 = build(DriverConfig::Openraft(OpenraftConfig {
        id: 1,
        raft_addr: raft1,
        raft_dir: dir1.path().join("raft"),
        bootstrap: true,
        initial_membership: Some(members),
        tuning: fast_tuning(),
        peer_tls: None,
        admin_listen: None,
    }))
    .await
    .expect("build node 1");

    let mut events = node1.driver.leadership_events();
    tokio::time::timeout(Duration::from_secs(15), async {
        while let Some(state) = events.next().await {
            if matches!(state, LeaderState::Leader { .. }) {
                return;
            }
        }
    })
    .await
    .expect("node 1 elected");
    drop(events);

    let build_follower = |id: u64, raft_addr, raft_dir| {
        build(DriverConfig::Openraft(OpenraftConfig {
            id,
            raft_addr,
            raft_dir,
            bootstrap: false,
            initial_membership: None,
            tuning: fast_tuning(),
            peer_tls: None,
            admin_listen: None,
        }))
    };
    let node2 = build_follower(2, raft2, dir2.path().join("raft"))
        .await
        .expect("build node 2");
    let node3 = build_follower(3, raft3, dir3.path().join("raft"))
        .await
        .expect("build node 3");

    // Stage two learners.
    node1
        .admin
        .add_learner(NewMember {
            id: 2,
            raft_addr: raft2.to_string(),
            service_endpoint: "127.0.0.1:2".into(),
            admin_endpoint: "127.0.0.1:22".into(),
        })
        .await
        .expect("add learner 2");
    node1
        .admin
        .add_learner(NewMember {
            id: 3,
            raft_addr: raft3.to_string(),
            service_endpoint: "127.0.0.1:3".into(),
            admin_endpoint: "127.0.0.1:33".into(),
        })
        .await
        .expect("add learner 3");

    // Promote only node 2; node 3 must remain a learner, not be evicted.
    node1.admin.promote(2).await.expect("promote 2");

    let view = node1.admin.list_members().await.expect("list");
    assert_eq!(
        view.members.len(),
        3,
        "all three nodes remain in membership"
    );
    assert_eq!(
        view.members
            .iter()
            .find(|member| member.id == 2)
            .unwrap()
            .role,
        MemberRole::Voter,
        "node 2 was promoted to voter"
    );
    assert_eq!(
        view.members
            .iter()
            .find(|member| member.id == 3)
            .unwrap()
            .role,
        MemberRole::Learner,
        "node 3 stays a learner across node 2's promotion"
    );

    node3.shutdown().await;
    node2.shutdown().await;
    node1.shutdown().await;
}
