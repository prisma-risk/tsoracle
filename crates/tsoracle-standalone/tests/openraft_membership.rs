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

#![cfg(all(feature = "openraft", feature = "test-support"))]

mod common;

use std::collections::BTreeMap;
use std::time::Duration;

use tempfile::tempdir;
use tokio_stream::StreamExt;
use tsoracle_consensus::LeaderState;
use tsoracle_standalone::{
    DriverConfig, MemberAddr, MemberRole, NewMember, OpenraftConfig, RaftTuning, build,
    build_openraft_with_listeners,
};

use common::lease_port;

#[test]
fn admin_proto_types_exist() {
    // Compiles only if the generated module is present and named as expected.
    let _ = tsoracle_standalone::admin_proto::ChangeResponse::default();
    let _ = tsoracle_standalone::admin_proto::MemberRole::Voter;
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
    let (raft1, lease1) = lease_port().await;
    let (raft2, lease2) = lease_port().await;

    let mut members = BTreeMap::new();
    members.insert(
        1,
        MemberAddr {
            raft_addr: raft1.to_string(),
            service_endpoint: "127.0.0.1:1".into(),
            admin_endpoint: "127.0.0.1:11".into(),
        },
    );
    let node1 = build_openraft_with_listeners(
        OpenraftConfig {
            id: 1,
            raft_addr: raft1,
            raft_dir: dir1.path().join("raft"),
            bootstrap: true,
            initial_membership: Some(members),
            tuning: fast_tuning(),
            peer_tls: None,
            admin_listen: None,
            admin_tls: None,
        },
        lease1.into_listener(),
        None,
    )
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

    let node2 = build_openraft_with_listeners(
        OpenraftConfig {
            id: 2,
            raft_addr: raft2,
            raft_dir: dir2.path().join("raft"),
            bootstrap: false,
            initial_membership: None,
            tuning: fast_tuning(),
            peer_tls: None,
            admin_listen: None,
            admin_tls: None,
        },
        lease2.into_listener(),
        None,
    )
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
    let (raft1, lease1) = lease_port().await;
    let (raft2, lease2) = lease_port().await;
    let (admin_addr, admin_lease) = lease_port().await;

    let mut members = BTreeMap::new();
    members.insert(
        1,
        MemberAddr {
            raft_addr: raft1.to_string(),
            service_endpoint: "127.0.0.1:1".into(),
            admin_endpoint: admin_addr.to_string(),
        },
    );
    let node1 = build_openraft_with_listeners(
        OpenraftConfig {
            id: 1,
            raft_addr: raft1,
            raft_dir: dir1.path().join("raft"),
            bootstrap: true,
            initial_membership: Some(members),
            tuning: fast_tuning(),
            peer_tls: None,
            admin_listen: Some(admin_addr),
            admin_tls: None,
        },
        lease1.into_listener(),
        Some(admin_lease.into_listener()),
    )
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

    let node2 = build_openraft_with_listeners(
        OpenraftConfig {
            id: 2,
            raft_addr: raft2,
            raft_dir: dir2.path().join("raft"),
            bootstrap: false,
            initial_membership: None,
            tuning: fast_tuning(),
            peer_tls: None,
            admin_listen: None,
            admin_tls: None,
        },
        lease2.into_listener(),
        None,
    )
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
    let (raft1, lease1) = lease_port().await;
    let (raft2, lease2) = lease_port().await;
    let (raft3, lease3) = lease_port().await;

    let mut members = BTreeMap::new();
    members.insert(
        1,
        MemberAddr {
            raft_addr: raft1.to_string(),
            service_endpoint: "127.0.0.1:1".into(),
            admin_endpoint: "127.0.0.1:11".into(),
        },
    );
    let node1 = build_openraft_with_listeners(
        OpenraftConfig {
            id: 1,
            raft_addr: raft1,
            raft_dir: dir1.path().join("raft"),
            bootstrap: true,
            initial_membership: Some(members),
            tuning: fast_tuning(),
            peer_tls: None,
            admin_listen: None,
            admin_tls: None,
        },
        lease1.into_listener(),
        None,
    )
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

    let build_follower = |id: u64, raft_addr, raft_dir, listener| {
        build_openraft_with_listeners(
            OpenraftConfig {
                id,
                raft_addr,
                raft_dir,
                bootstrap: false,
                initial_membership: None,
                tuning: fast_tuning(),
                peer_tls: None,
                admin_listen: None,
                admin_tls: None,
            },
            listener,
            None,
        )
    };
    let node2 = build_follower(2, raft2, dir2.path().join("raft"), lease2.into_listener())
        .await
        .expect("build node 2");
    let node3 = build_follower(3, raft3, dir3.path().join("raft"), lease3.into_listener())
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

#[tokio::test]
async fn admin_insecure_routable_bind_rejected_at_build() {
    // The guard runs at the top of build_openraft — before open_rocksdb,
    // Raft::new, peer bind/spawn, and initialize() — so neither the admin
    // port nor the raft port is touched and the raft_dir stays empty.
    let routable: std::net::SocketAddr = "0.0.0.0:0".parse().unwrap();

    let raft_dir = tempdir().unwrap();
    let raft_dir_path = raft_dir.path().to_path_buf();
    let cfg = OpenraftConfig {
        id: 1,
        raft_addr: "127.0.0.1:0".parse().unwrap(),
        raft_dir: raft_dir_path.clone(),
        bootstrap: true,
        initial_membership: Some(BTreeMap::from([(
            1u64,
            MemberAddr {
                raft_addr: "127.0.0.1:9".into(),
                service_endpoint: "127.0.0.1:8".into(),
                admin_endpoint: "127.0.0.1:7".into(),
            },
        )])),
        tuning: RaftTuning::default(),
        peer_tls: None,
        admin_listen: Some(routable),
        admin_tls: None,
    };

    let err = match build(DriverConfig::Openraft(cfg)).await {
        Ok(_) => panic!("expected AdminInsecureRoutable"),
        Err(e) => e,
    };
    assert!(
        matches!(
            err,
            tsoracle_standalone::StandaloneError::AdminInsecureRoutable { .. }
        ),
        "got {err:?}"
    );
    // Guard fires before create_dir_all/open_rocksdb; raft_dir is the
    // tempdir itself (pre-created), so assert it stays empty.
    let entries = std::fs::read_dir(&raft_dir_path)
        .expect("raft_dir is the tempdir, still present")
        .count();
    assert_eq!(
        entries, 0,
        "guard must run before open_rocksdb; raft_dir should be untouched"
    );
}

#[tokio::test]
async fn shared_ca_for_peer_and_admin_rejected_at_build() {
    use tsoracle_standalone::{AdminTlsConfig, PeerTlsConfig};

    let ca_dir = tempdir().unwrap();
    let ca_path = ca_dir.path().join("shared-ca.pem");
    // Realistic-shaped placeholder; the cross-trio check is bytes/path-based,
    // it does not parse the PEM. The build fails before any TLS parse
    // because the Config error is returned first.
    std::fs::write(
        &ca_path,
        b"-----BEGIN CERTIFICATE-----\nshared\n-----END CERTIFICATE-----\n",
    )
    .unwrap();
    let cert_path = ca_dir.path().join("node.crt");
    let key_path = ca_dir.path().join("node.key");
    std::fs::write(&cert_path, b"placeholder-cert").unwrap();
    std::fs::write(&key_path, b"placeholder-key").unwrap();

    let raft_dir = tempdir().unwrap();
    let cfg = OpenraftConfig {
        id: 1,
        raft_addr: "127.0.0.1:0".parse().unwrap(),
        raft_dir: raft_dir.path().to_path_buf(),
        bootstrap: true,
        initial_membership: Some(BTreeMap::from([(
            1u64,
            MemberAddr {
                raft_addr: "127.0.0.1:9".into(),
                service_endpoint: "127.0.0.1:8".into(),
                admin_endpoint: "127.0.0.1:7".into(),
            },
        )])),
        tuning: RaftTuning::default(),
        peer_tls: Some(PeerTlsConfig {
            cert: cert_path.clone(),
            key: key_path.clone(),
            ca: ca_path.clone(),
        }),
        admin_listen: Some("127.0.0.1:0".parse().unwrap()),
        admin_tls: Some(AdminTlsConfig {
            cert: cert_path,
            key: key_path,
            ca: ca_path,
        }),
    };

    let err = match build(DriverConfig::Openraft(cfg)).await {
        Ok(_) => panic!("expected Config rejection for shared CA"),
        Err(e) => e,
    };
    let msg = match err {
        tsoracle_standalone::StandaloneError::Config(m) => m,
        other => panic!("expected Config, got {other:?}"),
    };
    assert!(
        msg.contains("same CA") || msg.contains("distinct from the peer CA"),
        "got {msg:?}"
    );
}

/// Generate a self-signed CA + server leaf (SAN 127.0.0.1) + client leaf,
/// and return (`AdminTlsConfig` for the server, `ClientTlsConfig` ready to
/// dial it). Mirrors `peer_tls.rs`'s `write_node_certs` pattern.
fn write_admin_certs(
    dir: &std::path::Path,
) -> (
    tsoracle_standalone::AdminTlsConfig,
    tonic::transport::ClientTlsConfig,
) {
    use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
    use tonic::transport::{Certificate, ClientTlsConfig, Identity};

    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::new(vec!["tso-admin-ca".into()]).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();

    let server_key = KeyPair::generate().unwrap();
    let server_params = CertificateParams::new(vec!["127.0.0.1".into()]).unwrap();
    let server_cert = server_params
        .signed_by(&server_key, &ca_cert, &ca_key)
        .unwrap();

    let client_key = KeyPair::generate().unwrap();
    let client_params = CertificateParams::new(vec!["tso-admin-cli".into()]).unwrap();
    let client_cert = client_params
        .signed_by(&client_key, &ca_cert, &ca_key)
        .unwrap();

    let s_cert = dir.join("admin.crt");
    let s_key = dir.join("admin.key");
    let ca = dir.join("admin-ca.crt");
    std::fs::write(&s_cert, server_cert.pem()).unwrap();
    std::fs::write(&s_key, server_key.serialize_pem()).unwrap();
    std::fs::write(&ca, ca_cert.pem()).unwrap();

    let admin_tls = tsoracle_standalone::AdminTlsConfig {
        cert: s_cert,
        key: s_key,
        ca: ca.clone(),
    };
    let ca_pem = std::fs::read(&ca).unwrap();
    let client_tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(&ca_pem))
        .identity(Identity::from_pem(
            client_cert.pem(),
            client_key.serialize_pem(),
        ))
        .domain_name("127.0.0.1");
    (admin_tls, client_tls)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admin_mtls_listener_serves_with_matching_client_cert() {
    use tsoracle_standalone::admin_proto::ListMembersRequest;
    use tsoracle_standalone::admin_proto::membership_admin_client::MembershipAdminClient;

    let raft_dir = tempdir().unwrap();
    let cert_dir = tempdir().unwrap();
    let (admin_tls, client_tls) = write_admin_certs(cert_dir.path());

    let cfg = OpenraftConfig {
        id: 1,
        raft_addr: "127.0.0.1:0".parse().unwrap(),
        raft_dir: raft_dir.path().to_path_buf(),
        bootstrap: true,
        initial_membership: Some(BTreeMap::from([(
            1u64,
            MemberAddr {
                raft_addr: "127.0.0.1:9".into(),
                service_endpoint: "127.0.0.1:8".into(),
                admin_endpoint: "127.0.0.1:7".into(),
            },
        )])),
        tuning: fast_tuning(),
        peer_tls: None,
        admin_listen: Some("127.0.0.1:0".parse().unwrap()),
        admin_tls: Some(admin_tls),
    };

    let node = build(DriverConfig::Openraft(cfg))
        .await
        .expect("build with admin mTLS");

    // build() returns once raft is constructed and the listener is bound,
    // but election runs asynchronously — list_members().has_leader right
    // after build is a flake against fast_tuning's 150-300ms election
    // window. Mirror the wait pattern at line 90 above.
    let mut events = node.driver.leadership_events();
    tokio::time::timeout(Duration::from_secs(15), async {
        while let Some(state) = events.next().await {
            if matches!(state, LeaderState::Leader { .. }) {
                return;
            }
        }
    })
    .await
    .expect("single-node cluster elected");
    drop(events);

    let admin_addr = node.admin_listen_addr().expect("admin port bound");
    let endpoint = format!("https://{admin_addr}");

    let channel = tonic::transport::Channel::from_shared(endpoint)
        .unwrap()
        .tls_config(client_tls)
        .unwrap()
        .connect()
        .await
        .expect("client mTLS connect succeeds");
    let mut client = MembershipAdminClient::new(channel);
    let view = client
        .list_members(ListMembersRequest {})
        .await
        .expect("list_members ok")
        .into_inner();
    assert!(view.has_leader);
    assert_eq!(view.members.len(), 1);

    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admin_mtls_listener_rejects_no_client_cert() {
    use tsoracle_standalone::admin_proto::ListMembersRequest;
    use tsoracle_standalone::admin_proto::membership_admin_client::MembershipAdminClient;

    let raft_dir = tempdir().unwrap();
    let cert_dir = tempdir().unwrap();
    let (admin_tls, _client_tls) = write_admin_certs(cert_dir.path());

    // Server CA only — the test asserts the *client cert requirement*
    // rejects even when the channel CA matches and the TLS handshake
    // otherwise validates the server.
    let ca_pem = std::fs::read(&admin_tls.ca).unwrap();
    let no_cert_tls = tonic::transport::ClientTlsConfig::new()
        .ca_certificate(tonic::transport::Certificate::from_pem(&ca_pem))
        .domain_name("127.0.0.1");

    let cfg = OpenraftConfig {
        id: 1,
        raft_addr: "127.0.0.1:0".parse().unwrap(),
        raft_dir: raft_dir.path().to_path_buf(),
        bootstrap: true,
        initial_membership: Some(BTreeMap::from([(
            1u64,
            MemberAddr {
                raft_addr: "127.0.0.1:9".into(),
                service_endpoint: "127.0.0.1:8".into(),
                admin_endpoint: "127.0.0.1:7".into(),
            },
        )])),
        tuning: fast_tuning(),
        peer_tls: None,
        admin_listen: Some("127.0.0.1:0".parse().unwrap()),
        admin_tls: Some(admin_tls),
    };

    let node = build(DriverConfig::Openraft(cfg)).await.unwrap();
    let admin_addr = node.admin_listen_addr().expect("admin port bound");
    let endpoint = format!("https://{admin_addr}");

    let channel_result = tonic::transport::Channel::from_shared(endpoint)
        .unwrap()
        .tls_config(no_cert_tls)
        .unwrap()
        .connect()
        .await;

    // tonic+rustls may either fail at connect() (handshake closed) or
    // succeed and then fail on the first RPC when the server rejects a
    // missing client cert. Accept either — tonic 0.14 has no public
    // `From<io::Error>` on `transport::Error`, so don't unify the error
    // types; just assert no successful list_members ever returns.
    let rpc_succeeded = match channel_result {
        Err(_) => false,
        Ok(channel) => {
            let mut client = MembershipAdminClient::new(channel);
            client.list_members(ListMembersRequest {}).await.is_ok()
        }
    };
    assert!(
        !rpc_succeeded,
        "expected handshake or RPC failure with no client cert"
    );

    node.shutdown().await;
}
