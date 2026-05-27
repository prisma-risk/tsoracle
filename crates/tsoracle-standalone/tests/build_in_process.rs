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

//! In-process bring-up of each driver through the public `build()` entry point.
//!
//! The bin's `tests/smoke.rs` and the example `sigterm` test exercise the same
//! paths, but they spawn the `tsoracle` binary as a *subprocess* — a separate,
//! uninstrumented process — so they prove behaviour without earning coverage
//! for this library. These tests stand a single-node node up *in process*
//! instead, so the bootstrap, transport-handle, dispatch, and (for openraft)
//! the leadership-handoff drain paths are measured.
//!
//! The node-to-node peer transports (`drivers/*/network.rs`) still need a live
//! multi-node cluster and stay covered by the kind e2e lane; they are excluded
//! from the coverage report in the Makefile.

#[cfg(any(feature = "openraft", feature = "paxos"))]
mod common;

#[cfg(feature = "file")]
mod file_driver {
    use tempfile::tempdir;
    use tsoracle_standalone::{DriverConfig, FileConfig, build};

    /// `build()` dispatches the `File` variant, the file driver carries a no-op
    /// transport and no drain step, and `shutdown()` is harmless.
    #[tokio::test]
    async fn build_file_dispatches_and_shuts_down() {
        let dir = tempdir().unwrap();
        let mut node = build(DriverConfig::File(FileConfig {
            state_dir: dir.path().join("state"),
        }))
        .await
        .expect("build file driver");

        assert_eq!(node.driver.load_high_water().await.unwrap(), 0);
        assert!(
            node.take_drain().is_none(),
            "file driver has no pre-shutdown drain step"
        );
        node.shutdown().await;
    }
}

#[cfg(feature = "openraft")]
mod openraft_driver {
    use std::collections::BTreeMap;

    use tempfile::tempdir;
    use tsoracle_standalone::{
        DriverConfig, MemberAddr, OpenraftConfig, RaftTuning, StandaloneError, build,
    };

    // The boot-and-drain test below is the only consumer of these imports +
    // `build_openraft_with_listeners` + `lease_port`. Gating them on
    // `test-support` keeps the `--no-default-features --features openraft`
    // build (config-error tests only) warning-clean.
    #[cfg(feature = "test-support")]
    use crate::common::lease_port;
    #[cfg(feature = "test-support")]
    use std::time::Duration;
    #[cfg(feature = "test-support")]
    use tokio_stream::StreamExt;
    #[cfg(feature = "test-support")]
    use tsoracle_consensus::LeaderState;
    #[cfg(feature = "test-support")]
    use tsoracle_standalone::build_openraft_with_listeners;

    fn single_node_cfg(
        raft_addr: std::net::SocketAddr,
        service_endpoint: &str,
        raft_dir: std::path::PathBuf,
    ) -> OpenraftConfig {
        let mut members = BTreeMap::new();
        members.insert(
            1,
            MemberAddr {
                raft_addr: raft_addr.to_string(),
                service_endpoint: service_endpoint.to_string(),
                admin_endpoint: "127.0.0.1:3".to_string(),
            },
        );
        OpenraftConfig {
            id: 1,
            raft_addr,
            raft_dir,
            bootstrap: true,
            initial_membership: Some(members),
            // Fast timers so the single voter elects itself promptly; the smoke
            // test already relies on single-node openraft electing within 15s.
            tuning: RaftTuning {
                heartbeat_ms: 50,
                election_min_ms: 150,
                election_max_ms: 300,
            },
            peer_tls: None,
            admin_listen: None,
            admin_tls: None,
            allow_insecure_peer: false,
        }
    }

    /// A single-node openraft node boots through `build()`, elects itself, and
    /// its drain step (graceful leadership handoff) runs to completion. With no
    /// other voter the handoff finds no target and falls through — but the
    /// leader-detection, metrics read, and target-selection path are all
    /// exercised, which the subprocess tests can't reach in this library.
    #[cfg(feature = "test-support")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn build_openraft_single_node_boots_and_drains() {
        let dir = tempdir().unwrap();
        let (raft_addr, raft_lease) = lease_port().await;
        let cfg = single_node_cfg(raft_addr, "127.0.0.1:1", dir.path().join("raft"));

        let mut node = build_openraft_with_listeners(cfg, raft_lease.into_listener(), None)
            .await
            .expect("build openraft driver");

        // Wait until the node has promoted itself to leader so the drain below
        // exercises the leader branch of the handoff, not just the early return.
        let mut events = node.driver.leadership_events();
        let elected = tokio::time::timeout(Duration::from_secs(15), async {
            while let Some(state) = events.next().await {
                if matches!(state, LeaderState::Leader { .. }) {
                    return;
                }
            }
        })
        .await;
        assert!(
            elected.is_ok(),
            "single-node openraft did not elect itself within the budget"
        );
        drop(events);

        // Leader-side linearized read: commits a no-op barrier through the log
        // and reads the state machine after it commits.
        assert_eq!(node.driver.load_high_water().await.unwrap(), 0);

        let drain = node.take_drain().expect("openraft driver has a drain step");
        drain.await;
        node.shutdown().await;
    }

    #[tokio::test]
    async fn bootstrap_without_membership_is_a_config_error() {
        let cfg = OpenraftConfig {
            id: 1,
            raft_addr: "127.0.0.1:0".parse().unwrap(),
            raft_dir: std::path::PathBuf::from("/this/path/must/not/be/touched"),
            bootstrap: true,
            initial_membership: None,
            tuning: RaftTuning::default(),
            peer_tls: None,
            admin_listen: None,
            admin_tls: None,
            allow_insecure_peer: false,
        };
        assert!(matches!(
            build(DriverConfig::Openraft(cfg)).await,
            Err(StandaloneError::Config(_))
        ));
    }

    #[tokio::test]
    async fn membership_without_bootstrap_is_a_config_error() {
        let mut members = BTreeMap::new();
        members.insert(
            1,
            MemberAddr {
                raft_addr: "127.0.0.1:1".into(),
                service_endpoint: "127.0.0.1:2".into(),
                admin_endpoint: "127.0.0.1:3".into(),
            },
        );
        let cfg = OpenraftConfig {
            id: 1,
            raft_addr: "127.0.0.1:0".parse().unwrap(),
            raft_dir: std::path::PathBuf::from("/this/path/must/not/be/touched"),
            bootstrap: false,
            initial_membership: Some(members),
            tuning: RaftTuning::default(),
            peer_tls: None,
            admin_listen: None,
            admin_tls: None,
            allow_insecure_peer: false,
        };
        assert!(matches!(
            build(DriverConfig::Openraft(cfg)).await,
            Err(StandaloneError::Config(_))
        ));
    }

    #[tokio::test]
    async fn bootstrap_membership_missing_self_is_a_config_error() {
        let mut members = BTreeMap::new();
        members.insert(
            2,
            MemberAddr {
                raft_addr: "127.0.0.1:1".into(),
                service_endpoint: "127.0.0.1:2".into(),
                admin_endpoint: "127.0.0.1:3".into(),
            },
        );
        let cfg = OpenraftConfig {
            id: 1,
            raft_addr: "127.0.0.1:0".parse().unwrap(),
            raft_dir: std::path::PathBuf::from("/this/path/must/not/be/touched"),
            bootstrap: true,
            initial_membership: Some(members),
            tuning: RaftTuning::default(),
            peer_tls: None,
            admin_listen: None,
            admin_tls: None,
            allow_insecure_peer: false,
        };
        assert!(matches!(
            build(DriverConfig::Openraft(cfg)).await,
            Err(StandaloneError::Config(_))
        ));
    }

    /// Storage and consensus build fine, but the peer listener can't claim an
    /// already-bound address — surfaced as `PeerBind` rather than a background
    /// log line (bind happens before the server task is spawned).
    #[tokio::test]
    async fn peer_bind_conflict_is_a_bind_error() {
        let dir = tempdir().unwrap();
        // Hold the address so the driver's bind collides with it.
        let squatter = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let taken = squatter.local_addr().unwrap();

        let cfg = single_node_cfg(taken, "127.0.0.1:1", dir.path().join("raft"));
        assert!(matches!(
            build(DriverConfig::Openraft(cfg)).await,
            Err(StandaloneError::PeerBind { .. })
        ));
    }

    /// The peer listener binds and the node boots, but the admin listener can't
    /// claim an already-bound address — surfaced as `AdminBind` (distinct from
    /// the peer listener's `PeerBind`, so the operator sees the right port),
    /// not a background log line.
    #[cfg(feature = "test-support")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn admin_bind_conflict_is_a_bind_error() {
        let dir = tempdir().unwrap();
        let (raft_addr, raft_lease) = lease_port().await;
        // Hold the admin address so the admin server's bind collides with it.
        // The squatter must STAY bound — that's the point of the test — so we
        // can't pass an `admin_listener` through the test-support seam; the
        // build's own `TcpListener::bind(taken)` is exactly what we want to
        // fail with `AddrInUse`.
        let squatter = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let taken = squatter.local_addr().unwrap();

        let mut cfg = single_node_cfg(raft_addr, "127.0.0.1:1", dir.path().join("raft"));
        cfg.admin_listen = Some(taken);
        assert!(matches!(
            build_openraft_with_listeners(cfg, raft_lease.into_listener(), None).await,
            Err(StandaloneError::AdminBind { .. })
        ));
    }

    #[tokio::test]
    async fn openraft_peer_insecure_routable_bind_rejected_at_build() {
        // Mirrors admin_insecure_routable_bind_rejected_at_build:
        // guard runs at the top of build_openraft, before open_rocksdb,
        // so raft_dir stays untouched.
        let raft_dir = tempdir().unwrap();
        let raft_dir_path = raft_dir.path().to_path_buf();
        let cfg = OpenraftConfig {
            id: 1,
            raft_addr: "0.0.0.0:0".parse().unwrap(),
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
            admin_listen: None,
            admin_tls: None,
            allow_insecure_peer: false,
        };

        let err = match build(DriverConfig::Openraft(cfg)).await {
            Ok(_) => panic!("expected PeerInsecureRoutable"),
            Err(e) => e,
        };
        assert!(
            matches!(
                err,
                tsoracle_standalone::StandaloneError::PeerInsecureRoutable { .. }
            ),
            "got {err:?}"
        );
        let entries = std::fs::read_dir(&raft_dir_path)
            .expect("raft_dir is the tempdir, still present")
            .count();
        assert_eq!(
            entries, 0,
            "guard must run before open_rocksdb; raft_dir should be untouched"
        );
    }

    #[tokio::test]
    async fn openraft_peer_loopback_no_tls_allowed() {
        // Loopback binds are allowed plaintext for local-dev / sidecar.
        let dir = tempdir().unwrap();
        let (raft_addr, raft_lease) = lease_port().await;
        let cfg = single_node_cfg(raft_addr, "127.0.0.1:1", dir.path().join("raft"));
        assert!(raft_addr.ip().is_loopback(), "lease_port returns loopback");
        // peer_tls None, allow_insecure_peer false — loopback carve-out wins.
        drop(raft_lease);
        let node = build(DriverConfig::Openraft(cfg))
            .await
            .expect("loopback + no peer_tls should build");
        // Don't bother driving consensus — boot was the assertion.
        node.shutdown().await;
    }

    #[tokio::test]
    async fn openraft_peer_routable_opt_out_allowed() {
        // allow_insecure_peer=true opts the routable bind in;
        // build() must succeed past the guard. We don't drive consensus
        // (would need a real listener-bind on 0.0.0.0); proving the guard
        // is bypassed is the assertion, so the test uses 0.0.0.0:0 and
        // expects either Ok() or a downstream error other than
        // PeerInsecureRoutable.
        let dir = tempdir().unwrap();
        let raft_dir_path = dir.path().join("raft");
        let cfg = OpenraftConfig {
            id: 1,
            raft_addr: "0.0.0.0:0".parse().unwrap(),
            raft_dir: raft_dir_path,
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
            admin_listen: None,
            admin_tls: None,
            allow_insecure_peer: true,
        };
        match build(DriverConfig::Openraft(cfg)).await {
            Ok(node) => node.shutdown().await,
            Err(tsoracle_standalone::StandaloneError::PeerInsecureRoutable { .. }) => {
                panic!("opt-out should bypass the guard")
            }
            Err(_) => {
                // Any other error is acceptable for this guard-bypass test
                // (we only care that PeerInsecureRoutable did not fire).
            }
        }
    }

    #[tokio::test]
    async fn openraft_peer_routable_with_tls_allowed() {
        // peer_tls Some(...) bypasses the guard regardless of bind address.
        use tsoracle_standalone::PeerTlsConfig;
        let dir = tempdir().unwrap();
        let (cert, key, ca) = crate::common::write_peer_pems(dir.path());
        let cfg = OpenraftConfig {
            id: 1,
            raft_addr: "0.0.0.0:0".parse().unwrap(),
            raft_dir: dir.path().join("raft"),
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
            peer_tls: Some(PeerTlsConfig { cert, key, ca }),
            admin_listen: None,
            admin_tls: None,
            allow_insecure_peer: false,
        };
        match build(DriverConfig::Openraft(cfg)).await {
            Ok(node) => node.shutdown().await,
            Err(tsoracle_standalone::StandaloneError::PeerInsecureRoutable { .. }) => {
                panic!("routable + peer_tls should bypass the guard")
            }
            Err(_) => {} // any other error is fine for guard-bypass
        }
    }
}

#[cfg(feature = "paxos")]
mod paxos_driver {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use tempfile::tempdir;
    use tsoracle_standalone::{DriverConfig, PaxosConfig, build};
    // Race-free seam for the boot test; absent under test-support disabled,
    // and the `peer_bind_conflict` test below doesn't need it anyway.
    #[cfg(feature = "test-support")]
    use tsoracle_standalone::build_paxos_with_listeners;

    use crate::common::lease_port;

    /// A paxos node boots through `build()`: storage opens, the peer listener
    /// binds, and the lifecycle host starts. OmniPaxos refuses a single-node
    /// cluster, so the config names a second voter whose listener is never
    /// bound — `build_paxos` doesn't wait for quorum, and the unreachable peer
    /// is a normal transient during bring-up (the `PeerSink` retries). We don't
    /// drive consensus here (the paxos lifecycle is timing-flaky under
    /// coverage, see the test-suite notes); proving bootstrap wired everything
    /// up and that `shutdown()` stops the peer transport is enough.
    #[cfg(feature = "test-support")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn build_paxos_boots_and_shuts_down() {
        let dir = tempdir().unwrap();
        let (peer_listen, peer_lease) = lease_port().await;
        // A second voter that we deliberately never start, just to satisfy
        // OmniPaxos's >1-node requirement. `absent_lease` is dropped because
        // the build never actually binds this peer — the lease just stops
        // another test from snatching the port we're about to advertise.
        let (absent_peer, absent_lease) = lease_port().await;
        let mut peers = BTreeMap::new();
        peers.insert(1, peer_listen.to_string());
        peers.insert(2, absent_peer.to_string());
        // A redirect endpoint for the *other* node exercises the TsoPeer wiring
        // (the self entry, if any, is filtered out).
        let mut tso_peers = BTreeMap::new();
        tso_peers.insert(1, "127.0.0.1:9001".to_string());
        tso_peers.insert(2, "127.0.0.1:9002".to_string());

        let cfg = PaxosConfig {
            node_id: 1,
            peer_listen,
            peers,
            tso_peers,
            data_dir: dir.path().join("paxos"),
            tick_interval: Duration::from_millis(20),
            peer_tls: None,
            allow_insecure_peer: false,
        };

        drop(absent_lease);
        let mut node = build_paxos_with_listeners(cfg, peer_lease.into_listener())
            .await
            .expect("build paxos driver");
        assert!(
            node.take_drain().is_none(),
            "paxos driver has no pre-shutdown drain step"
        );
        node.shutdown().await;
    }

    /// A node absent from its own peer map can never be elected, so `build`
    /// rejects it before touching storage.
    #[tokio::test]
    async fn node_absent_from_peers_is_a_config_error() {
        let mut peers = BTreeMap::new();
        peers.insert(2, "127.0.0.1:1".to_string());
        let cfg = PaxosConfig {
            node_id: 1,
            peer_listen: "127.0.0.1:0".parse().unwrap(),
            peers,
            tso_peers: BTreeMap::new(),
            data_dir: std::path::PathBuf::from("/this/path/must/not/be/touched"),
            tick_interval: Duration::from_millis(20),
            peer_tls: None,
            allow_insecure_peer: false,
        };
        assert!(matches!(
            build(DriverConfig::Paxos(cfg)).await,
            Err(tsoracle_standalone::StandaloneError::Config(_))
        ));
    }

    /// Storage opens and OmniPaxos builds, but the peer listener can't claim an
    /// already-bound address.
    #[tokio::test]
    async fn peer_bind_conflict_is_a_bind_error() {
        let dir = tempdir().unwrap();
        let squatter = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let taken = squatter.local_addr().unwrap();
        let (unused, unused_lease) = lease_port().await;

        let mut peers = BTreeMap::new();
        peers.insert(1, taken.to_string());
        peers.insert(2, unused.to_string());
        let cfg = PaxosConfig {
            node_id: 1,
            peer_listen: taken,
            peers,
            tso_peers: BTreeMap::new(),
            data_dir: dir.path().join("paxos"),
            tick_interval: Duration::from_millis(20),
            peer_tls: None,
            allow_insecure_peer: false,
        };
        drop(unused_lease);
        assert!(matches!(
            build(DriverConfig::Paxos(cfg)).await,
            Err(tsoracle_standalone::StandaloneError::PeerBind { .. })
        ));
    }

    #[tokio::test]
    async fn paxos_peer_insecure_routable_bind_rejected_at_build() {
        // Mirrors openraft_peer_insecure_routable_bind_rejected_at_build:
        // guard runs at the top of build_paxos, before open_rocksdb,
        // so data_dir stays untouched.
        let data_dir = tempdir().unwrap();
        let data_dir_path = data_dir.path().to_path_buf();
        let mut peers = BTreeMap::new();
        peers.insert(1, "0.0.0.0:1".to_string());
        peers.insert(2, "127.0.0.1:2".to_string());
        let cfg = PaxosConfig {
            node_id: 1,
            peer_listen: "0.0.0.0:0".parse().unwrap(),
            peers,
            tso_peers: BTreeMap::new(),
            data_dir: data_dir_path.clone(),
            tick_interval: Duration::from_millis(20),
            peer_tls: None,
            allow_insecure_peer: false,
        };

        let err = match build(DriverConfig::Paxos(cfg)).await {
            Ok(_) => panic!("expected PeerInsecureRoutable"),
            Err(e) => e,
        };
        assert!(
            matches!(
                err,
                tsoracle_standalone::StandaloneError::PeerInsecureRoutable { .. }
            ),
            "got {err:?}"
        );
        let entries = std::fs::read_dir(&data_dir_path)
            .expect("data_dir is the tempdir, still present")
            .count();
        assert_eq!(
            entries, 0,
            "guard must run before open_rocksdb; data_dir should be untouched"
        );
    }

    #[tokio::test]
    async fn paxos_peer_loopback_no_tls_allowed() {
        // Loopback binds are allowed plaintext for local-dev / sidecar.
        let dir = tempdir().unwrap();
        let (peer_listen, peer_lease) = lease_port().await;
        let (absent_peer, absent_lease) = lease_port().await;
        assert!(
            peer_listen.ip().is_loopback(),
            "lease_port returns loopback"
        );
        // peer_tls None, allow_insecure_peer false — loopback carve-out wins.
        let mut peers = BTreeMap::new();
        peers.insert(1, peer_listen.to_string());
        peers.insert(2, absent_peer.to_string());
        let cfg = PaxosConfig {
            node_id: 1,
            peer_listen,
            peers,
            tso_peers: BTreeMap::new(),
            data_dir: dir.path().join("paxos"),
            tick_interval: Duration::from_millis(20),
            peer_tls: None,
            allow_insecure_peer: false,
        };
        drop(peer_lease);
        drop(absent_lease);
        let node = build(DriverConfig::Paxos(cfg))
            .await
            .expect("loopback + no peer_tls should build");
        // Don't bother driving consensus — boot was the assertion.
        node.shutdown().await;
    }

    #[tokio::test]
    async fn paxos_peer_routable_opt_out_allowed() {
        // allow_insecure_peer=true opts the routable bind in;
        // build() must succeed past the guard. We don't drive consensus
        // (would need a real listener-bind on 0.0.0.0); proving the guard
        // is bypassed is the assertion, so the test uses 0.0.0.0:0 and
        // expects either Ok() or a downstream error other than
        // PeerInsecureRoutable.
        let dir = tempdir().unwrap();
        let mut peers = BTreeMap::new();
        peers.insert(1, "0.0.0.0:0".to_string());
        peers.insert(2, "127.0.0.1:2".to_string());
        let cfg = PaxosConfig {
            node_id: 1,
            peer_listen: "0.0.0.0:0".parse().unwrap(),
            peers,
            tso_peers: BTreeMap::new(),
            data_dir: dir.path().join("paxos"),
            tick_interval: Duration::from_millis(20),
            peer_tls: None,
            allow_insecure_peer: true,
        };
        match build(DriverConfig::Paxos(cfg)).await {
            Ok(node) => node.shutdown().await,
            Err(tsoracle_standalone::StandaloneError::PeerInsecureRoutable { .. }) => {
                panic!("opt-out should bypass the guard")
            }
            Err(_) => {
                // Any other error is acceptable for this guard-bypass test
                // (we only care that PeerInsecureRoutable did not fire).
            }
        }
    }

    #[tokio::test]
    async fn paxos_peer_routable_with_tls_allowed() {
        // peer_tls Some(...) bypasses the guard regardless of bind address.
        use tsoracle_standalone::PeerTlsConfig;
        let dir = tempdir().unwrap();
        let (cert, key, ca) = crate::common::write_peer_pems(dir.path());
        let mut peers = BTreeMap::new();
        peers.insert(1, "0.0.0.0:0".to_string());
        peers.insert(2, "127.0.0.1:2".to_string());
        let cfg = PaxosConfig {
            node_id: 1,
            peer_listen: "0.0.0.0:0".parse().unwrap(),
            peers,
            tso_peers: BTreeMap::new(),
            data_dir: dir.path().join("paxos"),
            tick_interval: Duration::from_millis(20),
            peer_tls: Some(PeerTlsConfig { cert, key, ca }),
            allow_insecure_peer: false,
        };
        match build(DriverConfig::Paxos(cfg)).await {
            Ok(node) => node.shutdown().await,
            Err(tsoracle_standalone::StandaloneError::PeerInsecureRoutable { .. }) => {
                panic!("routable + peer_tls should bypass the guard")
            }
            Err(_) => {
                // Any other error is acceptable for this guard-bypass test
                // (we only care that PeerInsecureRoutable did not fire).
            }
        }
    }
}
