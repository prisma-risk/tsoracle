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
async fn lease_port() -> std::net::SocketAddr {
    // Bind an ephemeral port to learn a free address, then release it so the
    // driver's peer listener can claim it. Same TOCTOU-tolerant trick the smoke
    // tests use.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("read local addr");
    drop(listener);
    addr
}

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
    use std::time::Duration;

    use tempfile::tempdir;
    use tokio_stream::StreamExt;
    use tsoracle_consensus::LeaderState;
    use tsoracle_standalone::{
        DriverConfig, MemberAddr, OpenraftConfig, RaftTuning, StandaloneError, build,
    };

    use crate::lease_port;

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
        }
    }

    /// A single-node openraft node boots through `build()`, elects itself, and
    /// its drain step (graceful leadership handoff) runs to completion. With no
    /// other voter the handoff finds no target and falls through — but the
    /// leader-detection, metrics read, and target-selection path are all
    /// exercised, which the subprocess tests can't reach in this library.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn build_openraft_single_node_boots_and_drains() {
        let dir = tempdir().unwrap();
        let raft_addr = lease_port().await;
        let cfg = single_node_cfg(raft_addr, "127.0.0.1:1", dir.path().join("raft"));

        let mut node = build(DriverConfig::Openraft(cfg))
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
}

#[cfg(feature = "paxos")]
mod paxos_driver {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use tempfile::tempdir;
    use tsoracle_standalone::{DriverConfig, PaxosConfig, build};

    use crate::lease_port;

    /// A paxos node boots through `build()`: storage opens, the peer listener
    /// binds, and the lifecycle host starts. OmniPaxos refuses a single-node
    /// cluster, so the config names a second voter whose listener is never
    /// bound — `build_paxos` doesn't wait for quorum, and the unreachable peer
    /// is a normal transient during bring-up (the `PeerSink` retries). We don't
    /// drive consensus here (the paxos lifecycle is timing-flaky under
    /// coverage, see the test-suite notes); proving bootstrap wired everything
    /// up and that `shutdown()` stops the peer transport is enough.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn build_paxos_boots_and_shuts_down() {
        let dir = tempdir().unwrap();
        let peer_listen = lease_port().await;
        // A second voter that we deliberately never start, just to satisfy
        // OmniPaxos's >1-node requirement.
        let absent_peer = lease_port().await;
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
        };

        let mut node = build(DriverConfig::Paxos(cfg))
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
        let unused = lease_port().await;

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
        };
        assert!(matches!(
            build(DriverConfig::Paxos(cfg)).await,
            Err(tsoracle_standalone::StandaloneError::PeerBind { .. })
        ));
    }
}
