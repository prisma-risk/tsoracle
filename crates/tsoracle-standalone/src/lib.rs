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

//! Driver selection, configuration, and peer transport for a standalone
//! tsoracle node. See `build`.
#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used))]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tsoracle_consensus::ConsensusDriver;

mod config;
pub use config::{
    DriverConfig, FileConfig, MemberAddr, OpenraftConfig, PaxosConfig, PeerTlsConfig, RaftTuning,
    parse_peer_map,
};

mod drivers;

#[cfg(feature = "file")]
pub use drivers::file::init_file_seeded;

mod error;
pub use error::StandaloneError;

mod transport;
pub use transport::TransportHandle;

#[cfg(any(feature = "openraft", feature = "paxos"))]
pub(crate) mod peer_tls;

/// A constructed, running standalone node: the consensus driver plus the
/// background peer-transport task (if any). The caller (the bin) owns the
/// client-facing `tsoracle_server::Server`; this type owns only the driver
/// and peer transport.
pub struct Standalone {
    pub driver: Arc<dyn ConsensusDriver>,
    transport: TransportHandle,
    /// Driver-specific action run when the shutdown signal fires, BEFORE the
    /// client server stops accepting (openraft: graceful leadership handoff;
    /// file/paxos: none). Lazy — it reads live state when awaited at shutdown.
    drain: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl Standalone {
    /// Take the pre-shutdown drain action, if the driver has one. Await the
    /// returned future when the shutdown signal fires (before stopping the
    /// client gRPC server), then call [`Standalone::shutdown`] for the peer
    /// transport. `None` for drivers without a drain step (file, paxos).
    pub fn take_drain(&mut self) -> Option<Pin<Box<dyn Future<Output = ()> + Send>>> {
        self.drain.take()
    }

    /// Cooperatively stop the peer transport. Call off the same shutdown
    /// signal that stops the client gRPC server.
    pub async fn shutdown(mut self) {
        self.transport.shutdown().await;
    }
}

/// Open storage, construct the selected driver, and spawn its peer transport
/// (binding the peer listener before returning, so a bind failure is a
/// startup error rather than a background log line).
pub async fn build(cfg: DriverConfig) -> Result<Standalone, StandaloneError> {
    match cfg {
        #[cfg(feature = "file")]
        DriverConfig::File(c) => drivers::file::build_file(c),
        #[cfg(feature = "openraft")]
        DriverConfig::Openraft(c) => drivers::openraft::build_openraft(c).await,
        #[cfg(feature = "paxos")]
        DriverConfig::Paxos(c) => drivers::paxos::build_paxos(c).await,
    }
}
