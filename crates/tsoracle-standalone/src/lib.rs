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

//! Driver selection, configuration, and peer transport for a standalone
//! tsoracle node. See `build`.
//!
//! [`Standalone::admin`] is the runtime membership-admin handle ([`MembershipAdmin`]): live for the openraft driver (add a learner, promote it to voter, remove a node, and list members — served over the `--admin-listen` gRPC port), and [`UnsupportedAdmin`] for the file and paxos drivers, which reject every mutating op.
#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used))]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tsoracle_consensus::ConsensusDriver;

mod config;
pub use config::{
    AdminTlsConfig, DriverConfig, FileConfig, MemberAddr, OpenraftConfig, PaxosConfig,
    PeerTlsConfig, RaftTuning, parse_peer_map,
};

mod drivers;

#[cfg(feature = "file")]
pub use drivers::file::init_file_seeded;

/// Test-only entry point that builds an openraft node from pre-bound listeners
/// instead of binding internally. See `drivers::openraft::build_openraft_with_listeners`.
#[cfg(all(any(test, feature = "test-support"), feature = "openraft"))]
pub use drivers::openraft::build_openraft_with_listeners;

/// Test-only entry point that builds a paxos node from a pre-bound peer
/// listener. See `drivers::paxos::build_paxos_with_listeners`.
#[cfg(all(any(test, feature = "test-support"), feature = "paxos"))]
pub use drivers::paxos::build_paxos_with_listeners;

mod error;
pub use error::StandaloneError;

/// Driver-agnostic membership-admin surface. Re-exported types at the crate
/// root preserve the original public surface; the module itself is also
/// public so the `test_support` sub-module is reachable when its feature is
/// enabled.
pub mod admin;
pub use admin::{
    AdminError, MemberEntry, MemberRole, MembershipAdmin, MembershipView, NewMember,
    UnsupportedAdmin,
};

/// Generated `tsoracle.admin.v1` gRPC types (client + server). Public so the
/// `tsoracle admin` CLI can use the client stub.
#[cfg(feature = "openraft")]
pub mod admin_proto {
    tonic::include_proto!("tsoracle.admin.v1");
}

mod transport;
pub use transport::{FatalSignal, TransportHandle};

#[cfg(any(feature = "openraft", feature = "paxos"))]
pub(crate) mod peer_tls;

#[cfg(feature = "openraft")]
pub(crate) mod admin_tls;

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
    /// Runtime membership administration for this driver.
    pub admin: Arc<dyn MembershipAdmin>,
    /// Peer transport for the membership-admin gRPC server (openraft only;
    /// `noop` for file/paxos in this sub-project).
    admin_transport: TransportHandle,
    /// Address the admin gRPC server actually bound to, after `:0` resolution.
    /// `None` when no admin listener was requested or for drivers that do not
    /// serve an admin surface (file, paxos).
    admin_listen_addr: Option<std::net::SocketAddr>,
    /// Fail-fast signal shared by the transport supervisors: trips when a
    /// peer or admin server task exits unexpectedly. Inert for the file
    /// driver, which spawns no servers.
    fatal: FatalSignal,
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
        self.admin_transport.shutdown().await;
        self.transport.shutdown().await;
    }

    /// Address the admin gRPC server actually bound to (resolves `:0` to the
    /// OS-picked port). `None` when no admin listener was requested or for
    /// drivers without an admin surface.
    pub fn admin_listen_addr(&self) -> Option<std::net::SocketAddr> {
        self.admin_listen_addr
    }

    /// Fail-fast signal: resolves if a peer or admin transport server task
    /// dies while the node is supposed to be healthy (issue #616's zombie
    /// mode). The bin selects on [`FatalSignal::tripped`] next to the OS
    /// shutdown signal and exits non-zero so an orchestrator restarts the
    /// node instead of leaving the consensus driver running with no peer
    /// transport.
    pub fn fatal_signal(&self) -> FatalSignal {
        self.fatal.clone()
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
