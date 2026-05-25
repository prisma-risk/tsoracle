//! Driver selection, configuration, and peer transport for a standalone
//! tsoracle node. See `build`.
#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used))]

use std::sync::Arc;

use tsoracle_consensus::ConsensusDriver;

mod config;
pub use config::{
    DriverConfig, FileConfig, MemberAddr, OpenraftConfig, PaxosConfig, RaftTuning, parse_peer_map,
};

mod drivers;

mod error;
pub use error::StandaloneError;

mod transport;
pub use transport::TransportHandle;

/// A constructed, running standalone node: the consensus driver plus the
/// background peer-transport task (if any). The caller (the bin) owns the
/// client-facing `tsoracle_server::Server`; this type owns only the driver
/// and peer transport.
pub struct Standalone {
    pub driver: Arc<dyn ConsensusDriver>,
    transport: TransportHandle,
}

impl Standalone {
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
