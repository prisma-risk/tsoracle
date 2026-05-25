//! Driver selection, configuration, and peer transport for a standalone
//! tsoracle node. See `build`.
#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used))]

mod config;
pub use config::{
    DriverConfig, FileConfig, MemberAddr, OpenraftConfig, PaxosConfig, RaftTuning, parse_peer_map,
};

mod error;
pub use error::StandaloneError;

mod transport;
pub use transport::TransportHandle;
