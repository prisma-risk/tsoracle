use std::net::SocketAddr;
use std::path::PathBuf;

/// Failure modes when bootstrapping a standalone node.
#[derive(Debug, thiserror::Error)]
pub enum StandaloneError {
    #[error("failed to open storage at {path}: {source}")]
    Storage {
        path: PathBuf,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("failed to bind peer transport on {addr}: {source}")]
    PeerBind {
        addr: SocketAddr,
        #[source]
        source: std::io::Error,
    },
    #[error("driver bootstrap failed: {0}")]
    Bootstrap(Box<dyn std::error::Error + Send + Sync>),
    #[error("invalid configuration: {0}")]
    Config(String),
    #[error("peer-transport TLS is configured but not yet supported (sub-project 2)")]
    TlsUnsupported,
}
