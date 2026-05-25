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
    #[error("failed to load TLS material from {path}: {source}")]
    Tls {
        path: std::path::PathBuf,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}
