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
    #[error("failed to bind admin server on {addr}: {source}")]
    AdminBind {
        addr: SocketAddr,
        #[source]
        source: std::io::Error,
    },
    #[error(
        "admin listener on {addr} requires --admin-tls-{{cert,key,ca}} \
         (only loopback addresses may bind without admin TLS)"
    )]
    AdminInsecureRoutable { addr: SocketAddr },
    #[error(
        "peer listener on {addr} requires --peer-tls-{{cert,key,ca}} or \
         --allow-insecure-peer (only loopback addresses may bind without peer TLS)"
    )]
    PeerInsecureRoutable { addr: SocketAddr },
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
