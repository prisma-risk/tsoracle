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

//! Embed the tsoracle server in your own binary with the file driver and
//! graceful Ctrl-C shutdown.
//!
//! Run: `cargo run -p example-embedded-server`
//! Then talk to it on http://127.0.0.1:50551. Ctrl-C exits cleanly.

use std::net::SocketAddr;
use std::path::PathBuf;
use tsoracle_driver_file::FileDriver;
use tsoracle_server::Server;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let dir = PathBuf::from("./tsoracle-embedded-data");
    let driver = FileDriver::open_or_init(&dir)?;

    let addr: SocketAddr = "127.0.0.1:50551".parse()?;
    let server = Server::builder().consensus_driver(driver).build()?;

    let shutdown = async {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("ctrl-c received, draining");
    };

    println!("Embedded tsoracle on http://{addr} — press Ctrl-C to shut down");
    server.serve_with_shutdown(addr, shutdown).await?;
    Ok(())
}
