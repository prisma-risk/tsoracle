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

//! Embed the tsoracle server in your own binary with the file driver.
//!
//! Run: `cargo run --example single-node-server -p tsoracle`

use std::path::PathBuf;
use tsoracle_driver_file::FileDriver;
use tsoracle_server::Server;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let dir = PathBuf::from("./tsoracle-example-data");
    let driver = FileDriver::open_or_init(&dir)?;

    let server = Server::builder().consensus_driver(driver).build()?;

    let addr = "127.0.0.1:50551".parse().unwrap();
    println!("Embedded tsoracle on http://{addr}");
    server.serve(addr).await?;
    Ok(())
}
