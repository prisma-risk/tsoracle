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

//! Demonstrates request coalescing under load.
//!
//! Run a `tsoracle serve` in another terminal, then:
//! `cargo run --example client-burst -p tsoracle -- http://127.0.0.1:50551`

use std::sync::Arc;
use std::time::Instant;
use tsoracle_client::Client;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let endpoint = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "http://127.0.0.1:50551".to_string());
    let client = Arc::new(Client::connect(vec![endpoint]).await?);

    let start = Instant::now();
    let mut handles = Vec::new();
    for _ in 0..10_000 {
        let client = client.clone();
        handles.push(tokio::spawn(async move { client.get_ts().await }));
    }
    let mut total = 0usize;
    for handle in handles {
        if handle.await?.is_ok() {
            total += 1;
        }
    }
    let elapsed = start.elapsed();
    println!(
        "issued {total} timestamps in {:?} ({:.0}/s)",
        elapsed,
        total as f64 / elapsed.as_secs_f64()
    );
    Ok(())
}
