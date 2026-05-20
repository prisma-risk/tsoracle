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
