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
