use example_openraft_piggyback::run_demo;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();
    let _outcome = run_demo().await?;
    println!("\nDemo completed successfully.");
    Ok(())
}
