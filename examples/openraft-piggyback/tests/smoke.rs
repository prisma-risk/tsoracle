//! End-to-end guard: piggyback envelope preserves the freshness invariant
//! across failover and isolates host KV state from the TSO field.

use std::time::Duration;

use tokio::time::timeout;

// Bring in the binary crate's library-shaped run_demo. `main.rs` defines the
// helper in the bin namespace, so we re-declare it here as a path import via
// the bin target's auto-detected lib alias. If the build complains, see the
// note below for the alternative wiring.
//
// `example-openraft-piggyback` exposes `run_demo` directly through its bin
// target, accessible from the `tests/` integration harness as a path module.
use example_openraft_piggyback::run_demo;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn piggyback_demo_preserves_freshness_invariant() {
    let outcome = timeout(Duration::from_secs(30), run_demo())
        .await
        .expect("demo finished within 30s")
        .expect("demo ran without error");

    assert!(
        outcome.post_failover_high_water > outcome.pre_failover_high_water,
        "post-failover high-water ({}) must exceed pre-failover ({})",
        outcome.post_failover_high_water,
        outcome.pre_failover_high_water,
    );
    assert!(
        outcome.post_failover_first_ts > outcome.pre_failover_last_ts,
        "post-failover timestamp ({:?}) must exceed last pre-failover timestamp ({:?})",
        outcome.post_failover_first_ts,
        outcome.pre_failover_last_ts,
    );
    assert!(
        outcome.high_water_unchanged_across_getts,
        "steady-state GetTs must not advance the durable high-water",
    );
    assert!(
        !outcome.kv_after_writes.is_empty(),
        "host KV writes must land in the host SM",
    );
}
