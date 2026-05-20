//! Shared test scaffolding for `tsoracle-driver-openraft` integration tests.
//!
//! Each `tests/*.rs` declares `mod common;` and imports via `use common::*;`.
//! Rust compiles this module per integration-test binary (a known minor
//! duplication; negligible at our scale).

#![allow(dead_code)] // each test binary uses a subset; allow the rest

use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use openraft::Raft;
use openraft_toolkit::test_fakes::{MemNetwork, PartitionController};
use tempfile::TempDir;
use tokio::time::Instant;
use tsoracle_driver_openraft::{
    HighWaterStateMachine, OpenraftDriver, StandaloneHost, TypeConfig,
};

/// One node in a test cluster. Holds the raft handle, a clone of the state
/// machine for direct reads, the rocksdb tempdir (so files outlive the test),
/// and the node id.
pub struct TestNode {
    pub id: u64,
    pub raft: Raft<TypeConfig, HighWaterStateMachine>,
    pub sm: HighWaterStateMachine,
    pub log_dir: TempDir,
}

/// A built test cluster. `network` and `partitions` are `None` for
/// single-node clusters (those use a panicking network). `drivers[i]`
/// corresponds to `nodes[i]`.
pub struct TestCluster {
    pub nodes: Vec<TestNode>,
    pub network: Option<Arc<MemNetwork<TypeConfig>>>,
    pub partitions: Option<Arc<PartitionController<u64>>>,
    pub drivers: Vec<Arc<OpenraftDriver<StandaloneHost>>>,
}

/// Poll `f` on a 50ms cadence until it yields `expected` or `timeout`
/// elapses. Panics with a descriptive message on timeout.
pub async fn eventually_eq<T, F, Fut>(expected: T, timeout: Duration, mut f: F)
where
    T: PartialEq + Debug,
    F: FnMut() -> Fut,
    Fut: Future<Output = T>,
{
    let deadline = Instant::now() + timeout;
    let mut last = f().await;
    while last != expected && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
        last = f().await;
    }
    assert_eq!(
        last, expected,
        "eventually_eq timed out after {:?}: last={:?} expected={:?}",
        timeout, last, expected
    );
}

// Builders below are stubs filled in by subsequent tasks:
// - build_single_node, build_three_node: cluster constructors
// - reopen_node: restart-replay primitive

pub async fn build_single_node() -> TestCluster {
    unimplemented!("build_single_node lands in a follow-up task")
}

pub async fn build_three_node() -> TestCluster {
    unimplemented!("build_three_node lands in a follow-up task")
}

pub async fn reopen_node(_prior: TestNode) -> TestNode {
    unimplemented!("reopen_node lands in a follow-up task")
}
