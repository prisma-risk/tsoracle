//! In-memory fakes for the toolkit's own tests and downstream conformance suites.
//! Only compiled with the `test-fakes` feature or under `cfg(test)`.

// Test scaffolding: `clippy::unwrap_used` / `expect_used` do not fire inside
// `#[test]` / `#[cfg(test)]` (via `clippy.toml`), but these fakes live behind
// a feature flag rather than under `cfg(test)` so consumer crates can pull
// them in. Same intent — exempt the whole sub-tree.
#![allow(clippy::unwrap_used, clippy::expect_used)]

pub mod mem_network;
pub mod partition;

pub use mem_network::{MemNetwork, MemNetworkFactory, MemNetworkPeer};
pub use partition::PartitionController;
