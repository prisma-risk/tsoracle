//! In-memory fakes for the toolkit's own tests and downstream conformance suites.
//! Only compiled with the `test-fakes` feature or under `cfg(test)`.

pub mod mem_network;
pub mod partition;

pub use mem_network::{MemNetwork, MemNetworkFactory, MemNetworkPeer};
pub use partition::PartitionController;
