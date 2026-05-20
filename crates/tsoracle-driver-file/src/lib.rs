//! Single-node, fsync-durable [`ConsensusDriver`] implementation for tsoracle.
//!
//! Persists the committed high-water as a 17-byte CRC-checked record
//! (4-byte magic `"TSOR"`, 1-byte version, `u64` high-water, `u32` crc32c)
//! and `fsync`s the file before any timestamp in that window is handed
//! out. That fsync is the durability boundary: a crash and restart can
//! never rewind the issued timestamp sequence.
//!
//! This is the durability story for the non-replicated configuration —
//! one process, one disk, one fsync per window extension. The standalone
//! `tsoracle serve` CLI wires it up automatically; embedded users
//! construct one via [`FileDriver::open_or_init`] (or
//! [`FileDriver::init_seeded`] for a one-shot migration from a prior
//! sequence).
//!
//! For multi-node high availability, implement [`ConsensusDriver`] against
//! your own replicated log instead; see the `examples/openraft-cluster`
//! crate for a worked openraft integration.
//!
//! [`ConsensusDriver`]: tsoracle_consensus::ConsensusDriver

mod driver;
pub mod record;

pub use driver::{FileDriver, FileDriverError};
