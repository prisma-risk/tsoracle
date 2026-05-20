//! openraft-backed `ConsensusDriver` for tsoracle.
//!
//! This crate replicates the TSO high-water mark across an openraft cluster
//! and exposes the result via the [`tsoracle_consensus::ConsensusDriver`]
//! trait. The caller supplies a pre-built [`openraft::Raft`] handle (with its
//! own `RaftNetworkFactory` and `RaftLogStorage`); this crate provides the
//! `RaftTypeConfig`, the log-entry type, the in-memory state machine, and
//! the trait bridge.
//!
//! # Layering
//!
//! - [`log_entry`] defines the single command type replicated through the log.
//! - [`state_machine`] implements `openraft::storage::RaftStateMachine` over
//!   an in-memory `u64` counter with bincode snapshots.
//! - [`type_config`] declares the `RaftTypeConfig` via
//!   `openraft_toolkit::declare_raft_types_ext!`.
//! - [`driver`] wraps the `Raft` handle and the state machine into the
//!   `ConsensusDriver` impl.

pub mod driver;
pub mod log_entry;
pub mod state_machine;
pub mod type_config;

pub use driver::OpenraftDriver;
pub use log_entry::HighWaterCommand;
pub use state_machine::{HighWaterStateMachine, HighWaterStateMachineSnapshot};
pub use type_config::{HighWaterApplied, OpenraftPeer, TypeConfig};
