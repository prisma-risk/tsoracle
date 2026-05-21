//! gRPC server for tsoracle.
//!
//! Wires four pieces together:
//! - [`tsoracle_core::Allocator`] — the sync window allocator.
//! - A user-supplied [`tsoracle_consensus::ConsensusDriver`] — leadership
//!   state plus durable high-water persistence.
//! - The tonic-generated `TsoServiceServer` from `tsoracle-proto`,
//!   mounted on `GetTs` and `GetTsBatch`.
//! - The internal leader-watch pipeline and failover fence, which keep
//!   timestamps strictly monotonic across leader transitions.
//!
//! Followers respond to RPCs with `FAILED_PRECONDITION` and a
//! `tsoracle-leader-hint-bin` binary trailer; `tsoracle-client` consumes
//! the hint to redirect without an extra round-trip.
//!
//! Use [`Server::builder`] to embed in another binary, or use the
//! `tsoracle` CLI from `tsoracle-bin` for a standalone process. The
//! [`docs`] module contains the docs.rs-rendered operations chapter; the
//! repo's `docs/key-subsystems.md` covers the same internals in depth.

#[macro_use]
mod failpoint;
mod fence;
mod leader_hint;
mod server;
mod service;

pub mod docs;

pub use server::{BuildError, Server, ServerBuilder, ServerError, ServingState};

#[cfg(any(test, feature = "test-fakes"))]
pub mod test_fakes;

#[doc(hidden)]
pub use leader_hint::decode_leader_hint as __priv_decode_leader_hint;
