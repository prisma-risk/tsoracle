//! Core algorithm for tsoracle.
//!
//! No I/O, no async, no tokio. Runtime-neutral. Property-testable in microseconds.

mod allocator;
mod clock;
mod epoch;
mod timestamp;

pub mod docs;

pub use allocator::{Allocator, CoreError, WindowGrant};
pub use clock::{Clock, SystemClock};
pub use epoch::Epoch;
pub use timestamp::{LOGICAL_MAX, PHYSICAL_MS_MAX, Timestamp, TimestampError};

#[cfg(any(test, feature = "test-clock"))]
pub use clock::testing;
