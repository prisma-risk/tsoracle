//
//  ░▀█▀░█▀▀░█▀█░█▀▄░█▀█░█▀▀░█░░░█▀▀
//  ░░█░░▀▀█░█░█░█▀▄░█▀█░█░░░█░░░█▀▀
//  ░░▀░░▀▀▀░▀▀▀░▀░▀░▀░▀░▀▀▀░▀▀▀░▀▀▀
//
//  tsoracle — Distributed Timestamp Oracle
//
//  Copyright (c) 2026 Prisma Risk
//  Licensed under the Apache License, Version 2.0
//  https://github.com/prisma-risk/tsoracle
//

//! Optional backtrace capture for error types.
//!
//! [`Bt`] is a zero-sized type unless the `bt` Cargo feature is enabled. With
//! the feature off, [`Bt::capture`] is a no-op and the field adds nothing to
//! the enclosing error's layout. With the feature on, [`Bt::capture`] delegates
//! to [`std::backtrace::Backtrace::capture`], which itself honors the
//! `RUST_BACKTRACE` / `RUST_LIB_BACKTRACE` environment variables — so symbol
//! resolution stays opt-in at runtime even when the feature is compiled in.
//!
//! Embedding `Bt` in a `thiserror` enum variant:
//!
//! ```ignore
//! use tsoracle_core::Bt;
//!
//! #[derive(Debug, thiserror::Error)]
//! pub enum MyError {
//!     #[error("something went wrong{bt}")]
//!     SomethingWentWrong { bt: Bt },
//! }
//!
//! let err = MyError::SomethingWentWrong { bt: Bt::capture() };
//! ```

use core::fmt;

/// Optional captured backtrace.
///
/// See module docs for behavior under the `bt` feature.
#[cfg(feature = "bt")]
#[derive(Debug)]
pub struct Bt(std::backtrace::Backtrace);

#[cfg(not(feature = "bt"))]
#[derive(Debug, Clone, Copy)]
pub struct Bt;

impl Bt {
    /// Capture a backtrace at the call site.
    ///
    /// With the `bt` feature off this is a no-op that returns the unit-sized
    /// [`Bt`]. With the feature on, capture is delegated to
    /// [`std::backtrace::Backtrace::capture`] and is gated by the
    /// `RUST_BACKTRACE` / `RUST_LIB_BACKTRACE` environment variables.
    #[inline]
    pub fn capture() -> Self {
        #[cfg(feature = "bt")]
        {
            Self(std::backtrace::Backtrace::capture())
        }
        #[cfg(not(feature = "bt"))]
        {
            Self
        }
    }
}

impl fmt::Display for Bt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        #[cfg(feature = "bt")]
        {
            use std::backtrace::BacktraceStatus;
            if matches!(self.0.status(), BacktraceStatus::Captured) {
                // Newline-prefix so the backtrace renders on its own line
                // beneath the error message in default `Display` output.
                write!(formatter, "\n{}", self.0)?;
            }
            Ok(())
        }
        #[cfg(not(feature = "bt"))]
        {
            let _ = formatter;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capture_does_not_panic() {
        let _ = Bt::capture();
    }

    #[test]
    fn display_is_format_safe() {
        // Must format without panic in either feature mode. Whether content
        // is present depends on `RUST_BACKTRACE`/`RUST_LIB_BACKTRACE` and the
        // feature flag; only absence-of-panic is asserted here.
        let _ = format!("{}", Bt::capture());
    }

    #[cfg(not(feature = "bt"))]
    #[test]
    fn bt_is_zero_sized_without_feature() {
        // Without the feature, embedding `Bt` in a struct must add no bytes.
        // This is the load-bearing claim that makes opt-in instrumentation
        // free for downstream crates that never enable `bt`.
        assert_eq!(core::mem::size_of::<Bt>(), 0);
    }

    #[cfg(not(feature = "bt"))]
    #[test]
    fn display_is_empty_without_feature() {
        // No trailing backtrace section under `Display` when the feature is
        // off. Important so `#[error("...{bt}")]` formats identically to
        // `#[error("...")]` in the off configuration.
        assert_eq!(format!("{}", Bt::capture()), "");
    }

    #[cfg(feature = "bt")]
    #[test]
    fn bt_is_nonzero_sized_with_feature() {
        // With the feature on, `Bt` holds a `std::backtrace::Backtrace` and
        // therefore must occupy some space.
        assert!(core::mem::size_of::<Bt>() > 0);
    }
}
