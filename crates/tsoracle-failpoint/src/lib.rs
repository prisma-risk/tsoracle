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

#![doc = include_str!("../README.md")]

/// Re-export of the [`fail`] crate so the [`failpoint!`] expansion and any
/// test arming the registry both reach fail-rs through this crate — consumers
/// never name `fail` in their own dependency graph. Only present with the
/// `failpoints` feature on.
#[cfg(feature = "failpoints")]
#[doc(hidden)]
pub use fail;

/// Synchronous failpoint injection site.
///
/// With the `failpoints` feature on, forwards to [`fail::fail_point!`] so a
/// test can arm a `panic`, an error `return`, a `pause`, or a `sleep` at the
/// named site. With it off, both forms expand to `()` — zero code, and `fail`
/// is not linked. The expansion routes through `$crate::fail`, so consumers
/// only need a dependency on this crate (with the feature forwarded), never on
/// `fail` itself.
///
/// The single-argument form supports `panic`, `pause`, `sleep(ms)`, and
/// `print`. The closure form additionally supports `return` / `return(string)`
/// and must yield the enclosing function's exact return type — see
/// `docs/failpoint-testing.md`.
#[cfg(feature = "failpoints")]
#[macro_export]
macro_rules! failpoint {
    ($name:expr) => {
        $crate::fail::fail_point!($name)
    };
    ($name:expr, $closure:expr) => {
        $crate::fail::fail_point!($name, $closure)
    };
}

#[cfg(not(feature = "failpoints"))]
#[macro_export]
macro_rules! failpoint {
    ($name:expr) => {
        ()
    };
    ($name:expr, $closure:expr) => {
        ()
    };
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "failpoints")]
    #[test]
    fn failpoint_is_reachable() {
        // When the feature is enabled, the macro expands to a `fail`
        // registry call site. The closure form needs a Result-returning
        // context to compile (it can early-return) and is exercised in the
        // consumers' `tests/failpoints.rs` under real storage code paths.
        crate::failpoint!("test::point");
    }

    #[cfg(not(feature = "failpoints"))]
    #[test]
    fn failpoint_is_a_noop() {
        // When the feature is disabled, the macro must compile to nothing
        // measurable. Simply invoking it must not panic.
        crate::failpoint!("test::point");
    }
}
