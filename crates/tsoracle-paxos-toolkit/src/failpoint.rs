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

//! Failpoint injection sites. When the `failpoints` feature is enabled, the
//! `fail_point!` macro forwards to the `fail` crate's macro of the same name;
//! when disabled, it expands to nothing. Centralized here so other modules
//! invoke a single symbol regardless of feature state.

#[cfg(feature = "failpoints")]
#[allow(unused_imports)]
pub use fail::fail_point;

#[cfg(not(feature = "failpoints"))]
#[macro_export]
macro_rules! fail_point {
    ($($tt:tt)*) => {};
}

#[cfg(not(feature = "failpoints"))]
#[allow(unused_imports)]
pub use crate::fail_point;

#[cfg(test)]
mod tests {
    #[cfg(feature = "failpoints")]
    #[test]
    fn fail_point_is_reachable() {
        // When the feature is enabled, the macro expands to a call site
        // the `fail` registry can match against. We verify the symbol
        // exists by referencing it; actual injection is exercised in
        // `tests/failpoints.rs` under the real RocksDB code path.
        super::fail_point!("test::point");
    }

    #[cfg(not(feature = "failpoints"))]
    #[test]
    fn fail_point_is_a_noop() {
        // When the feature is disabled, the macro must compile to nothing
        // measurable. Simply invoking it must not panic.
        super::fail_point!("test::point");
    }
}
