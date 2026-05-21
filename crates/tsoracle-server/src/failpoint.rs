//! Per-crate failpoint wrapper. See `docs/failpoint-testing.md` for the
//! conventions enforced here: source sites never reference `fail::` directly;
//! they go through `crate::failpoint!`.
//!
//! With `feature = "failpoints"` off, both forms expand to `()` and the
//! `fail` crate is not in the build graph.

#[cfg(feature = "failpoints")]
#[macro_export]
macro_rules! failpoint {
    ($name:expr) => {
        fail::fail_point!($name)
    };
    ($name:expr, $closure:expr) => {
        fail::fail_point!($name, $closure)
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
