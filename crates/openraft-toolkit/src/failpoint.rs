//! Per-crate failpoint wrapper. See `docs/failpoint-testing.md`.

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
