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

//! Async yield points — the structural analogue of [`fail-rs`] failpoints,
//! but driven by a `tokio::sync::Notify` so the production code yields its
//! tokio worker while parked instead of blocking the thread. A
//! fail-crate `pause` action wedges the tokio timer driver, which is
//! the symptom that motivated this module.
//!
//! Each call site is named:
//!
//! ```ignore
//! tsoracle_driver_paxos::yieldpoint!("standalone_host::apply_task::between_iterations");
//! ```
//!
//! When the `yieldpoints` feature is off the macro expands to nothing.
//! When on, the macro consults the registry; if the named yield point
//! is armed, the production code awaits the registered `Notify`.
//!
//! From a test:
//!
//! ```ignore
//! let handle = yieldpoint::cfg("name");
//! // ... drive code into the yield point ...
//! handle.notify_one(); // release
//! yieldpoint::remove("name");
//! ```
//!
//! [`fail-rs`]: https://docs.rs/fail

#[cfg(feature = "yieldpoints")]
mod registry {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::OnceLock;

    use parking_lot::Mutex;
    use tokio::sync::Notify;

    fn store() -> &'static Mutex<HashMap<&'static str, Arc<Notify>>> {
        static STORE: OnceLock<Mutex<HashMap<&'static str, Arc<Notify>>>> = OnceLock::new();
        STORE.get_or_init(|| Mutex::new(HashMap::new()))
    }

    /// Arm `name`. The returned handle is shared with the
    /// [`yieldpoint!`](crate::yieldpoint) call site; wake the production
    /// code by calling `notify_one()` on it.
    pub fn cfg(name: &'static str) -> Arc<Notify> {
        let notify = Arc::new(Notify::new());
        store().lock().insert(name, notify.clone());
        notify
    }

    /// Clear the armed entry for `name`. The yield point expands to a
    /// no-op on subsequent invocations until armed again.
    pub fn remove(name: &'static str) {
        store().lock().remove(name);
    }

    /// Lookup used by the [`yieldpoint!`](crate::yieldpoint) macro.
    #[doc(hidden)]
    pub fn get(name: &'static str) -> Option<Arc<Notify>> {
        store().lock().get(name).cloned()
    }
}

#[cfg(feature = "yieldpoints")]
pub use registry::{cfg, get, remove};

/// Await the registered `Notify` at this site if armed; no-op otherwise.
///
/// Expands to `{}` when the `yieldpoints` cargo feature is off, so
/// production builds carry zero overhead. When on, an armed entry parks
/// the calling task on `Notify::notified().await` — yielding the tokio
/// worker so timers and other tasks continue to run. Release with
/// `notify_one()` on the handle returned by [`cfg`].
#[cfg(feature = "yieldpoints")]
#[macro_export]
macro_rules! yieldpoint {
    ($name:expr) => {{
        if let Some(yp) = $crate::yieldpoint::get($name) {
            yp.notified().await;
        }
    }};
}

#[cfg(not(feature = "yieldpoints"))]
#[macro_export]
macro_rules! yieldpoint {
    ($name:expr) => {{}};
}
