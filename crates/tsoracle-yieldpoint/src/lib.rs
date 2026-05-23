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
/// Expands to `{}` when the `yieldpoints` cargo feature is off (on
/// `yield-rs` itself), so production builds of consuming crates carry
/// zero overhead. When on, an armed entry parks the calling task on
/// `Notify::notified().await` — yielding the tokio worker so timers and
/// other tasks continue to run. Release with `notify_one()` on the
/// handle returned by [`cfg`].
#[cfg(feature = "yieldpoints")]
#[macro_export]
macro_rules! yieldpoint {
    ($name:expr) => {{
        if let Some(yp) = $crate::get($name) {
            yp.notified().await;
        }
    }};
}

#[cfg(not(feature = "yieldpoints"))]
#[macro_export]
macro_rules! yieldpoint {
    ($name:expr) => {{}};
}
