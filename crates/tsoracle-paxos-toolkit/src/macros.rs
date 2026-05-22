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

//! Type-alias macro for downstream `OmniPaxos` configurations.
//!
//! `declare_omnipaxos_types_ext!(Name, EntryType, StorageType)` expands to:
//!
//! ```ignore
//! pub type NameOmniPaxos = ::omnipaxos::OmniPaxos<EntryType, StorageType>;
//! pub type NameConfig    = ::omnipaxos::OmniPaxosConfig;
//! ```
//!
//! Mirrors the role of `tsoracle_openraft_toolkit::declare_raft_types_ext!`
//! in the sibling crate.

#[macro_export]
macro_rules! declare_omnipaxos_types_ext {
    ($name:ident, $entry:ty, $storage:ty) => {
        ::pastey::paste! {
            pub type [<$name OmniPaxos>] = ::omnipaxos::OmniPaxos<$entry, $storage>;
            pub type [<$name Config>] = ::omnipaxos::OmniPaxosConfig;
        }
    };
}

#[cfg(test)]
mod tests {
    // The macro is exercised at expansion sites; this tests only that the
    // symbol path resolves at compile time. A full expansion smoke test
    // lives outside the crate in `tests/macro_smoke.rs` (lands in #162).
    #[test]
    fn macro_symbol_exists() {
        let _ = stringify!(crate::declare_omnipaxos_types_ext!);
    }
}
