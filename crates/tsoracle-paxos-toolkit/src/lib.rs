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

//! Reusable OmniPaxos glue for tsoracle's paxos driver.
//!
//! This crate provides a RocksDB-backed [`omnipaxos::storage::Storage`] impl,
//! lifecycle helpers, the `declare_omnipaxos_types_ext!` macro, and in-memory
//! test fakes. The paxos driver crate consumes this crate; this crate itself
//! does not depend on the driver.

#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used))]
#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod codec;
pub mod failpoint;
pub mod lifecycle;
pub mod macros;
pub mod storage;

#[cfg(feature = "test-fakes")]
pub mod test_fakes;
