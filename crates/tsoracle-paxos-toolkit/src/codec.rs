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

//! Version-prefixed postcard codec used by every module that persists
//! OmniPaxos state. Every payload is encoded as `[version_byte |
//! postcard(value)]`; the leading byte lets the on-disk format evolve without
//! a silent misdecode — a stale reader hits [`CodecError::Version`] instead of
//! parsing old bytes against a new struct layout.
//!
//! The framing itself lives in the shared [`tsoracle_codec`] crate, re-used
//! verbatim by the openraft toolkit. Only [`SCHEMA_VERSION`] is owned here, so
//! this toolkit's on-disk format versions independently of the others.

pub use tsoracle_codec::{CodecError, decode, encode};

/// On-disk schema version stamped as the leading byte of every framed ballot,
/// stopsign, snapshot, and log entry. Bump when a persisted struct's postcard
/// layout changes incompatibly so a stale reader fails loudly.
pub const SCHEMA_VERSION: u8 = 1;
