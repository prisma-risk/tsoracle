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

//! Binary wire codec for openraft RPC payloads and storage records.
//!
//! Every payload is encoded as `[version_byte | postcard(value)]`. The leading
//! byte lets us evolve the wire format without an explicit migration when both
//! sides of an upgrade run mixed versions briefly.
//!
//! The framing itself lives in the shared [`tsoracle_codec`] crate, re-used
//! verbatim by the paxos toolkit. Only [`SCHEMA_VERSION`] is owned here, so
//! this toolkit's wire/on-disk format versions independently of the others.

pub use tsoracle_codec::{CodecError, codec_io_error, decode, encode};

/// On-disk/wire schema version stamped as the leading byte of every framed
/// snapshot, log entry, and storage record produced by this toolkit. Bump
/// when a persisted struct's postcard layout changes in a
/// backward-incompatible way (field reorder/insert/remove), or when the set of
/// framed records changes; a stale reader then fails loudly with
/// [`CodecError::Version`] instead of misdecoding.
///
/// v2 brought the log-store meta column (Vote/Committed/LastPurged) under this
/// same frame — it was previously persisted as bare, unversioned postcard, so a
/// v1 store's meta records read against this version loud-reject rather than
/// risk a silent misdecode of the recovery-critical vote.
///
/// v3 widened the openraft driver's `OpenraftPeer` membership node (a
/// `service_endpoint` field beside `addr`), changing the postcard layout of
/// membership log entries. The bump is global to this toolkit, so a v2 store's
/// records — for any config, not just the standalone driver — loud-reject when
/// read against v3 rather than risk a silent misdecode.
pub const SCHEMA_VERSION: u8 = 3;
