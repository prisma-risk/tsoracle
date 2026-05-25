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

#![no_main]

use libfuzzer_sys::fuzz_target;
use tsoracle_driver_openraft::OpenraftEntry;
use tsoracle_openraft_toolkit::{SCHEMA_VERSION, decode};

// Adversarial-bytes safety for the full openraft log record. The log store
// reads `[version | postcard(Entry<TypeConfig>)]` from the log column family
// on recovery and from append-entries replication (see
// `log_store/mod.rs::decode_record`), so a panic here crashes a follower or a
// restarting node.
//
// `log_entry_decode` already fuzzes the inner `HighWaterCommand`, but that is
// only the `EntryPayload::Normal` payload. Decoding the whole `Entry` here also
// drives the version gate (empty / version-mismatch branches) and the
// variable-length `EntryPayload::Membership(Membership<C>)` variant, which the
// inner-command target never reaches.
fuzz_target!(|data: &[u8]| {
    let _ = decode::<OpenraftEntry>(SCHEMA_VERSION, data);
});
