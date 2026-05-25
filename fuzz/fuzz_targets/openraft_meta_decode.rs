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
use tsoracle_driver_openraft::{OpenraftLogId, OpenraftVote};

// Adversarial-bytes safety for the openraft meta column family. On every node
// restart the log store reads three singletons back from the meta CF with
// *bare* postcard — no version frame — via `log_store::meta::read`: the current
// `Vote` (label `Vote`) and a `LogId` for the `Committed` and `LastPurged`
// labels. A panic decoding any of them crashes startup.
//
// This is distinct from `toolkit_codec_decode` (which covers the version-framed
// `[version | postcard]` records used for the log and snapshot). The meta path
// is unversioned, so it gets its own raw-postcard target over the two concrete
// types the meta CF actually holds.
fuzz_target!(|data: &[u8]| {
    let _ = postcard::from_bytes::<OpenraftVote>(data);
    let _ = postcard::from_bytes::<OpenraftLogId>(data);
});
