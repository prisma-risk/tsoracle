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
use tsoracle_openraft_toolkit::{SCHEMA_VERSION, decode};

// Adversarial-bytes safety for the openraft meta column family. On every node
// restart the log store reads three singletons back from the meta CF via
// `log_store::meta::read`, which routes them through the same
// `[version_byte | postcard(body)]` frame as the log and snapshot records (the
// meta column was brought under the frame in `SCHEMA_VERSION` v2): the current
// `Vote` (label `Vote`) and a `LogId` for the `Committed` and `LastPurged`
// labels. A panic decoding any of them crashes startup.
//
// Mirrors `toolkit_codec_decode`'s split between the framing and the postcard
// body: `decode::<T>(SCHEMA_VERSION, data)` drives the expected-version path
// (the one `meta::read` takes), while `decode::<T>(data[0], data)` escapes the
// version gate so the postcard decoder sees the whole body for any leading
// byte. Distinct from `toolkit_codec_decode` in the concrete types it
// exercises — the two the meta CF actually holds, including the
// variable-length `LogId`.
fuzz_target!(|data: &[u8]| {
    let _ = decode::<OpenraftVote>(SCHEMA_VERSION, data);
    let _ = decode::<OpenraftLogId>(SCHEMA_VERSION, data);
    if let Some(&first) = data.first() {
        let _ = decode::<OpenraftVote>(first, data);
        let _ = decode::<OpenraftLogId>(first, data);
    }
});
