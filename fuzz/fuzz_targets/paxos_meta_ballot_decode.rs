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
use tsoracle_paxos_toolkit::storage::meta::decode_ballot;

// Adversarial-bytes safety for the toolkit's meta-column ballot decoder. The
// promised/accepted `Ballot` is the one omnipaxos-shaped value the toolkit
// reads back as postcard from disk on every recovery (see
// `storage/meta.rs::decode_ballot`); a panic in that decode crashes startup.
// This is the paxos toolkit's counterpart of `toolkit_codec_decode` — the
// paxos `codec` is a bare postcard passthrough with no framing, so the
// meaningful surface is the concrete type the toolkit owns reading back,
// rather than the wrapper itself.
fuzz_target!(|data: &[u8]| {
    let _ = decode_ballot(data);
});
