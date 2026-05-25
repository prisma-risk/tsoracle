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
use tsoracle_driver_paxos::HighWaterSnapshot;
use tsoracle_paxos_toolkit::codec::{SCHEMA_VERSION, decode, encode};

// Reverse-roundtrip property for the version-framed paxos snapshot codec. The
// `HighWaterSnapshot` is persisted to the RocksDB meta column and transferred
// during snapshot catch-up as `[SCHEMA_VERSION | postcard(..)]` (the paxos
// storage layer encodes it with `encode_postcard` and reads it back with
// `decode_postcard`, both of which route through this codec). Beyond "does not
// panic" (covered by `paxos_snapshot_decode`), we assert the codec is a stable
// canonical form: decode -> encode -> decode reproduces the same value.
//
// The assertion is value equality on the fields, deliberately NOT byte
// equality of the re-encodings:
//   - `applied_barriers` is a `HashMap`, whose iteration order is randomized
//     per instance, so two equal maps can serialize to different byte orders;
//     `HashMap`'s `==` compares them as sets, which is order-independent.
//   - `postcard::from_bytes` ignores trailing bytes, so `re-encode == input`
//     would false-positive on a valid prefix followed by junk.
fuzz_target!(|data: &[u8]| {
    let Ok(value) = decode::<HighWaterSnapshot>(SCHEMA_VERSION, data) else {
        return;
    };
    let reencoded = encode(SCHEMA_VERSION, &value).expect("re-encoding a decoded value must succeed");
    let roundtripped: HighWaterSnapshot =
        decode(SCHEMA_VERSION, &reencoded).expect("re-encoded bytes must decode");
    assert_eq!(
        value.value, roundtripped.value,
        "codec changed the high-water value across decode -> encode -> decode"
    );
    assert_eq!(
        value.applied_barriers, roundtripped.applied_barriers,
        "codec changed the applied-barriers ledger across decode -> encode -> decode"
    );
});
