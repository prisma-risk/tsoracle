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
use tsoracle_driver_openraft::HighWaterStateMachineSnapshot;
use tsoracle_openraft_toolkit::{SCHEMA_VERSION, decode, encode};

// Reverse-roundtrip property for the version-framed openraft snapshot codec.
// The snapshot is persisted and streamed as `[SCHEMA_VERSION | postcard(..)]`
// (see `state_machine.rs`, written with `encode` and read with `decode`), so
// the bytes that reach `decode` on `install_snapshot` / store load come from a
// peer or disk. Beyond "does not panic" (covered by `snapshot_payload_decode`),
// we assert the codec is a stable canonical form: decoding then re-encoding
// then decoding again must reproduce the same value.
//
// The property is value equality, not byte equality:
//   - `postcard::from_bytes` ignores trailing bytes, so `re-encode == input`
//     would false-positive on any valid prefix followed by junk.
//   - `StoredMembership` is built from ordered `BTreeMap`/`BTreeSet`, so value
//     equality is well-defined here; byte equality of two re-encodings would
//     still be the wrong assertion to bake in as the general contract.
fuzz_target!(|data: &[u8]| {
    let Ok(value) = decode::<HighWaterStateMachineSnapshot>(SCHEMA_VERSION, data) else {
        return;
    };
    let reencoded = encode(SCHEMA_VERSION, &value).expect("re-encoding a decoded value must succeed");
    let roundtripped: HighWaterStateMachineSnapshot = decode(SCHEMA_VERSION, &reencoded)
        .expect("re-encoded bytes must decode");
    assert_eq!(
        value, roundtripped,
        "codec is not a stable canonical form: decode -> encode -> decode changed the value"
    );
});
