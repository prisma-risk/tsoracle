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

// Adversarial-bytes safety for the OmniPaxos snapshot decoder. A
// `HighWaterSnapshot` is transferred between nodes during snapshot catch-up
// and persisted to the RocksDB meta column, so it is decoded from bytes a
// recovering or lagging node did not produce itself. Its `applied_barriers`
// `HashMap<u64, u64>` is variable-length — the field most likely to expose a
// postcard length/allocation edge case, which makes it worth a target
// distinct from `paxos_log_entry_decode`.
fuzz_target!(|data: &[u8]| {
    let _ = postcard::from_bytes::<HighWaterSnapshot>(data);
});
