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
use tsoracle_driver_paxos::HighWaterCommand;

// Adversarial-bytes safety for the OmniPaxos log entry decoder. Every
// `HighWaterCommand` (`Advance` / `Barrier`) rides a replicated entry: bytes
// arrive over the peer transport and are read back from the RocksDB log
// column on recovery, so a panic here crashes a follower or a restarting
// node. The paxos counterpart of `log_entry_decode` (which covers the
// openraft `HighWaterCommand`).
fuzz_target!(|data: &[u8]| {
    let _ = postcard::from_bytes::<HighWaterCommand>(data);
});
