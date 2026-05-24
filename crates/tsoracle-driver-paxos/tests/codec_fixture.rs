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

//! Pins the v1 on-disk frame of a paxos `HighWaterCommand` log entry: the
//! exact bytes `RocksdbStorage` persists per entry. A layout change trips this
//! and forces a `SCHEMA_VERSION` bump.

use tsoracle_driver_paxos::HighWaterCommand;
use tsoracle_paxos_toolkit::codec::{SCHEMA_VERSION, encode};

#[test]
fn log_entry_pins_v1_layout() {
    // Body [0,5] = HighWaterCommand::Advance variant (0) + at_least 5.
    let cmd = HighWaterCommand::Advance { at_least: 5 };
    let framed = encode(SCHEMA_VERSION, &cmd).expect("encode");
    assert_eq!(framed, vec![1, 0, 5]);
}
