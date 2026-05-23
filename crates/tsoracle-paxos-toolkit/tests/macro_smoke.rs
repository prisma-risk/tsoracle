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

//! Smoke test for the `declare_omnipaxos_types_ext!` macro: verify the
//! expansion produces well-formed type aliases that compile.

#[path = "common/mod.rs"]
mod common;

use common::TestCommand;
use tsoracle_paxos_toolkit::declare_omnipaxos_types_ext;
use tsoracle_paxos_toolkit::test_fakes::mem_storage::MemStorage;

declare_omnipaxos_types_ext!(MyTsoSetup, TestCommand, MemStorage<TestCommand>);

#[test]
fn macro_produces_expected_aliases() {
    // Type-check only: reference the produced aliases to force the macro
    // expansion to compile and resolve.
    fn _check(_op: Option<MyTsoSetupOmniPaxos>, _cfg: Option<MyTsoSetupConfig>) {}
    _check(None, None);
}
