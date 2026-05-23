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

//! Smoke test confirming that the shared `common/` harness compiles into a
//! test binary and produces a 3-node `MemCluster` without panicking.

#[path = "common/mod.rs"]
mod common;

#[test]
fn harness_compiles_with_three_node_cluster() {
    let cluster = common::build_mem_cluster(3);
    assert_eq!(cluster.nodes.len(), 3);
}
