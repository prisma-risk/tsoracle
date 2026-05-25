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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Watch BOTH protos unconditionally, decoupled from the compile guard below.
    // Cargo only re-runs a build script when a watched path changes; if we only
    // watched the protos we actually compiled, adding a proto after a build where
    // it was absent (e.g. the paxos proto lands a task later than the raft one)
    // would NOT re-trigger this script, leaving a stale OUT_DIR with no generated
    // code for the newly-added proto. Watching both up front avoids that trap.
    println!("cargo:rerun-if-changed=proto/raft_peer.proto");
    println!("cargo:rerun-if-changed=proto/paxos_peer.proto");

    // Compile each enabled driver's peer proto, but only if the file is present.
    // The `.exists()` guard keeps the script green while the protos are being
    // relocated across tasks; in the finished tree both exist, so nothing is skipped.
    let mut protos: Vec<&str> = Vec::new();
    if std::env::var_os("CARGO_FEATURE_OPENRAFT").is_some()
        && std::path::Path::new("proto/raft_peer.proto").exists()
    {
        protos.push("proto/raft_peer.proto");
    }
    if std::env::var_os("CARGO_FEATURE_PAXOS").is_some()
        && std::path::Path::new("proto/paxos_peer.proto").exists()
    {
        protos.push("proto/paxos_peer.proto");
    }
    if !protos.is_empty() {
        tonic_prost_build::configure()
            .build_server(true)
            .build_client(true)
            .compile_protos(&protos, &["proto"])?;
    }
    Ok(())
}
