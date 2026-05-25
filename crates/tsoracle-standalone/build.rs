fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Compile each enabled driver's peer proto, but only if the file is present.
    // The `.exists()` guard keeps the script green while the protos are being
    // relocated across tasks (the paxos proto lands in a later task than the
    // raft one); in the finished tree both protos exist, so nothing is skipped.
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
    for proto in protos {
        println!("cargo:rerun-if-changed={proto}");
    }
    Ok(())
}
