fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR")?);
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(out_dir.join("tsoracle_descriptor.bin"))
        .compile_protos(&["proto/tsoracle/v1/tso.proto"], &["proto"])?;
    println!("cargo:rerun-if-changed=proto/tsoracle/v1/tso.proto");
    Ok(())
}
