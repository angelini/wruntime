fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Every feature-set build must regenerate from the one canonical descriptor.
    println!("cargo:rerun-if-changed=../proto/wruntime.proto");
    let descriptor_path = std::path::PathBuf::from(std::env::var_os("OUT_DIR").unwrap())
        .join("wruntime_descriptor.bin");
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(descriptor_path)
        .compile_protos(&["../proto/wruntime.proto"], &["../proto"])?;
    Ok(())
}
