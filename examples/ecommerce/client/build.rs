fn main() {
    // Compile inventory.proto with WrClientGenerator to emit a typed InventoryServiceClient.
    // Include client.proto in the same pass so RunRequest/RunResponse are available.
    prost_build::Config::new()
        .service_generator(Box::new(wr_build::WrClientGenerator))
        .compile_protos(
            &["../schemas/inventory.proto", "../schemas/client.proto"],
            &["../schemas"],
        )
        .unwrap();
    println!("cargo:rerun-if-changed=../schemas/inventory.proto");
    println!("cargo:rerun-if-changed=../schemas/client.proto");
}
