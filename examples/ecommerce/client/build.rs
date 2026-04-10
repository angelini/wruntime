fn main() {
    // Generate service handler for client.proto (client_service_handle) plus
    // client stubs for inventory.proto (InventoryServiceClient).
    prost_build::Config::new()
        .service_generator(Box::new(wr_build::WrCombinedGenerator::new(
            wr_build::WrServiceGenerator,
            wr_build::WrClientGenerator,
        )))
        .compile_protos(
            &["../schemas/inventory.proto", "../schemas/client.proto"],
            &["../schemas"],
        )
        .unwrap();
    println!("cargo:rerun-if-changed=../schemas/inventory.proto");
    println!("cargo:rerun-if-changed=../schemas/client.proto");
}
