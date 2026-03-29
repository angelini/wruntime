fn main() {
    // Client calls inventory RPCs, so it needs inventory's message types.
    // WrClientGenerator adds a typed InventoryServiceClient to the generated output.
    prost_build::Config::new()
        .service_generator(Box::new(wr_build::WrClientGenerator))
        .compile_protos(&["../schemas/inventory.proto"], &["../schemas"])
        .unwrap();
    println!("cargo:rerun-if-changed=../schemas/inventory.proto");
}
