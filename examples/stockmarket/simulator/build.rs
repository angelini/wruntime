fn main() {
    // Generate clients for exchange and ledger services.
    // SimulatorService messages are included so SimRunRequest/SimRunResponse
    // are available (the Run route is handled manually, like the ecommerce client).
    prost_build::Config::new()
        .service_generator(Box::new(wr_build::WrClientGenerator))
        .compile_protos(
            &[
                "../schemas/exchange.proto",
                "../schemas/ledger.proto",
                "../schemas/simulator.proto",
            ],
            &["../schemas"],
        )
        .unwrap();
    println!("cargo:rerun-if-changed=../schemas/exchange.proto");
    println!("cargo:rerun-if-changed=../schemas/ledger.proto");
    println!("cargo:rerun-if-changed=../schemas/simulator.proto");
}
