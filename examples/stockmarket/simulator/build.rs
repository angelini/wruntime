fn main() {
    // Generate SimulatorService trait + router, AND clients for exchange/ledger.
    prost_build::Config::new()
        .service_generator(Box::new(wr_build::WrCombinedGenerator::new(
            wr_build::WrServiceGenerator,
            wr_build::WrClientGenerator,
        )))
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
