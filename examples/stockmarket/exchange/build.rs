fn main() {
    // Generate ExchangeService trait + router, AND LedgerServiceClient for
    // recording trades on the ledger module after order matching.
    prost_build::Config::new()
        .service_generator(Box::new(wr_build::WrCombinedGenerator::new(
            wr_build::WrServiceGenerator,
            wr_build::WrClientGenerator,
        )))
        .compile_protos(
            &["../schemas/exchange.proto", "../schemas/ledger.proto"],
            &["../schemas"],
        )
        .unwrap();
    println!("cargo:rerun-if-changed=../schemas/exchange.proto");
    println!("cargo:rerun-if-changed=../schemas/ledger.proto");
}
