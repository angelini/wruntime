fn main() {
    // Generate CoordinatorService trait + router, AND client stubs for
    // CollectorService and AgentService.
    prost_build::Config::new()
        .service_generator(Box::new(wr_build::WrCombinedGenerator::new(
            wr_build::WrServiceGenerator,
            wr_build::WrClientGenerator,
        )))
        .compile_protos(
            &[
                "../schemas/coordinator.proto",
                "../schemas/collector.proto",
                "../schemas/agent.proto",
            ],
            &["../schemas"],
        )
        .unwrap();
    println!("cargo:rerun-if-changed=../schemas/coordinator.proto");
    println!("cargo:rerun-if-changed=../schemas/collector.proto");
    println!("cargo:rerun-if-changed=../schemas/agent.proto");
}
