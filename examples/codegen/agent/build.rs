fn main() {
    prost_build::Config::new()
        .service_generator(Box::new(wr_build::WrServiceGenerator))
        .compile_protos(&["../schemas/agent.proto"], &["../schemas"])
        .unwrap();
    println!("cargo:rerun-if-changed=../schemas/agent.proto");
}
