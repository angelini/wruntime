fn main() {
    prost_build::Config::new()
        .service_generator(Box::new(wr_build::WrServiceGenerator))
        .compile_protos(&["../schemas/tracing_test.proto"], &["../schemas"])
        .unwrap();
    println!("cargo:rerun-if-changed=../schemas/tracing_test.proto");
}
