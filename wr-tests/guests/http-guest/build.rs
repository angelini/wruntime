fn main() {
    prost_build::Config::new()
        .service_generator(Box::new(wr_build::WrServiceGenerator))
        .compile_protos(&["../schemas/http_test.proto"], &["../schemas"])
        .unwrap();
    println!("cargo:rerun-if-changed=../schemas/http_test.proto");
}
