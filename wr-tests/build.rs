fn main() {
    // Compile test proto files so the test harness has access to request/response
    // message types for constructing protobuf-encoded requests and decoding responses.
    // No service generator — only message types are needed on the test side.
    prost_build::Config::new()
        .compile_protos(
            &[
                "guests/schemas/db_test.proto",
                "guests/schemas/tracing_test.proto",
                "guests/schemas/blobstore_test.proto",
                "guests/schemas/http_test.proto",
                "guests/schemas/llm_test.proto",
            ],
            &["guests/schemas"],
        )
        .expect("failed to compile test proto files");

    println!("cargo:rerun-if-changed=guests/schemas/db_test.proto");
    println!("cargo:rerun-if-changed=guests/schemas/tracing_test.proto");
    println!("cargo:rerun-if-changed=guests/schemas/blobstore_test.proto");
    println!("cargo:rerun-if-changed=guests/schemas/http_test.proto");
    println!("cargo:rerun-if-changed=guests/schemas/llm_test.proto");
}
