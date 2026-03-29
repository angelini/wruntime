fn main() {
    // Client calls inventory RPCs, so it needs inventory's message types.
    prost_build::compile_protos(&["../schemas/inventory.proto"], &["../schemas"]).unwrap();
    println!("cargo:rerun-if-changed=../schemas/inventory.proto");
}
