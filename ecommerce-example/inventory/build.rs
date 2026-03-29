fn main() {
    prost_build::compile_protos(&["../schemas/inventory.proto"], &["../schemas"]).unwrap();
    println!("cargo:rerun-if-changed=../schemas/inventory.proto");
}
