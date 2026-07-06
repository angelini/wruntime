/// Proto types generated from the test .proto files (message types only).
#[allow(dead_code)]
mod generated {
    include!(concat!(env!("OUT_DIR"), "/test.rs"));
}

pub use generated::*;
