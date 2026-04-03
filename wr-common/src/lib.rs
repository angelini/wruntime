/// Generated protobuf types and gRPC client/server stubs for all
/// inter-service communication in wruntime.
pub mod wruntime {
    tonic::include_proto!("wruntime");
}

#[cfg(feature = "discovery")]
pub mod discovery;
pub mod node;
pub mod telemetry;
