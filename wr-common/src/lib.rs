/// Generated protobuf types and gRPC client/server stubs for all
/// inter-service communication in wruntime.
pub mod wruntime {
    tonic::include_proto!("wruntime");
}

#[cfg(feature = "config")]
pub mod config;
#[cfg(feature = "discovery")]
pub mod discovery;
pub mod http_headers;
#[cfg(feature = "http-pool")]
pub mod http_pool;
pub mod naming;
pub mod node;
#[cfg(feature = "pool")]
pub mod pool;
#[cfg(feature = "signal")]
pub mod signal;
pub mod telemetry;
#[cfg(feature = "tls")]
pub mod tls;
