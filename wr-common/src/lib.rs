/// Default freshness threshold for the PostgreSQL manager lease.
pub const DEFAULT_MANAGER_LIVENESS_THRESHOLD_SECS: u64 = 5;

/// Generated protobuf types and gRPC client/server stubs for all
/// inter-service communication in wruntime.
pub mod wruntime {
    tonic::include_proto!("wruntime");

    /// Complete protobuf descriptor used by listener-scoped authorization
    /// completeness tests. This is generated from the canonical proto source.
    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("wruntime_descriptor");
}

pub mod agent_policy;
#[cfg(feature = "config")]
pub mod authorization_policy;
#[cfg(feature = "config")]
pub mod config;
#[cfg(feature = "config")]
pub mod deployment_contract;
#[cfg(feature = "discovery")]
pub mod discovery;
pub mod http_headers;
#[cfg(feature = "http-pool")]
pub mod http_pool;
pub mod identity;
pub mod lifecycle;
/// Exact semantic lifecycle-state classification shared by observers.
pub mod lifecycle_observation;
#[cfg(any(feature = "signal", test))]
pub mod lifecycle_service;
pub mod manager_client;
pub mod naming;
pub mod node;
#[cfg(feature = "pool")]
pub mod pool;
#[cfg(any(feature = "signal", test))]
pub mod process_lifecycle;
#[cfg(any(feature = "signal", test))]
pub mod signal;
#[cfg(feature = "config")]
pub mod snapshot_consumer;
#[cfg(any(feature = "signal", test))]
pub mod task_group;
pub mod telemetry;
#[cfg(feature = "tls")]
pub mod tls;
pub mod uri;
