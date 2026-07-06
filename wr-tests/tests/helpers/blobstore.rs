use std::sync::Arc;

use wr_engine::blobstore::BlobstoreRuntime;
use wr_engine::config::BlobstoreConfig;

use super::db::{ModuleServices, ModuleState};
use super::proxy::http_pool;

pub fn blobstore_client() -> Arc<BlobstoreRuntime> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let endpoint = std::env::var("WRT_TEST_S3_ENDPOINT")
        .expect("WRT_TEST_S3_ENDPOINT must be set for this test");
    let access_key = std::env::var("WRT_TEST_S3_ACCESS_KEY")
        .expect("WRT_TEST_S3_ACCESS_KEY must be set for this test");
    let secret_key = std::env::var("WRT_TEST_S3_SECRET_KEY")
        .expect("WRT_TEST_S3_SECRET_KEY must be set for this test");
    let config = BlobstoreConfig {
        endpoint,
        access_key_id: access_key,
        secret_access_key: secret_key,
        region: "us-east-1".into(),
        max_object_size: 16 * 1024 * 1024,
        max_list_objects: 1000,
    };
    Arc::new(BlobstoreRuntime::new(&config).expect("BlobstoreRuntime"))
}

/// Build a `ModuleState` with a blobstore client for WASM guest tests.
pub fn blobstore_state(blobstore: Arc<BlobstoreRuntime>) -> ModuleState {
    ModuleState::new(
        "blobstore-test".into(),
        "test-ns".into(),
        "http://127.0.0.1:9001".parse().unwrap(),
        http_pool(),
        ModuleServices {
            blobstore: Some(blobstore),
            ..Default::default()
        },
    )
    .expect("ModuleState")
}

/// Build a `ModuleState` with a blobstore client and explicit size/list limits.
pub fn blobstore_state_with_limits(
    blobstore: Arc<BlobstoreRuntime>,
    blob_limits: wr_engine::config::BlobstoreLimits,
) -> ModuleState {
    ModuleState::new(
        "blobstore-test".into(),
        "test-ns".into(),
        "http://127.0.0.1:9001".parse().unwrap(),
        http_pool(),
        ModuleServices {
            blobstore: Some(blobstore),
            blob_limits,
            ..Default::default()
        },
    )
    .expect("ModuleState")
}
