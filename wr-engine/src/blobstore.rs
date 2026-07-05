use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::path::Path as ObjectPath;
use object_store::{Error as ObjectStoreError, ObjectStore, ObjectStoreExt, PutPayload};

use crate::config::{BlobstoreConfig, BlobstoreLimits};
use crate::state::ModuleState;

/// Shared S3 client configuration. One `AmazonS3` store is built per bucket on
/// first use and cached — building is cheap (no network I/O) but constructs an
/// HTTP client, so we avoid rebuilding it on every call.
pub struct BlobstoreRuntime {
    endpoint: String,
    region: String,
    access_key_id: String,
    secret_access_key: String,
    buckets: Mutex<HashMap<String, Arc<AmazonS3>>>,
}

impl BlobstoreRuntime {
    pub fn new(config: &BlobstoreConfig) -> anyhow::Result<Self> {
        Ok(Self {
            endpoint: config.endpoint.clone(),
            region: config.region.clone(),
            access_key_id: config.access_key_id.clone(),
            secret_access_key: config.secret_access_key.clone(),
            buckets: Mutex::new(HashMap::new()),
        })
    }

    fn bucket(&self, name: &str) -> Result<Arc<AmazonS3>, ObjectStoreError> {
        let mut cache = self
            .buckets
            .lock()
            .expect("blobstore bucket cache poisoned");
        if let Some(store) = cache.get(name) {
            return Ok(store.clone());
        }
        let store = Arc::new(
            AmazonS3Builder::new()
                .with_endpoint(&self.endpoint)
                .with_region(&self.region)
                .with_bucket_name(name)
                .with_access_key_id(&self.access_key_id)
                .with_secret_access_key(&self.secret_access_key)
                .with_allow_http(true)
                .with_virtual_hosted_style_request(false)
                .build()?,
        );
        cache.insert(name.to_string(), store.clone());
        Ok(store)
    }
}

// ── WIT bindings ─────────────────────────────────────────────────────────────

wasmtime::component::bindgen!({
    path:  "../wit/blobstore.wit",
    world: "blobstore-access",
    imports: { default: async },
});

pub use wruntime::blobstore::store::BlobError;
use wruntime::blobstore::store::{Host, ObjectMeta};

// ── Namespace isolation helpers ───────────────────────────────────────────────

/// Normalize a path by resolving `.`, `..`, and collapsing duplicate `/`
/// separators. Returns `None` if the result would escape above the root
/// (more `..` segments than real segments).
fn normalize_key(key: &str) -> Option<String> {
    let mut segments: Vec<&str> = Vec::new();
    for seg in key.split('/') {
        match seg {
            "" | "." => {}
            ".." => {
                segments.pop()?;
            }
            s => segments.push(s),
        }
    }
    Some(segments.join("/"))
}

/// Prepend the namespace prefix to a key after normalizing it. Returns an
/// error if the key attempts path traversal.
fn scoped_key(prefix: &Option<Arc<str>>, key: &str) -> Result<String, BlobError> {
    let clean = normalize_key(key)
        .ok_or_else(|| BlobError::AccessDenied("path traversal in key".into()))?;
    match prefix {
        Some(p) => Ok(format!("{p}{clean}")),
        None => Ok(clean),
    }
}

/// Strip the namespace prefix from a key returned by S3.
fn unscoped_key(prefix: &Option<Arc<str>>, key: &str) -> String {
    match prefix {
        Some(p) => key.strip_prefix(&**p).unwrap_or(key).to_string(),
        None => key.to_string(),
    }
}

impl ModuleState {
    /// Fold the per-operation boilerplate shared by the single-key methods:
    /// capability lookup → bucket store → namespace-scoped object path. Returns the
    /// per-store size/list limits alongside, so callers enforce them without a second
    /// capability borrow. Composes the existing `scoped_key`/`map_os_err` unchanged.
    fn resolve_object(
        &mut self,
        bucket: &str,
        key: &str,
    ) -> Result<(Arc<AmazonS3>, ObjectPath, BlobstoreLimits), BlobError> {
        let cap = self.blobstore()?;
        let store = cap.runtime.bucket(bucket).map_err(map_os_err)?;
        let path = ObjectPath::from(scoped_key(&cap.prefix, key)?);
        Ok((store, path, cap.limits))
    }
}

// ── Host implementation ───────────────────────────────────────────────────────

impl Host for ModuleState {
    async fn put_object(
        &mut self,
        bucket: String,
        key: String,
        data: Vec<u8>,
    ) -> Result<(), BlobError> {
        let (store, path, limits) = self.resolve_object(&bucket, &key)?;
        if data.len() > limits.max_object_size {
            return Err(BlobError::TooLarge(format!(
                "upload of {} bytes exceeds max_object_size of {} bytes",
                data.len(),
                limits.max_object_size
            )));
        }
        store
            .put(&path, PutPayload::from(data))
            .await
            .map_err(map_os_err)?;
        Ok(())
    }

    async fn get_object(&mut self, bucket: String, key: String) -> Result<Vec<u8>, BlobError> {
        use futures::StreamExt;

        let (store, path, limits) = self.resolve_object(&bucket, &key)?;
        let result = store.get(&path).await.map_err(map_os_err)?;
        let mut stream = result.into_stream();
        let mut buf: Vec<u8> = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(map_os_err)?;
            if buf.len() + chunk.len() > limits.max_object_size {
                return Err(BlobError::TooLarge(format!(
                    "download exceeds max_object_size of {} bytes",
                    limits.max_object_size
                )));
            }
            buf.extend_from_slice(&chunk);
        }
        Ok(buf)
    }

    async fn delete_object(&mut self, bucket: String, key: String) -> Result<(), BlobError> {
        let (store, path, _) = self.resolve_object(&bucket, &key)?;
        store.delete(&path).await.map_err(map_os_err)?;
        Ok(())
    }

    async fn list_objects(
        &mut self,
        bucket: String,
        prefix: Option<String>,
    ) -> Result<Vec<ObjectMeta>, BlobError> {
        use futures::StreamExt;

        let (store, key_prefix, limits) = {
            let cap = self.blobstore()?;
            (
                cap.runtime.bucket(&bucket).map_err(map_os_err)?,
                cap.prefix.clone(),
                cap.limits,
            )
        };
        let scoped_prefix = scoped_key(&key_prefix, &prefix.unwrap_or_default())?;
        let prefix_path = (!scoped_prefix.is_empty()).then(|| ObjectPath::from(scoped_prefix));

        let mut all: Vec<ObjectMeta> = Vec::new();
        let mut stream = store.list(prefix_path.as_ref());
        while let Some(meta) = stream.next().await {
            let meta = meta.map_err(map_os_err)?;
            if all.len() >= limits.max_list_objects {
                return Err(BlobError::TooLarge(format!(
                    "listing exceeds max_list_objects of {}",
                    limits.max_list_objects
                )));
            }
            all.push(ObjectMeta {
                key: unscoped_key(&key_prefix, meta.location.as_ref()),
                size: meta.size,
                last_modified: meta.last_modified.timestamp(),
                etag: meta.e_tag.unwrap_or_default(),
            });
        }
        Ok(all)
    }

    async fn head_object(&mut self, bucket: String, key: String) -> Result<ObjectMeta, BlobError> {
        let (store, path, _) = self.resolve_object(&bucket, &key)?;
        let meta = store.head(&path).await.map_err(map_os_err)?;
        Ok(ObjectMeta {
            key,
            size: meta.size,
            last_modified: meta.last_modified.timestamp(),
            etag: meta.e_tag.unwrap_or_default(),
        })
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn map_os_err(e: ObjectStoreError) -> BlobError {
    match e {
        ObjectStoreError::NotFound { path, .. } => BlobError::NotFound(path),
        ObjectStoreError::PermissionDenied { path, .. } => BlobError::AccessDenied(path),
        ObjectStoreError::Unauthenticated { path, .. } => BlobError::AccessDenied(path),
        e => BlobError::Io(e.to_string()),
    }
}

pub use wruntime::blobstore::store::add_to_linker;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::config::BlobstoreConfig;
    use crate::state::ModuleState;

    fn proxy_uri() -> hyper::Uri {
        "http://127.0.0.1:9001".parse().unwrap()
    }

    fn test_http_pool() -> wr_common::http_pool::HttpClientPool<http_body_util::Full<bytes::Bytes>>
    {
        wr_common::http_pool::HttpClientPool::new(1)
    }

    fn test_config() -> BlobstoreConfig {
        BlobstoreConfig {
            endpoint: "http://127.0.0.1:8900".into(),
            access_key_id: "test-key".into(),
            secret_access_key: "test-secret".into(),
            region: "us-east-1".into(),
            max_object_size: 16 * 1024 * 1024,
            max_list_objects: 1000,
        }
    }

    // ── normalize_key tests ────────────────────────────────────────────────────

    #[test]
    fn test_normalize_key_simple() {
        assert_eq!(normalize_key("a/b/c"), Some("a/b/c".into()));
    }

    #[test]
    fn test_normalize_key_dot_segments() {
        assert_eq!(normalize_key("a/./b/../c"), Some("a/c".into()));
    }

    #[test]
    fn test_normalize_key_double_slash() {
        assert_eq!(normalize_key("a//b///c"), Some("a/b/c".into()));
    }

    #[test]
    fn test_normalize_key_traversal_blocked() {
        assert_eq!(normalize_key("../etc/passwd"), None);
        assert_eq!(normalize_key("a/../../b"), None);
    }

    #[test]
    fn test_normalize_key_leading_dot_dot_in_subpath() {
        assert_eq!(normalize_key("a/b/../c"), Some("a/c".into()));
    }

    // ── scoped_key / unscoped_key tests ───────────────────────────────────────

    #[test]
    fn test_scoped_key_with_prefix() {
        let prefix = Some(Arc::<str>::from("wr/ecommerce/"));
        assert_eq!(
            scoped_key(&prefix, "file.txt").unwrap(),
            "wr/ecommerce/file.txt"
        );
    }

    #[test]
    fn test_scoped_key_without_prefix() {
        assert_eq!(scoped_key(&None, "file.txt").unwrap(), "file.txt");
    }

    #[test]
    fn test_scoped_key_rejects_traversal() {
        let prefix = Some(Arc::<str>::from("wr/ecommerce/"));
        assert!(scoped_key(&prefix, "../../other/secret").is_err());
    }

    #[test]
    fn test_scoped_key_normalizes_dots() {
        let prefix = Some(Arc::<str>::from("wr/ecommerce/"));
        assert_eq!(scoped_key(&prefix, "a/../b").unwrap(), "wr/ecommerce/b");
    }

    #[test]
    fn test_unscoped_key_strips_prefix() {
        let prefix = Some(Arc::<str>::from("wr/ecommerce/"));
        assert_eq!(unscoped_key(&prefix, "wr/ecommerce/file.txt"), "file.txt");
    }

    #[test]
    fn test_unscoped_key_without_prefix() {
        assert_eq!(unscoped_key(&None, "file.txt"), "file.txt");
    }

    #[test]
    fn test_unscoped_key_missing_prefix_is_passthrough() {
        let prefix = Some(Arc::<str>::from("wr/other/"));
        assert_eq!(unscoped_key(&prefix, "file.txt"), "file.txt");
    }

    // ── BlobstoreRuntime tests ───────────────────────────────────────────────

    #[test]
    fn test_new_runtime_from_config() {
        let config = test_config();
        let rt = BlobstoreRuntime::new(&config).expect("should create runtime");
        assert_eq!(rt.endpoint, "http://127.0.0.1:8900");
        assert_eq!(rt.region, "us-east-1");
    }

    #[test]
    fn test_bucket_builds_and_caches() {
        let rt = BlobstoreRuntime::new(&test_config()).unwrap();
        let a = rt.bucket("my-bucket").expect("should build store");
        let b = rt.bucket("my-bucket").expect("should return cached store");
        assert!(
            Arc::ptr_eq(&a, &b),
            "second call should return cached store"
        );
    }

    // ── map_os_err tests ─────────────────────────────────────────────────────

    #[test]
    fn test_map_os_err_not_found() {
        let err = map_os_err(ObjectStoreError::NotFound {
            path: "some/key".into(),
            source: "missing".into(),
        });
        assert!(matches!(err, BlobError::NotFound(p) if p == "some/key"));
    }

    #[test]
    fn test_map_os_err_permission_denied() {
        let err = map_os_err(ObjectStoreError::PermissionDenied {
            path: "some/key".into(),
            source: "denied".into(),
        });
        assert!(matches!(err, BlobError::AccessDenied(p) if p == "some/key"));
    }

    #[test]
    fn test_map_os_err_other_is_io() {
        let err = map_os_err(ObjectStoreError::Generic {
            store: "S3",
            source: "boom".into(),
        });
        assert!(matches!(err, BlobError::Io(_)));
    }

    // ── Host no-blobstore tests ──────────────────────────────────────────────

    #[tokio::test]
    async fn test_put_object_returns_error_when_no_blobstore() {
        let mut state = ModuleState::new(
            "test".into(),
            "test".into(),
            proxy_uri(),
            test_http_pool(),
            Default::default(),
        )
        .expect("state");
        let result = Host::put_object(&mut state, "b".into(), "k".into(), vec![1]).await;
        assert!(matches!(result, Err(BlobError::Io(_))));
    }

    #[tokio::test]
    async fn test_get_object_returns_error_when_no_blobstore() {
        let mut state = ModuleState::new(
            "test".into(),
            "test".into(),
            proxy_uri(),
            test_http_pool(),
            Default::default(),
        )
        .expect("state");
        let result = Host::get_object(&mut state, "b".into(), "k".into()).await;
        assert!(matches!(result, Err(BlobError::Io(_))));
    }

    #[tokio::test]
    async fn test_delete_object_returns_error_when_no_blobstore() {
        let mut state = ModuleState::new(
            "test".into(),
            "test".into(),
            proxy_uri(),
            test_http_pool(),
            Default::default(),
        )
        .expect("state");
        let result = Host::delete_object(&mut state, "b".into(), "k".into()).await;
        assert!(matches!(result, Err(BlobError::Io(_))));
    }

    #[tokio::test]
    async fn test_list_objects_returns_error_when_no_blobstore() {
        let mut state = ModuleState::new(
            "test".into(),
            "test".into(),
            proxy_uri(),
            test_http_pool(),
            Default::default(),
        )
        .expect("state");
        let result = Host::list_objects(&mut state, "b".into(), None).await;
        assert!(matches!(result, Err(BlobError::Io(_))));
    }

    #[tokio::test]
    async fn test_head_object_returns_error_when_no_blobstore() {
        let mut state = ModuleState::new(
            "test".into(),
            "test".into(),
            proxy_uri(),
            test_http_pool(),
            Default::default(),
        )
        .expect("state");
        let result = Host::head_object(&mut state, "b".into(), "k".into()).await;
        assert!(matches!(result, Err(BlobError::Io(_))));
    }
}
