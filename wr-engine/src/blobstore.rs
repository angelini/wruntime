use chrono::DateTime;
use s3::{creds::Credentials, Bucket, Region};
use std::sync::Arc;

use crate::config::BlobstoreConfig;
use crate::state::ModuleState;

/// Shared S3 client configuration. Credentials are stored once per engine;
/// a `Bucket` handle is constructed on each call (cheap — no network I/O).
pub struct BlobstoreRuntime {
    endpoint: String,
    region: String,
    credentials: Credentials,
}

impl BlobstoreRuntime {
    pub fn new(config: &BlobstoreConfig) -> anyhow::Result<Self> {
        let credentials = Credentials::new(
            Some(&config.access_key_id),
            Some(&config.secret_access_key),
            None,
            None,
            None,
        )
        .map_err(|e| anyhow::anyhow!("blobstore credentials error: {e}"))?;
        Ok(Self {
            endpoint: config.endpoint.clone(),
            region: config.region.clone(),
            credentials,
        })
    }

    fn bucket(&self, name: &str) -> Result<Box<Bucket>, s3::error::S3Error> {
        let region = Region::Custom {
            region: self.region.clone(),
            endpoint: self.endpoint.clone(),
        };
        Ok(Bucket::new(name, region, self.credentials.clone())?.with_path_style())
    }
}

// ── WIT bindings ─────────────────────────────────────────────────────────────

wasmtime::component::bindgen!({
    path:  "../wit/blobstore.wit",
    world: "blobstore-access",
    imports: { default: async },
});

use wruntime::blobstore::store::{BlobError, Host, ObjectMeta};

// ── Namespace isolation helpers ───────────────────────────────────────────────

/// Prepend the namespace prefix to a key. Returns the key unchanged when no
/// prefix is configured.
fn scoped_key(prefix: &Option<String>, key: &str) -> String {
    match prefix {
        Some(p) => format!("{p}{key}"),
        None => key.to_string(),
    }
}

/// Strip the namespace prefix from a key returned by S3.
fn unscoped_key(prefix: &Option<String>, key: &str) -> String {
    match prefix {
        Some(p) => key.strip_prefix(p.as_str()).unwrap_or(key).to_string(),
        None => key.to_string(),
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
        let rt = require_blobstore(&self.blobstore)?;
        let b = rt
            .bucket(&bucket)
            .map_err(|e| BlobError::Io(e.to_string()))?;
        let full_key = scoped_key(&self.blob_prefix, &key);
        b.put_object(&full_key, &data)
            .await
            .map(|_| ())
            .map_err(map_s3_err)
    }

    async fn get_object(&mut self, bucket: String, key: String) -> Result<Vec<u8>, BlobError> {
        let rt = require_blobstore(&self.blobstore)?;
        let b = rt
            .bucket(&bucket)
            .map_err(|e| BlobError::Io(e.to_string()))?;
        let full_key = scoped_key(&self.blob_prefix, &key);
        b.get_object(&full_key)
            .await
            .map(|r| r.to_vec())
            .map_err(map_s3_err)
    }

    async fn delete_object(&mut self, bucket: String, key: String) -> Result<(), BlobError> {
        let rt = require_blobstore(&self.blobstore)?;
        let b = rt
            .bucket(&bucket)
            .map_err(|e| BlobError::Io(e.to_string()))?;
        let full_key = scoped_key(&self.blob_prefix, &key);
        b.delete_object(&full_key)
            .await
            .map(|_| ())
            .map_err(map_s3_err)
    }

    async fn list_objects(
        &mut self,
        bucket: String,
        prefix: Option<String>,
    ) -> Result<Vec<ObjectMeta>, BlobError> {
        let rt = require_blobstore(&self.blobstore)?;
        let b = rt
            .bucket(&bucket)
            .map_err(|e| BlobError::Io(e.to_string()))?;
        let scoped_prefix = scoped_key(&self.blob_prefix, &prefix.unwrap_or_default());
        let mut all: Vec<ObjectMeta> = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let (page, _) = b
                .list_page(scoped_prefix.clone(), None, token, None, None)
                .await
                .map_err(map_s3_err)?;
            for obj in page.contents {
                all.push(ObjectMeta {
                    key: unscoped_key(&self.blob_prefix, &obj.key),
                    size: obj.size,
                    last_modified: parse_last_modified(&obj.last_modified),
                    etag: obj.e_tag.unwrap_or_default(),
                });
            }
            token = page.next_continuation_token;
            if token.is_none() {
                break;
            }
        }
        Ok(all)
    }

    async fn head_object(&mut self, bucket: String, key: String) -> Result<ObjectMeta, BlobError> {
        let rt = require_blobstore(&self.blobstore)?;
        let b = rt
            .bucket(&bucket)
            .map_err(|e| BlobError::Io(e.to_string()))?;
        let full_key = scoped_key(&self.blob_prefix, &key);
        let (head, status) = b.head_object(&full_key).await.map_err(|e| match e {
            s3::error::S3Error::HttpFailWithBody(404, msg) => BlobError::NotFound(msg),
            s3::error::S3Error::HttpFailWithBody(403, msg) => BlobError::AccessDenied(msg),
            e => BlobError::Io(e.to_string()),
        })?;
        if status == 404 {
            return Err(BlobError::NotFound(key));
        }
        Ok(ObjectMeta {
            key,
            size: head.content_length.unwrap_or(0) as u64,
            last_modified: head
                .last_modified
                .as_deref()
                .map(parse_last_modified)
                .unwrap_or(0),
            etag: head.e_tag.unwrap_or_default(),
        })
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn require_blobstore(
    client: &Option<Arc<BlobstoreRuntime>>,
) -> Result<Arc<BlobstoreRuntime>, BlobError> {
    client
        .clone()
        .ok_or_else(|| BlobError::Io("no blobstore configured for this module".into()))
}

fn map_s3_err(e: s3::error::S3Error) -> BlobError {
    match e {
        s3::error::S3Error::HttpFailWithBody(404, msg) => BlobError::NotFound(msg),
        s3::error::S3Error::HttpFailWithBody(403, msg) => BlobError::AccessDenied(msg),
        e => BlobError::Io(e.to_string()),
    }
}

/// Parse an S3 last-modified string to a Unix timestamp.
/// Handles both RFC 2822 ("Thu, 28 Mar 2024 12:00:00 GMT") and
/// RFC 3339 / ISO 8601 ("2024-03-28T12:00:00.000Z").
fn parse_last_modified(s: &str) -> i64 {
    if let Ok(dt) = DateTime::parse_from_rfc2822(s) {
        return dt.timestamp();
    }
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return dt.timestamp();
    }
    0
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

    fn test_http_client() -> hyper_util::client::legacy::Client<
        hyper_util::client::legacy::connect::HttpConnector,
        http_body_util::Full<bytes::Bytes>,
    > {
        hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
            .build_http()
    }

    fn test_config() -> BlobstoreConfig {
        BlobstoreConfig {
            endpoint: "http://127.0.0.1:8900".into(),
            access_key_id: "test-key".into(),
            secret_access_key: "test-secret".into(),
            region: "us-east-1".into(),
        }
    }

    // ── scoped_key / unscoped_key tests ───────────────────────────────────────

    #[test]
    fn test_scoped_key_with_prefix() {
        let prefix = Some("wr/ecommerce/".to_string());
        assert_eq!(scoped_key(&prefix, "file.txt"), "wr/ecommerce/file.txt");
    }

    #[test]
    fn test_scoped_key_without_prefix() {
        assert_eq!(scoped_key(&None, "file.txt"), "file.txt");
    }

    #[test]
    fn test_unscoped_key_strips_prefix() {
        let prefix = Some("wr/ecommerce/".to_string());
        assert_eq!(unscoped_key(&prefix, "wr/ecommerce/file.txt"), "file.txt");
    }

    #[test]
    fn test_unscoped_key_without_prefix() {
        assert_eq!(unscoped_key(&None, "file.txt"), "file.txt");
    }

    #[test]
    fn test_unscoped_key_missing_prefix_is_passthrough() {
        let prefix = Some("wr/other/".to_string());
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
    fn test_bucket_returns_path_style() {
        let rt = BlobstoreRuntime::new(&test_config()).unwrap();
        let bucket = rt.bucket("my-bucket").expect("should create bucket");
        assert_eq!(bucket.name(), "my-bucket");
        assert!(bucket.is_path_style());
    }

    #[test]
    fn test_bucket_uses_custom_region() {
        let mut config = test_config();
        config.region = "eu-west-1".into();
        config.endpoint = "http://s3.example.com".into();
        let rt = BlobstoreRuntime::new(&config).unwrap();
        let bucket = rt.bucket("test").expect("should create bucket");
        assert_eq!(bucket.region().to_string(), "eu-west-1");
    }

    // ── require_blobstore tests ──────────────────────────────────────────────

    #[test]
    fn test_require_blobstore_none_returns_error() {
        let result = require_blobstore(&None);
        assert!(matches!(result, Err(BlobError::Io(_))));
        if let Err(BlobError::Io(msg)) = result {
            assert!(msg.contains("no blobstore configured"));
        }
    }

    #[test]
    fn test_require_blobstore_some_returns_runtime() {
        let rt = Arc::new(BlobstoreRuntime::new(&test_config()).unwrap());
        let result = require_blobstore(&Some(rt));
        assert!(result.is_ok());
    }

    // ── map_s3_err tests ─────────────────────────────────────────────────────

    #[test]
    fn test_map_s3_err_404() {
        let err = map_s3_err(s3::error::S3Error::HttpFailWithBody(
            404,
            "not found".into(),
        ));
        assert!(matches!(err, BlobError::NotFound(msg) if msg == "not found"));
    }

    #[test]
    fn test_map_s3_err_403() {
        let err = map_s3_err(s3::error::S3Error::HttpFailWithBody(
            403,
            "forbidden".into(),
        ));
        assert!(matches!(err, BlobError::AccessDenied(msg) if msg == "forbidden"));
    }

    #[test]
    fn test_map_s3_err_other() {
        let err = map_s3_err(s3::error::S3Error::HttpFailWithBody(
            500,
            "server error".into(),
        ));
        assert!(matches!(err, BlobError::Io(_)));
    }

    // ── parse_last_modified tests ────────────────────────────────────────────

    #[test]
    fn test_parse_rfc2822() {
        // Thu, 28 Mar 2024 12:00:00 GMT
        let ts = parse_last_modified("Thu, 28 Mar 2024 12:00:00 GMT");
        assert_eq!(ts, 1711627200);
    }

    #[test]
    fn test_parse_rfc3339() {
        let ts = parse_last_modified("2024-03-28T12:00:00.000Z");
        assert_eq!(ts, 1711627200);
    }

    #[test]
    fn test_parse_rfc3339_no_millis() {
        let ts = parse_last_modified("2024-03-28T12:00:00Z");
        assert_eq!(ts, 1711627200);
    }

    #[test]
    fn test_parse_invalid_returns_zero() {
        assert_eq!(parse_last_modified("not-a-date"), 0);
        assert_eq!(parse_last_modified(""), 0);
    }

    // ── Host no-blobstore tests ──────────────────────────────────────────────

    #[tokio::test]
    async fn test_put_object_returns_error_when_no_blobstore() {
        let mut state = ModuleState::new(
            "test".into(),
            "test".into(),
            proxy_uri(),
            test_http_client(),
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
            test_http_client(),
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
            test_http_client(),
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
            test_http_client(),
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
            test_http_client(),
            Default::default(),
        )
        .expect("state");
        let result = Host::head_object(&mut state, "b".into(), "k".into()).await;
        assert!(matches!(result, Err(BlobError::Io(_))));
    }
}
