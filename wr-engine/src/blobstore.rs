use chrono::DateTime;
use s3::{creds::Credentials, Bucket, Region};
use std::sync::Arc;
use tokio::runtime::Handle;

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
});

use wruntime::blobstore::store::{BlobError, Host, ObjectMeta};

// ── Host implementation ───────────────────────────────────────────────────────

impl Host for ModuleState {
    fn put_object(&mut self, bucket: String, key: String, data: Vec<u8>) -> Result<(), BlobError> {
        let rt = require_blobstore(&self.blobstore)?;
        tokio::task::block_in_place(|| {
            Handle::current().block_on(async move {
                let b = rt
                    .bucket(&bucket)
                    .map_err(|e| BlobError::Io(e.to_string()))?;
                b.put_object(&key, &data)
                    .await
                    .map(|_| ())
                    .map_err(map_s3_err)
            })
        })
    }

    fn get_object(&mut self, bucket: String, key: String) -> Result<Vec<u8>, BlobError> {
        let rt = require_blobstore(&self.blobstore)?;
        tokio::task::block_in_place(|| {
            Handle::current().block_on(async move {
                let b = rt
                    .bucket(&bucket)
                    .map_err(|e| BlobError::Io(e.to_string()))?;
                b.get_object(&key)
                    .await
                    .map(|r| r.to_vec())
                    .map_err(map_s3_err)
            })
        })
    }

    fn delete_object(&mut self, bucket: String, key: String) -> Result<(), BlobError> {
        let rt = require_blobstore(&self.blobstore)?;
        tokio::task::block_in_place(|| {
            Handle::current().block_on(async move {
                let b = rt
                    .bucket(&bucket)
                    .map_err(|e| BlobError::Io(e.to_string()))?;
                b.delete_object(&key).await.map(|_| ()).map_err(map_s3_err)
            })
        })
    }

    fn list_objects(
        &mut self,
        bucket: String,
        prefix: Option<String>,
    ) -> Result<Vec<ObjectMeta>, BlobError> {
        let rt = require_blobstore(&self.blobstore)?;
        tokio::task::block_in_place(|| {
            Handle::current().block_on(async move {
                let b = rt
                    .bucket(&bucket)
                    .map_err(|e| BlobError::Io(e.to_string()))?;
                let prefix_str = prefix.unwrap_or_default();
                let mut all: Vec<ObjectMeta> = Vec::new();
                let mut token: Option<String> = None;
                loop {
                    let (page, _) = b
                        .list_page(prefix_str.clone(), None, token, None, None)
                        .await
                        .map_err(map_s3_err)?;
                    for obj in page.contents {
                        all.push(ObjectMeta {
                            key: obj.key,
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
            })
        })
    }

    fn head_object(&mut self, bucket: String, key: String) -> Result<ObjectMeta, BlobError> {
        let rt = require_blobstore(&self.blobstore)?;
        let key2 = key.clone();
        tokio::task::block_in_place(|| {
            Handle::current().block_on(async move {
                let b = rt
                    .bucket(&bucket)
                    .map_err(|e| BlobError::Io(e.to_string()))?;
                let (head, status) = b.head_object(&key).await.map_err(|e| match e {
                    s3::error::S3Error::HttpFailWithBody(404, msg) => BlobError::NotFound(msg),
                    s3::error::S3Error::HttpFailWithBody(403, msg) => BlobError::AccessDenied(msg),
                    e => BlobError::Io(e.to_string()),
                })?;
                if status == 404 {
                    return Err(BlobError::NotFound(key2));
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
            })
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
