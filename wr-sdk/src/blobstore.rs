pub use crate::bindings::wruntime::blobstore::store as raw;
pub use raw::{BlobError, ObjectMeta};

use crate::ServiceError;

fn normalize_path(value: &str, allow_empty: bool) -> Result<String, ServiceError> {
    let mut segments = Vec::new();
    for segment in value.split('/') {
        match segment {
            "" | "." => {}
            ".." => {
                if segments.pop().is_none() {
                    return Err(ServiceError::bad_request(
                        "object path traverses above root",
                    ));
                }
            }
            segment => segments.push(segment),
        }
    }
    let normalized = segments.join("/");
    if normalized.is_empty() && !allow_empty {
        return Err(ServiceError::bad_request("object key must not be empty"));
    }
    Ok(normalized)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectKey(String);

impl ObjectKey {
    pub fn parse(value: &str) -> Result<Self, ServiceError> {
        normalize_path(value, false).map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectPrefix(String);

impl ObjectPrefix {
    pub fn parse(value: &str) -> Result<Self, ServiceError> {
        normalize_path(value, true).map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BucketName(String);

impl BucketName {
    pub fn parse(value: &str) -> Result<Self, ServiceError> {
        let valid = (3..=63).contains(&value.len())
            && value
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'.')
            && value
                .as_bytes()
                .first()
                .is_some_and(u8::is_ascii_alphanumeric)
            && value
                .as_bytes()
                .last()
                .is_some_and(u8::is_ascii_alphanumeric)
            && !value.contains("..")
            && !value.contains(".-")
            && !value.contains("-.");
        if valid {
            Ok(Self(value.to_string()))
        } else {
            Err(ServiceError::bad_request("invalid S3 bucket name"))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Bucket {
    name: BucketName,
}

pub fn bucket(name: &str) -> Result<Bucket, ServiceError> {
    Ok(Bucket {
        name: BucketName::parse(name)?,
    })
}

impl Bucket {
    pub fn name(&self) -> &BucketName {
        &self.name
    }

    pub fn put(&self, key: &str, data: &[u8]) -> Result<(), ServiceError> {
        let key = ObjectKey::parse(key)?;
        raw::put_object(self.name.as_str(), key.as_str(), data).map_err(Into::into)
    }

    pub fn get(&self, key: &str) -> Result<Vec<u8>, ServiceError> {
        let key = ObjectKey::parse(key)?;
        raw::get_object(self.name.as_str(), key.as_str()).map_err(Into::into)
    }

    pub fn delete(&self, key: &str) -> Result<(), ServiceError> {
        let key = ObjectKey::parse(key)?;
        raw::delete_object(self.name.as_str(), key.as_str()).map_err(Into::into)
    }

    pub fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, ServiceError> {
        let prefix = ObjectPrefix::parse(prefix)?;
        let prefix = (!prefix.as_str().is_empty()).then_some(prefix.as_str());
        raw::list_objects(self.name.as_str(), prefix).map_err(Into::into)
    }

    pub fn head(&self, key: &str) -> Result<ObjectMeta, ServiceError> {
        let key = ObjectKey::parse(key)?;
        raw::head_object(self.name.as_str(), key.as_str()).map_err(Into::into)
    }
}

impl From<BlobError> for ServiceError {
    fn from(e: BlobError) -> Self {
        match e {
            BlobError::NotFound(msg) => ServiceError::not_found(format!("blobstore: {msg}")),
            BlobError::AccessDenied(msg) => {
                ServiceError::internal(format!("blobstore access denied: {msg}"))
            }
            BlobError::Io(msg) => ServiceError::internal(format!("blobstore io: {msg}")),
            BlobError::TooLarge(msg) => {
                ServiceError::internal(format!("blobstore too large: {msg}"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_blob_names_validate_and_normalize() {
        let key = ObjectKey::parse("a//./b").unwrap_or_else(|_| panic!("valid object key"));
        assert_eq!(key.as_str(), "a/b");
        assert!(ObjectKey::parse("../secret").is_err());
        assert!(ObjectKey::parse("").is_err());
        let empty_prefix = match ObjectPrefix::parse("") {
            Ok(prefix) => prefix,
            Err(_) => panic!("empty object prefix must be accepted"),
        };
        assert_eq!(empty_prefix.as_str(), "");

        let normalized_prefix = match ObjectPrefix::parse("daily//./reports/") {
            Ok(prefix) => prefix,
            Err(_) => panic!("valid object prefix must be accepted"),
        };
        assert_eq!(normalized_prefix.as_str(), "daily/reports");
        assert!(ObjectPrefix::parse("../../secret").is_err());
        assert!(BucketName::parse("Bad_Bucket").is_err());
        assert!(BucketName::parse("valid-bucket").is_ok());
        assert!(bucket("Bad_Bucket").is_err());
        let valid_bucket = match bucket("valid-bucket") {
            Ok(bucket) => bucket,
            Err(_) => panic!("valid bucket must be accepted"),
        };
        assert_eq!(valid_bucket.name().as_str(), "valid-bucket");
    }
}
