use crate::bindings::wruntime::blobstore::store::BlobError;
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
        assert!(BucketName::parse("Bad_Bucket").is_err());
        assert!(BucketName::parse("valid-bucket").is_ok());
    }
}
