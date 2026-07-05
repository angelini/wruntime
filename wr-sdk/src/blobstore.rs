use crate::bindings::wruntime::blobstore::store::BlobError;
use crate::ServiceError;

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
