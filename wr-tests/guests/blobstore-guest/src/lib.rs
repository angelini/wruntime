#[allow(dead_code)]
mod proto {
    include!(concat!(env!("OUT_DIR"), "/test.rs"));
}

#[allow(dead_code, unused_imports)]
mod bindings;

use wr_sdk::bindings::wasi::http::types::{IncomingRequest, ResponseOutparam};
use wr_sdk::bindings::wruntime::blobstore::store::{self, BlobError};
use wr_sdk::io::{read_body, send_response};
use wr_sdk::ServiceError;

struct Component;
wr_sdk::export!(Component with_types_in wr_sdk::bindings);

impl wr_sdk::ServiceGuest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        let path = request.path_with_query().unwrap_or_default();
        let body = read_body(request.consume().unwrap());
        let (status, resp) = proto::blobstore_test_service_router(&Component, &path, &body);
        send_response(response_out, status, resp);
    }
}

impl proto::BlobstoreTestService for Component {
    fn put(&self, req: proto::PutRequest) -> Result<proto::PutResponse, ServiceError> {
        store::put_object(&req.bucket, &req.key, &req.data)
            .map_err(|e| ServiceError::internal(format!("{e:?}")))?;
        Ok(proto::PutResponse {})
    }

    fn get(&self, req: proto::GetRequest) -> Result<proto::GetResponse, ServiceError> {
        let data = store::get_object(&req.bucket, &req.key)
            .map_err(|e| ServiceError::internal(format!("{e:?}")))?;
        Ok(proto::GetResponse { data })
    }

    fn delete(&self, req: proto::DeleteRequest) -> Result<proto::DeleteResponse, ServiceError> {
        store::delete_object(&req.bucket, &req.key)
            .map_err(|e| ServiceError::internal(format!("{e:?}")))?;
        Ok(proto::DeleteResponse {})
    }

    fn list(&self, req: proto::ListRequest) -> Result<proto::ListResponse, ServiceError> {
        let prefix = if req.prefix.is_empty() {
            None
        } else {
            Some(req.prefix.as_str())
        };
        let objects = store::list_objects(&req.bucket, prefix)
            .map_err(|e| ServiceError::internal(format!("{e:?}")))?;
        let proto_objects = objects
            .into_iter()
            .map(|o| proto::ObjectMeta {
                key: o.key,
                size: o.size,
                last_modified: o.last_modified,
                etag: o.etag,
            })
            .collect();
        Ok(proto::ListResponse {
            objects: proto_objects,
        })
    }

    fn head(&self, req: proto::HeadRequest) -> Result<proto::HeadResponse, ServiceError> {
        let meta = store::head_object(&req.bucket, &req.key)
            .map_err(|e| ServiceError::internal(format!("{e:?}")))?;
        Ok(proto::HeadResponse {
            key: meta.key,
            size: meta.size,
            last_modified: meta.last_modified,
            etag: meta.etag,
        })
    }

    fn round_trip(
        &self,
        req: proto::RoundTripRequest,
    ) -> Result<proto::RoundTripResponse, ServiceError> {
        store::put_object(&req.bucket, &req.key, &req.data)
            .map_err(|e| ServiceError::internal(format!("{e:?}")))?;
        let fetched = store::get_object(&req.bucket, &req.key)
            .map_err(|e| ServiceError::internal(format!("{e:?}")))?;
        let matches = fetched == req.data;
        Ok(proto::RoundTripResponse {
            data: fetched,
            matches,
        })
    }

    fn not_found(
        &self,
        req: proto::NotFoundRequest,
    ) -> Result<proto::NotFoundResponse, ServiceError> {
        match store::get_object(&req.bucket, &req.key) {
            Ok(_) => Ok(proto::NotFoundResponse {
                error_kind: "none".into(),
                error_message: "unexpectedly succeeded".into(),
            }),
            Err(e) => {
                let (kind, msg) = match e {
                    BlobError::NotFound(m) => ("not-found", m),
                    BlobError::AccessDenied(m) => ("access-denied", m),
                    BlobError::Io(m) => ("io", m),
                };
                Ok(proto::NotFoundResponse {
                    error_kind: kind.into(),
                    error_message: msg,
                })
            }
        }
    }
}
