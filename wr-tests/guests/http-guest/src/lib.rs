#[allow(dead_code)]
mod proto {
    include!(concat!(env!("OUT_DIR"), "/test.rs"));
}

#[allow(dead_code, unused_imports)]
mod bindings {
    wit_bindgen::generate!({
        path: "wit",
        world: "http-guest",
        generate_all,
    });
}

use wr_sdk::prelude::*;

struct Component;
wr_sdk::export!(Component with_types_in wr_sdk::bindings);

impl wr_sdk::ServiceGuest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        proto::http_test_service_handle(&Component, request, response_out);
    }
}

impl proto::HttpTestService for Component {
    fn egress(&self, req: proto::EgressRequest) -> Result<proto::EgressResponse, ServiceError> {
        let authority = wr_sdk::http::Authority::parse(&req.authority)?;
        let path = wr_sdk::http::PathAndQuery::parse(&req.path)?;
        let headers = [(
            wr_sdk::http::HeaderName::parse("content-type")?,
            wr_sdk::http::HeaderValue::from_bytes(b"application/x-protobuf")?,
        )];
        let response = wr_sdk::http::http_request_typed(&wr_sdk::http::TypedHttpRequest {
            authority,
            path,
            method: wr_sdk::http::Method::Post,
            headers: &headers,
            body: &req.body,
        })
        .map_err(|error| ServiceError::internal(format!("egress call failed: {error}")))?;
        Ok(proto::EgressResponse {
            status: u32::from(response.status),
            body: String::from_utf8_lossy(&response.body).into_owned(),
        })
    }

    fn get_url(&self, req: proto::GetUrlRequest) -> Result<proto::GetUrlResponse, ServiceError> {
        let rest = req.url.strip_prefix("http://").ok_or_else(|| {
            ServiceError::bad_request("test GET URL must use the local HTTP S3 endpoint")
        })?;
        let path_start = rest.find('/').unwrap_or(rest.len());
        let authority = wr_sdk::http::Authority::parse(&rest[..path_start])?;
        let path = wr_sdk::http::PathAndQuery::parse(if path_start == rest.len() {
            "/"
        } else {
            &rest[path_start..]
        })?;
        let response = wr_sdk::http::http_request_typed(&wr_sdk::http::TypedHttpRequest {
            authority,
            path,
            method: wr_sdk::http::Method::Get,
            headers: &[],
            body: &[],
        })?;
        Ok(proto::GetUrlResponse {
            status: u32::from(response.status),
            body: response.body,
        })
    }

    fn echo(&self, req: proto::EchoRequest) -> Result<proto::EchoResponse, ServiceError> {
        Ok(proto::EchoResponse {
            message: format!("echo:{}", req.message),
        })
    }
}
