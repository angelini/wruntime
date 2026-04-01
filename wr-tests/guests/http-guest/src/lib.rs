#[allow(dead_code)]
mod proto {
    include!(concat!(env!("OUT_DIR"), "/test.rs"));
}

#[allow(dead_code, unused_imports)]
mod bindings;

use wr_sdk::bindings::wasi::http::types::{IncomingRequest, ResponseOutparam};
use wr_sdk::io::{read_body, send_response};
use wr_sdk::ServiceError;

struct Component;
wr_sdk::export!(Component with_types_in wr_sdk::bindings);

impl wr_sdk::ServiceGuest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        let path = request.path_with_query().unwrap_or_default();
        let body = read_body(request.consume().unwrap());
        let (status, resp) = proto::http_test_service_router(&Component, &path, &body);
        send_response(response_out, status, resp);
    }
}

impl proto::HttpTestService for Component {
    fn egress(&self, req: proto::EgressRequest) -> Result<proto::EgressResponse, ServiceError> {
        match wr_sdk::http::http_rpc(&req.authority, &req.path, &[]) {
            Ok((status, body)) => Ok(proto::EgressResponse {
                status: status as u32,
                body: String::from_utf8_lossy(&body).into_owned(),
            }),
            Err(e) => Err(ServiceError::internal(format!("egress call failed: {e}"))),
        }
    }

    fn echo(&self, req: proto::EchoRequest) -> Result<proto::EchoResponse, ServiceError> {
        Ok(proto::EchoResponse {
            message: format!("echo:{}", req.message),
        })
    }
}
