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
        match wr_sdk::http::http_rpc(&req.authority, &req.path, &req.body) {
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
