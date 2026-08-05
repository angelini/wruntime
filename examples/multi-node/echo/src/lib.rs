#[allow(dead_code)]
mod proto {
    include!(concat!(env!("OUT_DIR"), "/multinode.rs"));
}

#[allow(dead_code, unused_imports)]
mod bindings {
    wit_bindgen::generate!({
        path: "wit",
        world: "echo",
        generate_all,
    });
}

use wr_sdk::prelude::*;

struct Component;
wr_sdk::export!(Component with_types_in wr_sdk::bindings);

impl wr_sdk::ServiceGuest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        proto::echo_service_handle(&Component, request, response_out);
    }
}

impl proto::EchoService for Component {
    fn echo(&self, request: proto::EchoRequest) -> Result<proto::EchoResponse, ServiceError> {
        Ok(proto::EchoResponse {
            message: request.message,
        })
    }
}
