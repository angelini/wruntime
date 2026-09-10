#[allow(dead_code)]
mod proto {
    include!(concat!(env!("OUT_DIR"), "/deployment.rs"));
}

#[allow(dead_code, unused_imports)]
mod bindings {
    wit_bindgen::generate!({
        path: "wit",
        world: "probe",
        generate_all,
    });
}

use wr_sdk::prelude::*;

struct Component;
wr_sdk::export!(Component with_types_in wr_sdk::bindings);

impl wr_sdk::ServiceGuest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        proto::probe_service_handle(&Component, request, response_out);
    }
}

impl proto::ProbeService for Component {
    fn check(&self, request: proto::CheckRequest) -> Result<proto::CheckResponse, ServiceError> {
        Ok(proto::CheckResponse {
            nonce: request.nonce,
        })
    }
}
