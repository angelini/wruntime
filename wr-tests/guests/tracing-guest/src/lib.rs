#[allow(dead_code)]
mod proto {
    include!(concat!(env!("OUT_DIR"), "/test.rs"));
}

#[allow(dead_code, unused_imports)]
mod bindings;

use wr_sdk::bindings::wasi::http::types::{IncomingRequest, ResponseOutparam};
use wr_sdk::io::{read_body, send_response};
use wr_sdk::tracing as sdk_tracing;
use wr_sdk::ServiceError;

struct Component;
wr_sdk::export!(Component with_types_in wr_sdk::bindings);

impl wr_sdk::ServiceGuest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        let path = request.path_with_query().unwrap_or_default();
        let body = read_body(request.consume().unwrap());
        let (status, resp) = proto::tracing_test_service_router(&Component, &path, &body);
        send_response(response_out, status, resp);
    }
}

impl proto::TracingTestService for Component {
    fn start_span(
        &self,
        req: proto::StartSpanRequest,
    ) -> Result<proto::StartSpanResponse, ServiceError> {
        let attrs: Vec<(&str, &str)> = req
            .attrs
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let _span = sdk_tracing::start(&req.name, &attrs);
        Ok(proto::StartSpanResponse { ok: true })
    }

    fn span_attributes(
        &self,
        req: proto::SpanAttributesRequest,
    ) -> Result<proto::SpanAttributesResponse, ServiceError> {
        let span = sdk_tracing::start(&req.span_name, &[]);
        for (k, v) in &req.attrs {
            sdk_tracing::set_attr(&span, k, v);
        }
        Ok(proto::SpanAttributesResponse { ok: true })
    }

    fn span_event(
        &self,
        req: proto::SpanEventRequest,
    ) -> Result<proto::SpanEventResponse, ServiceError> {
        let span = sdk_tracing::start(&req.span_name, &[]);
        let attrs: Vec<(&str, &str)> = req
            .event_attrs
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        sdk_tracing::record_event(&span, &req.event_name, &attrs);
        Ok(proto::SpanEventResponse { ok: true })
    }

    fn span_error(
        &self,
        req: proto::SpanErrorRequest,
    ) -> Result<proto::SpanErrorResponse, ServiceError> {
        let span = sdk_tracing::start(&req.span_name, &[]);
        sdk_tracing::set_error(&span, &req.message);
        Ok(proto::SpanErrorResponse { ok: true })
    }

    fn nested_spans(
        &self,
        req: proto::NestedSpansRequest,
    ) -> Result<proto::NestedSpansResponse, ServiceError> {
        let outer = sdk_tracing::start(&req.outer_name, &[("level", "outer")]);
        let inner = sdk_tracing::start(&req.inner_name, &[("level", "inner")]);
        sdk_tracing::set_attr(&inner, "nested", "true");
        sdk_tracing::record_event(&outer, "checkpoint", &[("stage", "mid")]);
        drop(inner);
        drop(outer);
        Ok(proto::NestedSpansResponse { ok: true })
    }
}
