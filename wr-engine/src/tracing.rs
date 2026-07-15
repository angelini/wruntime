use wasmtime::component::Resource;

use crate::state::{CounterGuard, ModuleState, ResourceKind};

/// Host-side state for an active WIT `active-span` resource.
///
/// Holds the tracing span for the duration of the WASM span's lifetime.
/// When the WASM module drops the `active-span` resource, `HostActiveSpan::drop`
/// is called, deleting `SpanState` from the table and dropping the `tracing::Span`,
/// which closes the span in the OTel pipeline.
pub struct SpanState {
    span: tracing::Span,
    _count: CounterGuard,
}

wasmtime::component::bindgen!({
    path: "../wit/tracing.wit",
    world: "tracing-access",
    imports: { default: async | trappable },
    with: {
        "wruntime:tracing/span.active-span": SpanState,
    },
});

use wruntime::tracing::span::AttributeValue;

fn to_otel_value(value: AttributeValue) -> opentelemetry::Value {
    match value {
        AttributeValue::Text(value) => opentelemetry::Value::String(value.into()),
        AttributeValue::Boolean(value) => opentelemetry::Value::Bool(value),
        AttributeValue::Signed(value) => opentelemetry::Value::I64(value),
        AttributeValue::Float(value) => opentelemetry::Value::F64(value),
    }
}

impl wruntime::tracing::span::Host for ModuleState {
    async fn start(
        &mut self,
        name: String,
        attrs: Vec<(String, AttributeValue)>,
    ) -> wasmtime::Result<Resource<SpanState>> {
        let (tc, table) = self.tracing_mut();
        // Parent the new span to the top of the guest span stack, falling back
        // to the request-level `active_span`. This gives automatic nesting:
        // e.g. db.query becomes a child of db.transaction.
        let parent = tc.span_stack.last().unwrap_or(&tc.active_span);
        let child = tracing::info_span!(
            parent: parent,
            "module",
            "otel.name" = name.as_str(),
            "wasm.span.name" = name.as_str()
        );
        {
            use tracing_opentelemetry::OpenTelemetrySpanExt as _;
            for (key, value) in attrs {
                child.set_attribute(opentelemetry::Key::new(key), to_otel_value(value));
            }
        }
        let guard = tc
            .accounting
            .try_track(ResourceKind::Span)
            .ok_or_else(|| wasmtime::Error::msg("tracing span cap exceeded"))?;
        let handle = table
            .push(SpanState {
                span: child.clone(),
                _count: guard,
            })
            .map_err(|_| wasmtime::Error::msg("tracing span table exhausted"))?;
        tc.span_stack.push(child);
        Ok(handle)
    }

    async fn start_root(
        &mut self,
        name: String,
        attrs: Vec<(String, AttributeValue)>,
    ) -> wasmtime::Result<Resource<SpanState>> {
        let root = tracing::info_span!(
            parent: tracing::Span::none(),
            "module",
            "otel.name" = name.as_str(),
            "wasm.span.name" = name.as_str()
        );
        {
            use tracing_opentelemetry::OpenTelemetrySpanExt as _;
            for (key, value) in attrs {
                root.set_attribute(opentelemetry::Key::new(key), to_otel_value(value));
            }
        }
        let (tc, table) = self.tracing_mut();
        let guard = tc
            .accounting
            .try_track(ResourceKind::Span)
            .ok_or_else(|| wasmtime::Error::msg("tracing span cap exceeded"))?;
        let handle = table
            .push(SpanState {
                span: root.clone(),
                _count: guard,
            })
            .map_err(|_| wasmtime::Error::msg("tracing span table exhausted"))?;
        // Set as outbound parent so subsequent HTTP calls inherit this trace.
        *tc.outbound_parent.lock().unwrap() = Some(root.clone());
        tc.span_stack.push(root);
        Ok(handle)
    }
}

impl wruntime::tracing::span::HostActiveSpan for ModuleState {
    async fn set_attribute(
        &mut self,
        self_: Resource<SpanState>,
        key: String,
        value: AttributeValue,
    ) -> wasmtime::Result<()> {
        if let Ok(state) = self.table().get(&self_) {
            use tracing_opentelemetry::OpenTelemetrySpanExt as _;
            state
                .span
                .set_attribute(opentelemetry::Key::new(key), to_otel_value(value));
        }
        Ok(())
    }

    async fn record_event(
        &mut self,
        self_: Resource<SpanState>,
        name: String,
        attrs: Vec<(String, AttributeValue)>,
    ) -> wasmtime::Result<()> {
        if let Ok(state) = self.table().get(&self_) {
            use tracing_opentelemetry::OpenTelemetrySpanExt as _;
            let attrs = attrs
                .into_iter()
                .map(|(key, value)| opentelemetry::KeyValue::new(key, to_otel_value(value)))
                .collect();
            state.span.add_event(name, attrs);
        }
        Ok(())
    }

    async fn set_error(
        &mut self,
        self_: Resource<SpanState>,
        message: String,
    ) -> wasmtime::Result<()> {
        if let Ok(state) = self.table().get(&self_) {
            state.span.in_scope(|| {
                tracing::error!(
                    "otel.status_code" = "ERROR",
                    "exception.message" = message.as_str(),
                );
            });
        }
        Ok(())
    }

    async fn drop(&mut self, self_: Resource<SpanState>) -> wasmtime::Result<()> {
        let state = self.table().delete(self_)?;
        let tc = self.tracing_context();
        // Remove this span from the stack so subsequent spans don't parent to it.
        if let Some(pos) = tc.span_stack.iter().position(|s| s.id() == state.span.id()) {
            tc.span_stack.remove(pos);
        }
        // If this span was the outbound parent, clear it so subsequent
        // outbound calls start fresh traces again.
        {
            let mut parent = tc.outbound_parent.lock().unwrap();
            if parent.as_ref().and_then(|s| s.id()) == state.span.id() {
                *parent = None;
            }
        }
        // SpanState drops here → tracing::Span drops → span ends in OTLP
        Ok(())
    }
}

pub use wruntime::tracing::span::add_to_linker;

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::wruntime::tracing::span::{AttributeValue, Host, HostActiveSpan};
    use crate::state::ModuleState;

    fn proxy_uri() -> hyper::Uri {
        "http://127.0.0.1:9001".parse().unwrap()
    }

    fn test_http_pool() -> wr_common::http_pool::HttpClientPool<http_body_util::Full<bytes::Bytes>>
    {
        wr_common::http_pool::HttpClientPool::new(1)
    }

    #[tokio::test]
    async fn test_start_returns_valid_handle() {
        let mut state = ModuleState::new(
            "test".into(),
            "test".into(),
            proxy_uri(),
            test_http_pool(),
            Default::default(),
        )
        .expect("state");
        let span = Host::start(&mut state, "my-operation".into(), vec![])
            .await
            .expect("start");
        HostActiveSpan::drop(&mut state, span).await.expect("drop");
    }

    #[tokio::test]
    async fn test_set_attribute_on_span() {
        let mut state = ModuleState::new(
            "test".into(),
            "test".into(),
            proxy_uri(),
            test_http_pool(),
            Default::default(),
        )
        .expect("state");
        let span = Host::start(&mut state, "op".into(), vec![])
            .await
            .expect("start");
        let rep = span.rep();
        HostActiveSpan::set_attribute(
            &mut state,
            wasmtime::component::Resource::new_borrow(rep),
            "db.table".into(),
            AttributeValue::Text("users".into()),
        )
        .await
        .expect("set_attribute");
        HostActiveSpan::drop(&mut state, span).await.expect("drop");
    }

    #[tokio::test]
    async fn test_record_event_on_span() {
        let mut state = ModuleState::new(
            "test".into(),
            "test".into(),
            proxy_uri(),
            test_http_pool(),
            Default::default(),
        )
        .expect("state");
        let span = Host::start(&mut state, "op".into(), vec![])
            .await
            .expect("start");
        let rep = span.rep();
        HostActiveSpan::record_event(
            &mut state,
            wasmtime::component::Resource::new_borrow(rep),
            "cache.miss".into(),
            vec![("key".into(), AttributeValue::Text("user:42".into()))],
        )
        .await
        .expect("record_event");
        HostActiveSpan::drop(&mut state, span).await.expect("drop");
    }

    #[tokio::test]
    async fn test_set_error_on_span() {
        let mut state = ModuleState::new(
            "test".into(),
            "test".into(),
            proxy_uri(),
            test_http_pool(),
            Default::default(),
        )
        .expect("state");
        let span = Host::start(&mut state, "op".into(), vec![])
            .await
            .expect("start");
        let rep = span.rep();
        HostActiveSpan::set_error(
            &mut state,
            wasmtime::component::Resource::new_borrow(rep),
            "connection refused".into(),
        )
        .await
        .expect("set_error");
        HostActiveSpan::drop(&mut state, span).await.expect("drop");
    }
}
