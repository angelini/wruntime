use wasmtime::component::Resource;

use crate::state::ModuleState;

/// Host-side state for an active WIT `active-span` resource.
///
/// Holds the tracing span for the duration of the WASM span's lifetime.
/// When the WASM module drops the `active-span` resource, `HostActiveSpan::drop`
/// is called, deleting `SpanState` from the table and dropping the `tracing::Span`,
/// which closes the span in the OTel pipeline.
pub struct SpanState {
    span: tracing::Span,
}

wasmtime::component::bindgen!({
    path: "../wit/tracing.wit",
    world: "tracing-access",
    imports: { default: async },
    with: {
        "wruntime:tracing/span.active-span": SpanState,
    },
});

impl wruntime::tracing::span::Host for ModuleState {
    async fn start(&mut self, name: String, attrs: Vec<(String, String)>) -> Resource<SpanState> {
        // Parent the new span to the top of the guest span stack, falling back
        // to the request-level `active_span`. This gives automatic nesting:
        // e.g. db.query becomes a child of db.transaction.
        let parent = self.span_stack.last().unwrap_or(&self.active_span);
        let child = tracing::info_span!(
            parent: parent,
            "module",
            "otel.name" = name.as_str(),
            "wasm.span.name" = name.as_str()
        );
        {
            use tracing_opentelemetry::OpenTelemetrySpanExt as _;
            for (key, value) in attrs {
                child.set_attribute(
                    opentelemetry::Key::new(key),
                    opentelemetry::Value::String(value.into()),
                );
            }
        }
        self.span_stack.push(child.clone());
        self.table()
            .push(SpanState { span: child })
            .expect("ResourceTable capacity exceeded")
    }

    async fn start_root(
        &mut self,
        name: String,
        attrs: Vec<(String, String)>,
    ) -> Resource<SpanState> {
        let root = tracing::info_span!(
            parent: tracing::Span::none(),
            "module",
            "otel.name" = name.as_str(),
            "wasm.span.name" = name.as_str()
        );
        {
            use tracing_opentelemetry::OpenTelemetrySpanExt as _;
            for (key, value) in attrs {
                root.set_attribute(
                    opentelemetry::Key::new(key),
                    opentelemetry::Value::String(value.into()),
                );
            }
        }
        // Set as outbound parent so subsequent HTTP calls inherit this trace.
        *self.outbound_parent.lock().unwrap() = Some(root.clone());
        self.span_stack.push(root.clone());
        self.table()
            .push(SpanState { span: root })
            .expect("ResourceTable capacity exceeded")
    }
}

impl wruntime::tracing::span::HostActiveSpan for ModuleState {
    async fn set_attribute(&mut self, self_: Resource<SpanState>, key: String, value: String) {
        if let Ok(state) = self.table().get(&self_) {
            use tracing_opentelemetry::OpenTelemetrySpanExt as _;
            state.span.set_attribute(
                opentelemetry::Key::new(key),
                opentelemetry::Value::String(value.into()),
            );
        }
    }

    async fn record_event(
        &mut self,
        self_: Resource<SpanState>,
        name: String,
        attrs: Vec<(String, String)>,
    ) {
        if let Ok(state) = self.table().get(&self_) {
            state.span.in_scope(|| {
                tracing::info!(event = name.as_str(), attrs = ?attrs);
            });
        }
    }

    async fn set_error(&mut self, self_: Resource<SpanState>, message: String) {
        if let Ok(state) = self.table().get(&self_) {
            state.span.in_scope(|| {
                tracing::error!(
                    "otel.status_code" = "ERROR",
                    "exception.message" = message.as_str(),
                );
            });
        }
    }

    async fn drop(&mut self, self_: Resource<SpanState>) -> wasmtime::Result<()> {
        let state = self.table().delete(self_)?;
        // Remove this span from the stack so subsequent spans don't parent to it.
        if let Some(pos) = self
            .span_stack
            .iter()
            .position(|s| s.id() == state.span.id())
        {
            self.span_stack.remove(pos);
        }
        // If this span was the outbound parent, clear it so subsequent
        // outbound calls start fresh traces again.
        {
            let mut parent = self.outbound_parent.lock().unwrap();
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
    use super::wruntime::tracing::span::{Host, HostActiveSpan};
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
        let span = Host::start(&mut state, "my-operation".into(), vec![]).await;
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
        let span = Host::start(&mut state, "op".into(), vec![]).await;
        let rep = span.rep();
        HostActiveSpan::set_attribute(
            &mut state,
            wasmtime::component::Resource::new_borrow(rep),
            "db.table".into(),
            "users".into(),
        )
        .await;
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
        let span = Host::start(&mut state, "op".into(), vec![]).await;
        let rep = span.rep();
        HostActiveSpan::record_event(
            &mut state,
            wasmtime::component::Resource::new_borrow(rep),
            "cache.miss".into(),
            vec![("key".into(), "user:42".into())],
        )
        .await;
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
        let span = Host::start(&mut state, "op".into(), vec![]).await;
        let rep = span.rep();
        HostActiveSpan::set_error(
            &mut state,
            wasmtime::component::Resource::new_borrow(rep),
            "connection refused".into(),
        )
        .await;
        HostActiveSpan::drop(&mut state, span).await.expect("drop");
    }
}
