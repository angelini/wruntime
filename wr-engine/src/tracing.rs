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
        let child = self.active_span.in_scope(|| {
            tracing::info_span!(
                "module",
                "otel.name" = name.as_str(),
                "wasm.span.name" = name.as_str()
            )
        });
        child.follows_from(self.active_span.id());
        {
            use tracing_opentelemetry::OpenTelemetrySpanExt as _;
            for (key, value) in attrs {
                child.set_attribute(
                    opentelemetry::Key::new(key),
                    opentelemetry::Value::String(value.into()),
                );
            }
        }
        self.table()
            .push(SpanState { span: child })
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
        self.table().delete(self_)?;
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
