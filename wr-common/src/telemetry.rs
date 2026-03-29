use anyhow::Result;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::{global, KeyValue};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    metrics::SdkMeterProvider,
    propagation::TraceContextPropagator,
    runtime,
    trace::{Config as TraceConfig, RandomIdGenerator, Sampler, TracerProvider},
    Resource,
};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

const OTLP_ENDPOINT: &str = "http://localhost:4317";

/// Holds provider handles and shuts them down cleanly when dropped.
pub struct TelemetryGuard {
    tracer_provider: TracerProvider,
    meter_provider: SdkMeterProvider,
    logger_provider: opentelemetry_sdk::logs::LoggerProvider,
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        for result in self.tracer_provider.force_flush() {
            if let Err(e) = result {
                eprintln!("tracer force_flush error: {e}");
            }
        }
        if let Err(e) = self.tracer_provider.shutdown() {
            eprintln!("tracer provider shutdown error: {e}");
        }
        if let Err(e) = self.meter_provider.shutdown() {
            eprintln!("metrics provider shutdown error: {e}");
        }
        if let Err(e) = self.logger_provider.shutdown() {
            eprintln!("logger provider shutdown error: {e}");
        }
    }
}

/// Initialise OpenTelemetry (traces, metrics, logs) and the `tracing` subscriber.
///
/// All three signals are exported via OTLP/gRPC to [`OTLP_ENDPOINT`].
/// The returned [`TelemetryGuard`] must be kept alive for the duration of the
/// process — dropping it flushes and shuts down all providers.
pub fn init(service_name: &'static str) -> Result<TelemetryGuard> {
    let resource = Resource::new(vec![KeyValue::new(
        opentelemetry_semantic_conventions::resource::SERVICE_NAME,
        service_name,
    )]);

    // ── Traces ────────────────────────────────────────────────────────────
    let tracer_provider = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(
            opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint(OTLP_ENDPOINT),
        )
        .with_trace_config(
            TraceConfig::default()
                .with_sampler(Sampler::AlwaysOn)
                .with_id_generator(RandomIdGenerator::default())
                .with_resource(resource.clone()),
        )
        .install_batch(runtime::Tokio)?;

    global::set_tracer_provider(tracer_provider.clone());

    // W3C TraceContext: propagates trace-id/span-id via `traceparent` header.
    global::set_text_map_propagator(TraceContextPropagator::new());

    // Obtain a concrete tracer to pass to the tracing_opentelemetry layer.
    // tracing_opentelemetry::layer() defaults to NoopTracer in 0.27 — with_tracer() is required.
    let tracer = tracer_provider.tracer(service_name);

    // ── Metrics ───────────────────────────────────────────────────────────
    let meter_provider = opentelemetry_otlp::new_pipeline()
        .metrics(runtime::Tokio)
        .with_exporter(
            opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint(OTLP_ENDPOINT),
        )
        .with_resource(resource.clone())
        .build()?;

    global::set_meter_provider(meter_provider.clone());

    // ── Logs ──────────────────────────────────────────────────────────────
    let logger_provider = opentelemetry_otlp::new_pipeline()
        .logging()
        .with_exporter(
            opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint(OTLP_ENDPOINT),
        )
        .with_resource(resource.clone())
        .install_batch(runtime::Tokio)?;

    // ── tracing subscriber ────────────────────────────────────────────────
    // Three layers:
    //   fmt           — human-readable output to stdout
    //   otel tracing  — bridges tracing spans → OTel trace spans
    //   otel logs     — bridges tracing events → OTel log records
    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()))
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .with(OpenTelemetryTracingBridge::new(&logger_provider))
        .init();

    Ok(TelemetryGuard {
        tracer_provider,
        meter_provider,
        logger_provider,
    })
}

/// Inject the current tracing span's OTel context into outgoing HTTP headers
/// as a W3C `traceparent` header.  Call this in any service that forwards
/// requests to another wruntime component.
pub fn inject_context(headers: &mut http::HeaderMap) {
    let cx = tracing::Span::current().context();
    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&cx, &mut opentelemetry_http::HeaderInjector(headers));
    });
}

/// Extract an OTel trace context from incoming HTTP headers and set it as the
/// parent of `span`.  Call this at the entry point of any wruntime component
/// that receives forwarded requests so the engine dispatch span is linked to
/// the originating proxy span.
pub fn set_parent_from_headers(span: &tracing::Span, headers: &http::HeaderMap) {
    let cx = global::get_text_map_propagator(|propagator| {
        propagator.extract(&opentelemetry_http::HeaderExtractor(headers))
    });
    span.set_parent(cx);
}
