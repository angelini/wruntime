use anyhow::Result;
use opentelemetry::global;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    logs::SdkLoggerProvider,
    metrics::SdkMeterProvider,
    propagation::TraceContextPropagator,
    trace::{RandomIdGenerator, Sampler, SdkTracerProvider},
    Resource,
};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

const OTLP_ENDPOINT: &str = "http://localhost:4317";

/// Holds provider handles and shuts them down cleanly when dropped.
pub struct TelemetryGuard {
    tracer_provider: SdkTracerProvider,
    meter_provider: SdkMeterProvider,
    logger_provider: SdkLoggerProvider,
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        if let Err(e) = self.tracer_provider.force_flush() {
            eprintln!("tracer force_flush error: {e}");
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
/// All three signals are exported via OTLP/gRPC to [`OTLP_ENDPOINT`] unless
/// `OTEL_SDK_DISABLED` is true or 1.
/// The returned [`TelemetryGuard`] must be kept alive for the duration of the
/// process — dropping it flushes and shuts down all providers.
pub fn init(service_name: &'static str) -> Result<TelemetryGuard> {
    let otel_disabled = std::env::var("OTEL_SDK_DISABLED")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);

    let resource = Resource::builder().with_service_name(service_name).build();

    let (tracer_provider, meter_provider, logger_provider) = if otel_disabled {
        // No-op providers — no exporters, no network connections.
        let tracer_provider = SdkTracerProvider::builder()
            .with_resource(resource.clone())
            .build();
        let meter_provider = SdkMeterProvider::builder()
            .with_resource(resource.clone())
            .build();
        let logger_provider = SdkLoggerProvider::builder()
            .with_resource(resource.clone())
            .build();
        (tracer_provider, meter_provider, logger_provider)
    } else {
        // ── Traces ────────────────────────────────────────────────────────
        let trace_exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(OTLP_ENDPOINT)
            .build()?;

        let tracer_provider = SdkTracerProvider::builder()
            .with_sampler(Sampler::AlwaysOn)
            .with_id_generator(RandomIdGenerator::default())
            .with_resource(resource.clone())
            .with_batch_exporter(trace_exporter)
            .build();

        // ── Metrics ───────────────────────────────────────────────────────
        let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_tonic()
            .with_endpoint(OTLP_ENDPOINT)
            .build()?;

        let meter_provider = SdkMeterProvider::builder()
            .with_resource(resource.clone())
            .with_periodic_exporter(metric_exporter)
            .build();

        // ── Logs ──────────────────────────────────────────────────────────
        let log_exporter = opentelemetry_otlp::LogExporter::builder()
            .with_tonic()
            .with_endpoint(OTLP_ENDPOINT)
            .build()?;

        let logger_provider = SdkLoggerProvider::builder()
            .with_resource(resource.clone())
            .with_batch_exporter(log_exporter)
            .build();

        (tracer_provider, meter_provider, logger_provider)
    };

    global::set_tracer_provider(tracer_provider.clone());
    global::set_text_map_propagator(TraceContextPropagator::new());
    let tracer = tracer_provider.tracer(service_name);
    global::set_meter_provider(meter_provider.clone());

    // ── tracing subscriber ────────────────────────────────────────────────
    // Three layers:
    //   fmt           — human-readable output to stdout
    //   otel tracing  — bridges tracing spans → OTel trace spans
    //   otel logs     — bridges tracing events → OTel log records
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
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
    let _ = span.set_parent(cx);
}
