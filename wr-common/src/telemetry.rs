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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TelemetrySignal {
    Traces,
    Metrics,
    Logs,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TelemetryOperation {
    ForceFlush,
    Shutdown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TelemetryFailure {
    pub signal: TelemetrySignal,
    pub operation: TelemetryOperation,
    pub error: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TelemetryFinalizeStatus {
    Disabled,
    Finalized,
    AlreadyFinalized,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TelemetryFinalizeOutcome {
    pub status: TelemetryFinalizeStatus,
    pub failures: Vec<TelemetryFailure>,
}

impl TelemetryFinalizeOutcome {
    pub fn is_success(&self) -> bool {
        self.failures.is_empty()
    }
}

trait TelemetryFinalizer: Send {
    fn finalize(&self) -> Vec<TelemetryFailure>;
}

struct ProviderFinalizer {
    tracer_provider: SdkTracerProvider,
    meter_provider: SdkMeterProvider,
    logger_provider: SdkLoggerProvider,
}

impl TelemetryFinalizer for ProviderFinalizer {
    fn finalize(&self) -> Vec<TelemetryFailure> {
        let mut failures = Vec::new();
        if let Err(error) = self.tracer_provider.force_flush() {
            failures.push(TelemetryFailure {
                signal: TelemetrySignal::Traces,
                operation: TelemetryOperation::ForceFlush,
                error: error.to_string(),
            });
        }
        if let Err(error) = self.tracer_provider.shutdown() {
            failures.push(TelemetryFailure {
                signal: TelemetrySignal::Traces,
                operation: TelemetryOperation::Shutdown,
                error: error.to_string(),
            });
        }
        if let Err(error) = self.meter_provider.shutdown() {
            failures.push(TelemetryFailure {
                signal: TelemetrySignal::Metrics,
                operation: TelemetryOperation::Shutdown,
                error: error.to_string(),
            });
        }
        if let Err(error) = self.logger_provider.shutdown() {
            failures.push(TelemetryFailure {
                signal: TelemetrySignal::Logs,
                operation: TelemetryOperation::Shutdown,
                error: error.to_string(),
            });
        }
        failures
    }
}

/// Owns telemetry providers. Call [`TelemetryGuard::finalize`] after service
/// tasks join; `Drop` is only a quiet, non-panicking last-resort cleanup path.
pub struct TelemetryGuard {
    finalizer: Option<Box<dyn TelemetryFinalizer>>,
    disabled: bool,
    finalized: bool,
}

impl TelemetryGuard {
    /// Flush and shut down configured providers exactly once. Provider failures
    /// are returned on the first call and never printed again by `Drop`.
    #[must_use]
    pub fn finalize(&mut self) -> TelemetryFinalizeOutcome {
        if self.finalized {
            return TelemetryFinalizeOutcome {
                status: TelemetryFinalizeStatus::AlreadyFinalized,
                failures: Vec::new(),
            };
        }
        self.finalized = true;

        if self.disabled {
            self.finalizer.take();
            return TelemetryFinalizeOutcome {
                status: TelemetryFinalizeStatus::Disabled,
                failures: Vec::new(),
            };
        }

        let failures = self
            .finalizer
            .take()
            .map(|finalizer| finalizer.finalize())
            .unwrap_or_default();
        TelemetryFinalizeOutcome {
            status: TelemetryFinalizeStatus::Finalized,
            failures,
        }
    }
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        if !self.finalized {
            let _ = self.finalize();
        }
    }
}

/// Initialise OpenTelemetry (traces, metrics, logs) and the `tracing` subscriber.
///
/// All three signals are exported via OTLP/gRPC to [`OTLP_ENDPOINT`] unless
/// `OTEL_SDK_DISABLED` is true or 1.
/// Keep the returned [`TelemetryGuard`] alive for the process duration, then
/// call [`TelemetryGuard::finalize`] after all owned service tasks have joined.
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

    let finalizer = (!otel_disabled).then(|| {
        Box::new(ProviderFinalizer {
            tracer_provider,
            meter_provider,
            logger_provider,
        }) as Box<dyn TelemetryFinalizer>
    });

    Ok(TelemetryGuard {
        finalizer,
        disabled: otel_disabled,
        finalized: false,
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use super::*;

    struct TestFinalizer {
        calls: Arc<AtomicUsize>,
        fail: bool,
    }

    impl TelemetryFinalizer for TestFinalizer {
        fn finalize(&self) -> Vec<TelemetryFailure> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                vec![TelemetryFailure {
                    signal: TelemetrySignal::Traces,
                    operation: TelemetryOperation::ForceFlush,
                    error: "collector unavailable".to_owned(),
                }]
            } else {
                Vec::new()
            }
        }
    }

    fn test_guard(calls: Arc<AtomicUsize>, fail: bool) -> TelemetryGuard {
        TelemetryGuard {
            finalizer: Some(Box::new(TestFinalizer { calls, fail })),
            disabled: false,
            finalized: false,
        }
    }

    #[test]
    fn explicit_finalization_is_idempotent_and_drop_does_not_repeat_it() {
        let calls = Arc::new(AtomicUsize::new(0));
        {
            let mut guard = test_guard(Arc::clone(&calls), false);
            let first = guard.finalize();
            assert_eq!(first.status, TelemetryFinalizeStatus::Finalized);
            assert!(first.is_success());
            let second = guard.finalize();
            assert_eq!(second.status, TelemetryFinalizeStatus::AlreadyFinalized);
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn drop_performs_quiet_last_resort_cleanup_once() {
        let calls = Arc::new(AtomicUsize::new(0));
        drop(test_guard(Arc::clone(&calls), false));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn disabled_telemetry_is_a_quiet_noop() {
        let mut guard = TelemetryGuard {
            finalizer: None,
            disabled: true,
            finalized: false,
        };
        let outcome = guard.finalize();
        assert_eq!(outcome.status, TelemetryFinalizeStatus::Disabled);
        assert!(outcome.is_success());
    }

    #[test]
    fn provider_failure_is_returned_once() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut guard = test_guard(Arc::clone(&calls), true);
        let first = guard.finalize();
        assert_eq!(first.failures.len(), 1);
        assert_eq!(first.failures[0].error, "collector unavailable");
        assert!(guard.finalize().failures.is_empty());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
