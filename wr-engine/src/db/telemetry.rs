use opentelemetry::trace::Status;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

use super::wruntime::db::database::DbError;
use crate::state::ModuleState;

#[derive(Clone, Copy)]
pub(crate) enum DbOperation {
    Query,
    Execute,
    Stream,
    TransactionQuery,
    TransactionExecute,
    TransactionStream,
}

impl DbOperation {
    fn name(self) -> &'static str {
        match self {
            Self::Query => "query",
            Self::Execute => "execute",
            Self::Stream => "stream",
            Self::TransactionQuery => "transaction.query",
            Self::TransactionExecute => "transaction.execute",
            Self::TransactionStream => "transaction.stream",
        }
    }
}

pub(crate) struct DbSpan {
    span: Option<tracing::Span>,
    rows: u64,
}

impl ModuleState {
    pub(crate) fn start_db_span(&mut self, operation: DbOperation, sql: &str) -> DbSpan {
        let parent = self.guest_span_parent();
        let (namespace, include_query_text) = self.db_telemetry_config();
        DbSpan::new(
            &parent,
            namespace.as_deref(),
            operation,
            include_query_text.then_some(sql),
        )
    }
}

impl DbSpan {
    fn new(
        parent: &tracing::Span,
        namespace: Option<&str>,
        operation: DbOperation,
        query_text: Option<&str>,
    ) -> Self {
        let operation = operation.name();
        let span_name = namespace
            .map(|namespace| format!("{operation} {namespace}"))
            .unwrap_or_else(|| operation.to_string());
        let span = tracing::info_span!(
            parent: parent,
            "db.client",
            "otel.name" = span_name.as_str(),
            "otel.kind" = "client",
        );
        span.set_attribute("db.system.name", "postgresql");
        span.set_attribute("db.operation.name", operation);
        if let Some(namespace) = namespace {
            span.set_attribute("db.namespace", namespace.to_string());
        }
        if let Some(query_text) = query_text {
            span.set_attribute("db.query.text", normalize_query_text(query_text));
        }
        Self {
            span: Some(span),
            rows: 0,
        }
    }

    pub(crate) fn add_rows(&mut self, rows: usize) {
        self.rows = self.rows.saturating_add(rows as u64);
    }

    pub(crate) fn finish_result<T>(
        &mut self,
        result: &Result<T, DbError>,
        rows: impl FnOnce(&T) -> u64,
    ) {
        match result {
            Ok(value) => {
                self.rows = rows(value);
                self.finish(None);
            }
            Err(error) => self.finish(Some(error_type(error))),
        }
    }

    pub(crate) fn finish_success(&mut self) {
        self.finish(None);
    }

    pub(crate) fn finish_error(&mut self, error: &DbError) {
        self.finish(Some(error_type(error)));
    }

    pub(crate) fn finish_cancelled(&mut self) {
        self.finish(Some("cancelled"));
    }

    fn finish(&mut self, error_type: Option<&'static str>) {
        let Some(span) = self.span.take() else {
            return;
        };
        span.set_attribute(
            "db.response.returned_rows",
            i64::try_from(self.rows).unwrap_or(i64::MAX),
        );
        if let Some(error_type) = error_type {
            span.set_attribute("error.type", error_type);
            span.set_status(Status::error("database operation failed"));
        }
    }
}

impl Drop for DbSpan {
    fn drop(&mut self) {
        self.finish_cancelled();
    }
}

fn error_type(error: &DbError) -> &'static str {
    match error {
        DbError::Connection(_) => "connection",
        DbError::Query(_) => "query",
        DbError::UnsupportedResultType(_) => "unsupported_result_type",
    }
}

fn normalize_query_text(sql: &str) -> String {
    sql.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use super::normalize_query_text;

    #[test]
    fn query_text_normalization_only_collapses_whitespace() {
        assert_eq!(
            normalize_query_text(" SELECT  secret_value\nFROM\titems WHERE id = $1 "),
            "SELECT secret_value FROM items WHERE id = $1"
        );
    }
}
