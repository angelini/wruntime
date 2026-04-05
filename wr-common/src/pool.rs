use deadpool_postgres::{Config, Pool, PoolConfig, Runtime};

pub fn build_pool(database_url: &str, max_size: usize) -> anyhow::Result<Pool> {
    let mut cfg = Config::new();
    cfg.url = Some(database_url.to_string());
    cfg.pool = Some(PoolConfig {
        max_size,
        ..Default::default()
    });
    cfg.create_pool(Some(Runtime::Tokio1), tokio_postgres::NoTls)
        .map_err(Into::into)
}

/// Format a `tokio_postgres::Error` with its full source chain.
///
/// `tokio_postgres::Error::fmt` just prints "db error" for database errors —
/// the actual message (column name, constraint, syntax detail) lives in the
/// `source()` chain. This helper walks the chain so callers see the real
/// Postgres error instead of the opaque "db error" string.
pub fn pg_error_string(e: &tokio_postgres::Error) -> String {
    use std::error::Error;
    let mut msg = e.to_string();
    let mut source = e.source();
    while let Some(cause) = source {
        msg.push_str(": ");
        msg.push_str(&cause.to_string());
        source = cause.source();
    }
    msg
}
