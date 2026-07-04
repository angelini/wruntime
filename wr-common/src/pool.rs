use deadpool_postgres::{Config, Pool, PoolConfig, Runtime};

pub fn build_pool(database_url: &str, max_size: usize) -> anyhow::Result<Pool> {
    build_pool_with_options(database_url, max_size, None)
}

/// Build a pool that sets `search_path` on every connection via libpq options.
pub fn build_pool_with_search_path(
    database_url: &str,
    max_size: usize,
    search_path: &str,
) -> anyhow::Result<Pool> {
    build_pool_with_options(
        database_url,
        max_size,
        Some(format!("-c search_path={search_path}")),
    )
}

fn build_pool_with_options(
    database_url: &str,
    max_size: usize,
    options: Option<String>,
) -> anyhow::Result<Pool> {
    let mut cfg = Config::new();
    cfg.url = Some(database_url.to_string());
    cfg.options = options;
    cfg.pool = Some(PoolConfig {
        max_size,
        ..Default::default()
    });
    cfg.create_pool(Some(Runtime::Tokio1), tokio_postgres::NoTls)
        .map_err(Into::into)
}

/// Redact credentials from a database URL before including it in logs or errors.
pub fn redact_database_url(database_url: &str) -> String {
    let Some((scheme, rest)) = database_url.split_once("://") else {
        return if database_url.contains('@') {
            "<redacted database url>".to_string()
        } else {
            database_url.to_string()
        };
    };

    let split_at = rest.find(|c| c == '/' || c == '?').unwrap_or(rest.len());
    let (authority, path_and_query) = rest.split_at(split_at);
    let redacted_authority = match authority.rsplit_once('@') {
        Some((userinfo, host)) => {
            let redacted_userinfo = userinfo
                .split_once(':')
                .map(|(user, _)| format!("{user}:***"))
                .unwrap_or_else(|| userinfo.to_string());
            format!("{redacted_userinfo}@{host}")
        }
        None => authority.to_string(),
    };

    format!("{scheme}://{redacted_authority}{path_and_query}")
}

/// Build a connection URL by replacing the user:password in `admin_url` with
/// `role` and `password`. Preserves host, port, dbname, and query params.
pub fn guest_pool_url(admin_url: &str, role: &str, password: &str) -> String {
    // Parse: postgres://user:pass@host:port/db?params
    let Some(after_scheme) = admin_url.split_once("://") else {
        return admin_url.to_string();
    };
    let (scheme, rest) = (after_scheme.0, after_scheme.1);
    let host_and_rest = match rest.split_once('@') {
        Some((_, h)) => h,
        None => rest,
    };
    format!("{scheme}://{role}:{password}@{host_and_rest}")
}

/// Build a `deadpool_postgres` pool using per-namespace credentials.
pub fn build_guest_pool(
    admin_url: &str,
    role: &str,
    password: &str,
    max_size: usize,
) -> anyhow::Result<Pool> {
    let url = guest_pool_url(admin_url, role, password);
    build_pool(&url, max_size)
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

#[cfg(test)]
mod tests {
    use super::{guest_pool_url, redact_database_url};

    #[test]
    fn test_redact_database_url_with_password() {
        assert_eq!(
            redact_database_url("postgres://admin:secret@localhost:5432/mydb?sslmode=require"),
            "postgres://admin:***@localhost:5432/mydb?sslmode=require"
        );
    }

    #[test]
    fn test_redact_database_url_without_password() {
        assert_eq!(
            redact_database_url("postgres://postgres@localhost:5433/wruntime_example"),
            "postgres://postgres@localhost:5433/wruntime_example"
        );
    }

    #[test]
    fn test_guest_pool_url_with_user_pass() {
        assert_eq!(
            guest_pool_url(
                "postgres://admin:secret@localhost:5432/mydb",
                "wr_ns_ecommerce",
                "abc123"
            ),
            "postgres://wr_ns_ecommerce:abc123@localhost:5432/mydb"
        );
    }

    #[test]
    fn test_guest_pool_url_user_only() {
        assert_eq!(
            guest_pool_url(
                "postgres://postgres@localhost:5433/wruntime_example",
                "wr_ns_payments",
                "pw"
            ),
            "postgres://wr_ns_payments:pw@localhost:5433/wruntime_example"
        );
    }

    #[test]
    fn test_guest_pool_url_with_query_params() {
        assert_eq!(
            guest_pool_url(
                "postgres://admin:pass@host:5432/db?sslmode=require",
                "role",
                "pw"
            ),
            "postgres://role:pw@host:5432/db?sslmode=require"
        );
    }
}
