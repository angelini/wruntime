use anyhow::{Context, Result};
use tracing::info;

const V1_SQL: &str = include_str!("../migrations/V1__initial.sql");
const V2_SQL: &str = include_str!("../migrations/V2__secrets.sql");
const V3_SQL: &str = include_str!("../migrations/V3__managers.sql");
const V4_SQL: &str = include_str!("../migrations/V4__engine_heartbeats.sql");
const V5_SQL: &str = include_str!("../migrations/V5__schedules.sql");

const MIGRATIONS: &[(i32, &str)] = &[
    (1, V1_SQL),
    (2, V2_SQL),
    (3, V3_SQL),
    (4, V4_SQL),
    (5, V5_SQL),
];

/// Run all pending manager migrations.
///
/// Uses a simple `wr_migrations` tracking table to record which versions have
/// been applied. Each migration is executed in a transaction.
pub async fn run_migrations(client: &deadpool_postgres::Object) -> Result<()> {
    client
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS wr_migrations (
                version INT PRIMARY KEY,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )",
        )
        .await
        .context("failed to create wr_migrations table")?;

    for &(version, sql) in MIGRATIONS {
        let applied: bool = client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM wr_migrations WHERE version = $1)",
                &[&version],
            )
            .await
            .context("failed to check migration version")?
            .get(0);

        if applied {
            continue;
        }

        // Run each migration in a single batch (tokio_postgres::Client doesn't
        // support `transaction()` via &self, and batch_execute is already atomic
        // for DDL statements).
        client
            .batch_execute(sql)
            .await
            .with_context(|| format!("migration V{version} failed"))?;
        client
            .execute(
                "INSERT INTO wr_migrations (version) VALUES ($1)",
                &[&version],
            )
            .await
            .with_context(|| format!("failed to record migration V{version}"))?;

        info!(version, "migration applied");
    }

    Ok(())
}
