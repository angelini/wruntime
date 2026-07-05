use anyhow::{Context, Result};
use tracing::info;

const V1_SQL: &str = include_str!("../migrations/V1__initial.sql");
const V2_SQL: &str = include_str!("../migrations/V2__secrets.sql");
const V3_SQL: &str = include_str!("../migrations/V3__managers.sql");
const V4_SQL: &str = include_str!("../migrations/V4__engine_heartbeats.sql");
const V5_SQL: &str = include_str!("../migrations/V5__schedules.sql");
const V6_SQL: &str = include_str!("../migrations/V6__peer_address.sql");
const V7_SQL: &str = include_str!("../migrations/V7__system_schema.sql");
const V8_SQL: &str = include_str!("../migrations/V8__module_heartbeats.sql");
const V9_SQL: &str = include_str!("../migrations/V9__schedule_leases.sql");

const MIGRATIONS: &[(i32, &str)] = &[
    (1, V1_SQL),
    (2, V2_SQL),
    (3, V3_SQL),
    (4, V4_SQL),
    (5, V5_SQL),
    (6, V6_SQL),
    (7, V7_SQL),
    (8, V8_SQL),
    (9, V9_SQL),
];

/// Run all pending manager migrations.
///
/// The entire run is serialized across active-active managers by a
/// transaction-scoped advisory lock (`pg_advisory_xact_lock`): the first
/// manager to arrive holds it for the whole run; any other blocks until the
/// first commits, then — under READ COMMITTED (tokio-postgres' default) — sees
/// every version already recorded and applies nothing. The lock auto-releases
/// on commit/rollback, so there is no leaked-lock path if a migration errors.
///
/// The connection's `search_path` determines where tables are created.
/// In production the pool sets `search_path = wr_system`; tests use an
/// isolated per-test schema.
pub async fn run_migrations(client: &mut deadpool_postgres::Object) -> Result<()> {
    // Include `public` in the search_path so V7 can find pre-migration tables
    // that still live in public and move them to the target schema. Applied to
    // the session (outside the transaction) so it persists for the whole run.
    let row = client
        .query_one("SHOW search_path", &[])
        .await
        .context("failed to read search_path")?;
    let current: String = row.get(0);
    if !current.contains("public") {
        client
            .batch_execute(&format!("SET search_path = {current}, public"))
            .await
            .context("failed to append public to search_path")?;
    }

    let txn = client
        .transaction()
        .await
        .context("failed to open migration transaction")?;

    // Must be the first statement: it guards the create-table + check-run-record
    // sequence below against a concurrent manager racing the same schema.
    txn.batch_execute("SELECT pg_advisory_xact_lock(hashtext('wr-manager-migrations'))")
        .await
        .context("failed to acquire migration advisory lock")?;

    txn.batch_execute(
        "CREATE TABLE IF NOT EXISTS wr_migrations (
                version INT PRIMARY KEY,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )",
    )
    .await
    .context("failed to create wr_migrations table")?;

    for &(version, sql) in MIGRATIONS {
        let applied: bool = txn
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

        txn.batch_execute(sql)
            .await
            .with_context(|| format!("migration V{version} failed"))?;
        txn.execute(
            "INSERT INTO wr_migrations (version) VALUES ($1)",
            &[&version],
        )
        .await
        .with_context(|| format!("failed to record migration V{version}"))?;

        info!(version, "migration applied");
    }

    txn.commit()
        .await
        .context("failed to commit manager migrations")?;

    Ok(())
}
