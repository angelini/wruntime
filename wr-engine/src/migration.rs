use anyhow::{Context, Result};
use deadpool_postgres::Pool;
use std::path::Path;
use tracing::info;

/// Compute a deterministic advisory lock key from a schema name.
/// Uses FNV-1a for cross-build stability (unlike `DefaultHasher`).
fn advisory_lock_key(schema: &str) -> i64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in schema.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash as i64
}

/// Load V-prefixed SQL migration files from a directory.
///
/// Reads `*.sql` files matching the refinery naming convention (`V{n}__{name}.sql`)
/// directly from the given directory (non-recursive). This avoids the spurious
/// warnings that `refinery::load_sql_migrations` emits for non-migration entries
/// produced by `WalkDir`.
fn load_migrations(dir: &Path) -> Result<Vec<refinery::Migration>> {
    let mut migrations = Vec::new();
    let mut entries: Vec<_> = std::fs::read_dir(dir)
        .with_context(|| format!("failed to read migrations directory '{}'", dir.display()))?
        .filter_map(Result::ok)
        .filter(|e| {
            e.path()
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("sql"))
        })
        .collect();
    entries.sort_by_key(|e| e.file_name());

    for entry in entries {
        let path = entry.path();
        let stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .context("non-UTF8 migration filename")?;
        let sql = std::fs::read_to_string(&path)
            .with_context(|| format!("failed to read migration file '{}'", path.display()))?;
        let migration = refinery::Migration::unapplied(stem, &sql)
            .map_err(|e| anyhow::anyhow!("invalid migration '{}': {e}", path.display()))?;
        migrations.push(migration);
    }

    migrations.sort();
    Ok(migrations)
}

/// Run refinery migrations for a single module.
///
/// 1. Loads SQL files from `migrations_path` at runtime
/// 2. Acquires a Postgres advisory lock (keyed by schema name)
/// 3. Sets `search_path` to the module's schema (no `public` fallback)
/// 4. Runs pending migrations via refinery
/// 5. Releases the advisory lock
pub async fn run_module_migrations(
    pool: &Pool,
    schema: &str,
    migrations_path: &str,
    module_name: &str,
) -> Result<()> {
    let migrations = load_migrations(Path::new(migrations_path))?;

    if migrations.is_empty() {
        info!(
            module = module_name,
            path = migrations_path,
            "no migration files found, skipping"
        );
        return Ok(());
    }

    let lock_key = advisory_lock_key(schema);
    let mut client = pool
        .get()
        .await
        .context("failed to get DB connection for migrations")?;

    // Acquire advisory lock to serialize migrations across engine replicas.
    client
        .execute("SELECT pg_advisory_lock($1)", &[&lock_key])
        .await
        .context("failed to acquire advisory lock")?;

    // Run migrations inside the module's schema, releasing the lock on all exit paths.
    let result = async {
        // Restrict search_path so migrations cannot touch other schemas.
        client
            .execute(&format!("SET search_path = \"{schema}\""), &[])
            .await
            .context("failed to set search_path")?;

        let runner = refinery::Runner::new(&migrations);
        runner
            .run_async(&mut **client)
            .await
            .context("migration execution failed")?;

        Ok::<(), anyhow::Error>(())
    }
    .await;

    // Always release the advisory lock.
    if let Err(e) = client
        .execute("SELECT pg_advisory_unlock($1)", &[&lock_key])
        .await
    {
        tracing::warn!(module = module_name, error = %e, "failed to release advisory lock");
    }

    result?;

    info!(
        module = module_name,
        schema,
        count = migrations.len(),
        "migrations complete",
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::advisory_lock_key;

    #[test]
    fn advisory_lock_key_is_deterministic() {
        let a = advisory_lock_key("wr__stockmarket__exchange");
        let b = advisory_lock_key("wr__stockmarket__exchange");
        assert_eq!(a, b);
    }

    #[test]
    fn advisory_lock_key_differs_by_schema() {
        let a = advisory_lock_key("wr__stockmarket__exchange");
        let b = advisory_lock_key("wr__stockmarket__ledger");
        assert_ne!(a, b);
    }
}
