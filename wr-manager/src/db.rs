use deadpool_postgres::Pool;
use prost::Message;
use tonic::Status;

use wr_common::wruntime::{EngineRegistration, RoutingRule, RoutingTable};

// ── Private helpers ──────────────────────────────────────────────────────────

/// Acquire the global routing-table lock within an existing transaction.
/// Returns the current version. Uses NOWAIT so concurrent writers get an
/// immediate `Status::aborted` instead of blocking.
async fn acquire_global_lock(txn: &deadpool_postgres::Transaction<'_>) -> Result<i64, Status> {
    let row = txn
        .query_one(
            "SELECT version FROM wr_manager_lock WHERE id = 1 FOR UPDATE NOWAIT",
            &[],
        )
        .await
        .map_err(map_lock_err)?;
    Ok(row.get(0))
}

/// Increment the routing table version and return the new value.
async fn increment_version(txn: &deadpool_postgres::Transaction<'_>) -> Result<i64, Status> {
    let row = txn
        .query_one(
            "UPDATE wr_manager_lock SET version = version + 1 WHERE id = 1 RETURNING version",
            &[],
        )
        .await
        .map_err(|e| Status::internal(format!("failed to increment version: {e}")))?;
    Ok(row.get(0))
}

/// Map a Postgres error to a tonic Status.
/// `LOCK_NOT_AVAILABLE` (55P03) becomes `Status::aborted` so callers can retry.
fn map_lock_err(e: tokio_postgres::Error) -> Status {
    let code = e.code().map(|c| c.code()).unwrap_or_default();
    if code == "55P03" {
        Status::aborted("concurrent write conflict")
    } else {
        Status::internal(format!("lock query failed: {e}"))
    }
}

fn internal(msg: impl std::fmt::Display) -> Status {
    Status::internal(msg.to_string())
}

// ── Engine operations ────────────────────────────────────────────────────────

/// Upsert an engine registration and its module schemas.
/// Does NOT acquire the global lock (engine registration doesn't affect routing).
pub async fn upsert_engine_and_schemas(
    pool: &Pool,
    reg: &EngineRegistration,
) -> Result<(), Status> {
    let mut client = pool.get().await.map_err(internal)?;
    let txn = client.transaction().await.map_err(internal)?;

    let registration_bytes = reg.encode_to_vec();

    txn.execute(
        "INSERT INTO wr_engines (engine_id, address, proxy_address, registration)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (engine_id) DO UPDATE
           SET address = EXCLUDED.address,
               proxy_address = EXCLUDED.proxy_address,
               registration = EXCLUDED.registration,
               updated_at = NOW()",
        &[
            &reg.engine_id,
            &reg.address,
            &reg.proxy_address,
            &registration_bytes,
        ],
    )
    .await
    .map_err(internal)?;

    for module in &reg.modules {
        txn.execute(
            "INSERT INTO wr_schemas (namespace, module_name, version, proto_schema)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (namespace, module_name, version) DO UPDATE
               SET proto_schema = EXCLUDED.proto_schema,
                   updated_at = NOW()",
            &[
                &module.namespace,
                &module.name,
                &module.version,
                &module.proto_schema,
            ],
        )
        .await
        .map_err(internal)?;
    }

    txn.commit().await.map_err(internal)?;
    Ok(())
}

/// Deregister an engine: mark its routing rules unhealthy, delete the engine
/// row, and bump the routing table version.
pub async fn deregister_engine(pool: &Pool, engine_id: &str) -> Result<(), Status> {
    let mut client = pool.get().await.map_err(internal)?;
    let txn = client.transaction().await.map_err(internal)?;

    acquire_global_lock(&txn).await?;

    let changed = txn
        .execute(
            "UPDATE wr_routing_rules SET healthy = FALSE, updated_at = NOW()
             WHERE engine_id = $1 AND healthy = TRUE",
            &[&engine_id],
        )
        .await
        .map_err(internal)?;

    txn.execute("DELETE FROM wr_engines WHERE engine_id = $1", &[&engine_id])
        .await
        .map_err(internal)?;

    if changed > 0 {
        increment_version(&txn).await?;
    }

    txn.commit().await.map_err(internal)?;
    Ok(())
}

/// List all registered engines (decoded from protobuf BYTEA).
pub async fn list_engines(pool: &Pool) -> Result<Vec<EngineRegistration>, Status> {
    let client = pool.get().await.map_err(internal)?;
    let rows = client
        .query("SELECT registration FROM wr_engines", &[])
        .await
        .map_err(internal)?;

    rows.iter()
        .map(|row| {
            let bytes: Vec<u8> = row.get(0);
            EngineRegistration::decode(bytes.as_slice())
                .map_err(|e| Status::internal(format!("failed to decode registration: {e}")))
        })
        .collect()
}

// ── Routing operations ───────────────────────────────────────────────────────

/// Upsert a routing rule. Acquires the global lock and bumps the version.
pub async fn upsert_routing_rule(pool: &Pool, rule: &RoutingRule) -> Result<(), Status> {
    let mut client = pool.get().await.map_err(internal)?;
    let txn = client.transaction().await.map_err(internal)?;

    acquire_global_lock(&txn).await?;

    txn.execute(
        "INSERT INTO wr_routing_rules (
            rule_id, source_namespace, source_module,
            destination_namespace, destination_module, destination_version,
            engine_id, engine_address, proxy_address, healthy
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
         ON CONFLICT (rule_id) DO UPDATE SET
            source_namespace = EXCLUDED.source_namespace,
            source_module = EXCLUDED.source_module,
            destination_namespace = EXCLUDED.destination_namespace,
            destination_module = EXCLUDED.destination_module,
            destination_version = EXCLUDED.destination_version,
            engine_id = EXCLUDED.engine_id,
            engine_address = EXCLUDED.engine_address,
            proxy_address = EXCLUDED.proxy_address,
            healthy = EXCLUDED.healthy,
            updated_at = NOW()",
        &[
            &rule.rule_id,
            &rule.source_namespace,
            &rule.source_module,
            &rule.destination_namespace,
            &rule.destination_module,
            &rule.destination_version,
            &rule.engine_id,
            &rule.engine_address,
            &rule.proxy_address,
            &rule.healthy,
        ],
    )
    .await
    .map_err(internal)?;

    increment_version(&txn).await?;
    txn.commit().await.map_err(internal)?;
    Ok(())
}

/// Delete a routing rule by ID. Returns true if a rule was actually deleted.
pub async fn delete_routing_rule(pool: &Pool, rule_id: &str) -> Result<bool, Status> {
    let mut client = pool.get().await.map_err(internal)?;
    let txn = client.transaction().await.map_err(internal)?;

    acquire_global_lock(&txn).await?;

    let deleted = txn
        .execute(
            "DELETE FROM wr_routing_rules WHERE rule_id = $1",
            &[&rule_id],
        )
        .await
        .map_err(internal)?;

    if deleted > 0 {
        increment_version(&txn).await?;
    }

    txn.commit().await.map_err(internal)?;
    Ok(deleted > 0)
}

/// Read the full routing table from the database.
pub async fn get_routing_table(pool: &Pool) -> Result<RoutingTable, Status> {
    let client = pool.get().await.map_err(internal)?;

    let version: i64 = client
        .query_one("SELECT version FROM wr_manager_lock WHERE id = 1", &[])
        .await
        .map_err(internal)?
        .get(0);

    let rows = client
        .query(
            "SELECT rule_id, source_namespace, source_module,
                    destination_namespace, destination_module, destination_version,
                    engine_id, engine_address, proxy_address, healthy
             FROM wr_routing_rules",
            &[],
        )
        .await
        .map_err(internal)?;

    let rules = rows
        .iter()
        .map(|row| RoutingRule {
            rule_id: row.get(0),
            source_namespace: row.get(1),
            source_module: row.get(2),
            destination_namespace: row.get(3),
            destination_module: row.get(4),
            destination_version: row.get(5),
            engine_id: row.get(6),
            engine_address: row.get(7),
            proxy_address: row.get(8),
            healthy: row.get(9),
        })
        .collect();

    Ok(RoutingTable {
        rules,
        version: version as u64,
    })
}

/// Mark all routing rules for an engine as unhealthy. Uses NOWAIT — on
/// contention the caller should log a warning and retry on the next tick.
pub async fn mark_rules_unhealthy_for_engine(pool: &Pool, engine_id: &str) -> Result<bool, Status> {
    let mut client = pool.get().await.map_err(internal)?;
    let txn = client.transaction().await.map_err(internal)?;

    acquire_global_lock(&txn).await?;

    let changed = txn
        .execute(
            "UPDATE wr_routing_rules SET healthy = FALSE, updated_at = NOW()
             WHERE engine_id = $1 AND healthy = TRUE",
            &[&engine_id],
        )
        .await
        .map_err(internal)?;

    if changed > 0 {
        increment_version(&txn).await?;
    }

    txn.commit().await.map_err(internal)?;
    Ok(changed > 0)
}

/// Update a single routing rule's health status. Uses NOWAIT.
pub async fn set_rule_health(pool: &Pool, rule_id: &str, healthy: bool) -> Result<(), Status> {
    let mut client = pool.get().await.map_err(internal)?;
    let txn = client.transaction().await.map_err(internal)?;

    acquire_global_lock(&txn).await?;

    txn.execute(
        "UPDATE wr_routing_rules SET healthy = $1, updated_at = NOW() WHERE rule_id = $2",
        &[&healthy, &rule_id],
    )
    .await
    .map_err(internal)?;

    increment_version(&txn).await?;
    txn.commit().await.map_err(internal)?;
    Ok(())
}

// ── Schema operations ────────────────────────────────────────────────────────

/// Upsert a schema (FileDescriptorSet bytes).
pub async fn upsert_schema(
    pool: &Pool,
    namespace: &str,
    module: &str,
    version: &str,
    data: &[u8],
) -> Result<(), Status> {
    let client = pool.get().await.map_err(internal)?;
    client
        .execute(
            "INSERT INTO wr_schemas (namespace, module_name, version, proto_schema)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (namespace, module_name, version) DO UPDATE
               SET proto_schema = EXCLUDED.proto_schema,
                   updated_at = NOW()",
            &[&namespace, &module, &version, &data],
        )
        .await
        .map_err(internal)?;
    Ok(())
}

/// Get a schema by (namespace, module, version).
pub async fn get_schema(
    pool: &Pool,
    namespace: &str,
    module: &str,
    version: &str,
) -> Result<Vec<u8>, Status> {
    let client = pool.get().await.map_err(internal)?;
    let row = client
        .query_opt(
            "SELECT proto_schema FROM wr_schemas
             WHERE namespace = $1 AND module_name = $2 AND version = $3",
            &[&namespace, &module, &version],
        )
        .await
        .map_err(internal)?
        .ok_or_else(|| {
            Status::not_found(format!("no schema for {namespace}/{module}/{version}"))
        })?;
    Ok(row.get(0))
}
