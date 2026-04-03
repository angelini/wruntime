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

/// Acquire the global routing-table lock, waiting if another transaction holds it.
/// Used by the background monitor which can afford to block briefly.
async fn acquire_global_lock_wait(txn: &deadpool_postgres::Transaction<'_>) -> Result<i64, Status> {
    let row = txn
        .query_one(
            "SELECT version FROM wr_manager_lock WHERE id = 1 FOR UPDATE",
            &[],
        )
        .await
        .map_err(|e| Status::internal(format!("lock query failed: {e}")))?;
    Ok(row.get(0))
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
               updated_at = NOW(),
               last_heartbeat = NOW()",
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

/// Update an engine's heartbeat timestamp.
pub async fn heartbeat_engine(pool: &Pool, engine_id: &str) -> Result<(), Status> {
    let client = pool.get().await.map_err(internal)?;
    let updated = client
        .execute(
            "UPDATE wr_engines SET last_heartbeat = NOW() WHERE engine_id = $1",
            &[&engine_id],
        )
        .await
        .map_err(internal)?;
    if updated == 0 {
        return Err(Status::not_found(format!(
            "engine {engine_id} not registered"
        )));
    }
    Ok(())
}

/// Batch-update rule health based on engine heartbeat timestamps.
/// Finds stale engines (heartbeat older than `timeout_secs`) and marks their
/// healthy rules unhealthy, and finds recovered engines and marks their
/// unhealthy rules healthy.
///
/// The health UPDATEs run without the global lock (they are idempotent).
/// The global lock is only acquired briefly to bump the routing table version
/// when changes occurred.
///
/// Returns `(stale_rule_ids, recovered_rule_ids)`.
pub async fn update_rule_health_from_heartbeats(
    pool: &Pool,
    timeout_secs: f64,
) -> Result<(Vec<String>, Vec<String>), Status> {
    let mut client = pool.get().await.map_err(internal)?;

    // Mark stale engines' rules unhealthy (no lock needed — idempotent)
    let stale_rows = client
        .query(
            "UPDATE wr_routing_rules SET healthy = FALSE, updated_at = NOW()
             WHERE rule_id IN (
                 SELECT r.rule_id FROM wr_routing_rules r
                 JOIN wr_engines e ON r.engine_id = e.engine_id
                 WHERE e.last_heartbeat < NOW() - make_interval(secs => $1::double precision)
                   AND r.healthy = TRUE
             )
             RETURNING rule_id",
            &[&timeout_secs],
        )
        .await
        .map_err(internal)?;

    // Mark recovered engines' rules healthy (no lock needed — idempotent)
    let recovered_rows = client
        .query(
            "UPDATE wr_routing_rules SET healthy = TRUE, updated_at = NOW()
             WHERE rule_id IN (
                 SELECT r.rule_id FROM wr_routing_rules r
                 JOIN wr_engines e ON r.engine_id = e.engine_id
                 WHERE e.last_heartbeat >= NOW() - make_interval(secs => $1::double precision)
                   AND r.healthy = FALSE
             )
             RETURNING rule_id",
            &[&timeout_secs],
        )
        .await
        .map_err(internal)?;

    let stale: Vec<String> = stale_rows.iter().map(|r| r.get(0)).collect();
    let recovered: Vec<String> = recovered_rows.iter().map(|r| r.get(0)).collect();

    // Only acquire the lock briefly to bump the version
    if !stale.is_empty() || !recovered.is_empty() {
        let txn = client.transaction().await.map_err(internal)?;
        acquire_global_lock_wait(&txn).await?;
        increment_version(&txn).await?;
        txn.commit().await.map_err(internal)?;
    }

    Ok((stale, recovered))
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

// ── Schema operations ────────────────────────────────────────────────────────

// ── Secret operations ────────────────────────────────────────────────────────

/// Insert or update an encrypted secret.
pub async fn upsert_secret(
    pool: &Pool,
    namespace: &str,
    key: &str,
    ciphertext: &[u8],
    nonce: &[u8],
) -> Result<(), Status> {
    let client = pool.get().await.map_err(internal)?;
    client
        .execute(
            "INSERT INTO wr_secrets (namespace, key, ciphertext, nonce)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (namespace, key) DO UPDATE
               SET ciphertext = EXCLUDED.ciphertext,
                   nonce = EXCLUDED.nonce,
                   updated_at = NOW()",
            &[&namespace, &key, &ciphertext, &nonce],
        )
        .await
        .map_err(internal)?;
    Ok(())
}

/// Delete a secret by (namespace, key). Returns true if a row was deleted.
pub async fn delete_secret(pool: &Pool, namespace: &str, key: &str) -> Result<bool, Status> {
    let client = pool.get().await.map_err(internal)?;
    let deleted = client
        .execute(
            "DELETE FROM wr_secrets WHERE namespace = $1 AND key = $2",
            &[&namespace, &key],
        )
        .await
        .map_err(internal)?;
    Ok(deleted > 0)
}

/// List secret metadata (namespace + key only, no values).
/// If namespace is empty, return all secrets across all namespaces.
pub async fn list_secrets(pool: &Pool, namespace: &str) -> Result<Vec<(String, String)>, Status> {
    let client = pool.get().await.map_err(internal)?;
    let rows = if namespace.is_empty() {
        client
            .query(
                "SELECT namespace, key FROM wr_secrets ORDER BY namespace, key",
                &[],
            )
            .await
            .map_err(internal)?
    } else {
        client
            .query(
                "SELECT namespace, key FROM wr_secrets WHERE namespace = $1 ORDER BY key",
                &[&namespace],
            )
            .await
            .map_err(internal)?
    };
    Ok(rows.iter().map(|r| (r.get(0), r.get(1))).collect())
}

/// Fetch encrypted secrets for specific (namespace, key) pairs.
pub async fn get_secrets(
    pool: &Pool,
    requests: &[(String, String)],
) -> Result<Vec<(String, String, Vec<u8>, Vec<u8>)>, Status> {
    if requests.is_empty() {
        return Ok(vec![]);
    }
    let client = pool.get().await.map_err(internal)?;
    let mut results = Vec::with_capacity(requests.len());
    for (namespace, key) in requests {
        let row = client
            .query_opt(
                "SELECT namespace, key, ciphertext, nonce FROM wr_secrets
                 WHERE namespace = $1 AND key = $2",
                &[namespace, key],
            )
            .await
            .map_err(internal)?;
        if let Some(row) = row {
            results.push((row.get(0), row.get(1), row.get(2), row.get(3)));
        }
    }
    Ok(results)
}

// ── Manager registration ────────────────────────────────────────────────────

/// A registered manager in the cluster.
pub struct ManagerRecord {
    pub manager_id: String,
    pub grpc_address: String,
    pub gossip_address: String,
}

/// Register (or re-register) this manager in the cluster.
pub async fn register_manager(
    pool: &Pool,
    manager_id: &str,
    grpc_address: &str,
    gossip_address: &str,
) -> Result<(), Status> {
    let client = pool.get().await.map_err(internal)?;
    client
        .execute(
            "INSERT INTO wr_managers (manager_id, grpc_address, gossip_address)
             VALUES ($1, $2, $3)
             ON CONFLICT (manager_id) DO UPDATE
               SET grpc_address = EXCLUDED.grpc_address,
                   gossip_address = EXCLUDED.gossip_address,
                   last_heartbeat = NOW()",
            &[&manager_id, &grpc_address, &gossip_address],
        )
        .await
        .map_err(internal)?;
    Ok(())
}

/// Remove this manager from the cluster.
pub async fn deregister_manager(pool: &Pool, manager_id: &str) -> Result<(), Status> {
    let client = pool.get().await.map_err(internal)?;
    client
        .execute(
            "DELETE FROM wr_managers WHERE manager_id = $1",
            &[&manager_id],
        )
        .await
        .map_err(internal)?;
    Ok(())
}

/// List all managers that have heartbeated within the given threshold.
pub async fn list_managers(pool: &Pool) -> Result<Vec<ManagerRecord>, Status> {
    let client = pool.get().await.map_err(internal)?;
    let rows = client
        .query(
            "SELECT manager_id, grpc_address, gossip_address FROM wr_managers
             WHERE last_heartbeat > NOW() - INTERVAL '60 seconds'",
            &[],
        )
        .await
        .map_err(internal)?;
    Ok(rows
        .iter()
        .map(|r| ManagerRecord {
            manager_id: r.get(0),
            grpc_address: r.get(1),
            gossip_address: r.get(2),
        })
        .collect())
}

/// Update this manager's heartbeat timestamp.
pub async fn heartbeat_manager(pool: &Pool, manager_id: &str) -> Result<(), Status> {
    let client = pool.get().await.map_err(internal)?;
    client
        .execute(
            "UPDATE wr_managers SET last_heartbeat = NOW() WHERE manager_id = $1",
            &[&manager_id],
        )
        .await
        .map_err(internal)?;
    Ok(())
}

/// Remove managers that haven't heartbeated within the threshold. Returns count deleted.
pub async fn cleanup_stale_managers(pool: &Pool, stale_threshold_secs: i64) -> Result<u64, Status> {
    let client = pool.get().await.map_err(internal)?;
    let deleted = client
        .execute(
            "DELETE FROM wr_managers WHERE last_heartbeat < NOW() - make_interval(secs => $1::double precision)",
            &[&(stale_threshold_secs as f64)],
        )
        .await
        .map_err(internal)?;
    Ok(deleted)
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
