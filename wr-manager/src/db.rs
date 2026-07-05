use deadpool_postgres::Pool;
use prost::Message;
use tokio_retry::strategy::ExponentialBackoff;
use tokio_retry::RetryIf;
use tonic::Status;

use wr_common::wruntime::{EngineRegistration, ModuleDescriptor, RoutingRule, RoutingTable};

/// Exponential backoff strategy for NOWAIT lock retries: 10ms, 20ms, 40ms, 80ms.
fn lock_retry_strategy() -> impl Iterator<Item = std::time::Duration> {
    ExponentialBackoff::from_millis(10).take(4)
}

/// Returns true if the error is a NOWAIT lock contention (retryable).
fn is_lock_contention(e: &Status) -> bool {
    e.code() == tonic::Code::Aborted
}

// ── Private helpers ──────────────────────────────────────────────────────────

/// Acquire the global routing-table lock within an existing transaction.
/// Returns the current version. Uses NOWAIT so concurrent writers get an
/// immediate `Status::aborted` instead of blocking.
async fn acquire_global_lock(
    txn: &deadpool_postgres::Transaction<'_>,
    operation: &str,
) -> Result<i64, Status> {
    let row = txn
        .query_one(
            "SELECT version FROM wr_manager_lock WHERE id = 1 FOR UPDATE NOWAIT",
            &[],
        )
        .await
        .map_err(|e| map_lock_err(e, operation))?;
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
fn map_lock_err(e: tokio_postgres::Error, operation: &str) -> Status {
    let code = e.code().map(|c| c.code()).unwrap_or_default();
    if code == "55P03" {
        Status::aborted(format!(
            "concurrent write conflict during {operation} — another routing table update is in progress, retry"
        ))
    } else {
        Status::internal(format!(
            "lock query failed during {operation}: {}",
            wr_common::pool::pg_error_string(&e)
        ))
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

/// Extension trait to replace `.map_err(|e| Status::internal(e.to_string()))?`
/// with `.internal()?`.
trait IntoInternalStatus<T> {
    fn internal(self) -> Result<T, Status>;
}

impl<T, E: std::fmt::Display> IntoInternalStatus<T> for Result<T, E> {
    fn internal(self) -> Result<T, Status> {
        self.map_err(|e| Status::internal(e.to_string()))
    }
}

// ── Engine operations ────────────────────────────────────────────────────────

/// Register an engine, its module schemas, and one default routing rule per
/// unique schema-bearing module tuple — all in a single transaction under the
/// global routing lock. Routes are the last statements before commit, so any
/// earlier failure rolls back the whole registration (no partial routes).
pub async fn register_engine_and_routes(
    pool: &Pool,
    reg: &EngineRegistration,
) -> Result<(), Status> {
    let mut client = pool.get().await.internal()?;
    let txn = client.transaction().await.internal()?;

    acquire_global_lock_wait(&txn).await?;

    let registration_bytes = reg.encode_to_vec();

    txn.execute(
        "INSERT INTO wr_engines (engine_id, address, proxy_address, peer_address, registration)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (engine_id) DO UPDATE
           SET address = EXCLUDED.address,
               proxy_address = EXCLUDED.proxy_address,
               peer_address = EXCLUDED.peer_address,
               registration = EXCLUDED.registration,
               updated_at = NOW(),
               last_heartbeat = NOW()",
        &[
            &reg.engine_id,
            &reg.address,
            &reg.proxy_address,
            &reg.peer_address,
            &registration_bytes,
        ],
    )
    .await
    .internal()?;

    for module in &reg.modules {
        if module.proto_schema.is_empty() {
            continue;
        }
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
        .internal()?;
    }

    // Publish one default routing rule per unique schema-bearing module tuple.
    // source_* = "", destination_* = module tuple, engine_address = reg.address,
    // peer_address = reg.peer_address, healthy = true.
    let empty = String::new();
    let healthy = true;
    let mut seen = std::collections::HashSet::new();
    let mut seen_advertised = std::collections::HashSet::new();
    let mut desired_rule_ids: Vec<String> = Vec::new();
    let mut advertised_ns: Vec<String> = Vec::new();
    let mut advertised_name: Vec<String> = Vec::new();
    let mut advertised_ver: Vec<String> = Vec::new();
    for module in &reg.modules {
        // Track every advertised tuple (schema-bearing or not) so heartbeat
        // reconciliation only removes modules this engine no longer advertises.
        if seen_advertised.insert((&module.namespace, &module.name, &module.version)) {
            advertised_ns.push(module.namespace.clone());
            advertised_name.push(module.name.clone());
            advertised_ver.push(module.version.clone());
        }
        if module.proto_schema.is_empty() {
            continue;
        }
        if !seen.insert((&module.namespace, &module.name, &module.version)) {
            continue;
        }
        let rule_id = format!(
            "{}/{}/{}/{}",
            reg.engine_id, module.namespace, module.name, module.version
        );
        txn.execute(
            "INSERT INTO wr_routing_rules (
                rule_id, source_namespace, source_module,
                destination_namespace, destination_module, destination_version,
                engine_id, engine_address, peer_address, healthy
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
             ON CONFLICT (rule_id) DO UPDATE SET
                source_namespace = EXCLUDED.source_namespace,
                source_module = EXCLUDED.source_module,
                destination_namespace = EXCLUDED.destination_namespace,
                destination_module = EXCLUDED.destination_module,
                destination_version = EXCLUDED.destination_version,
                engine_id = EXCLUDED.engine_id,
                engine_address = EXCLUDED.engine_address,
                peer_address = EXCLUDED.peer_address,
                healthy = EXCLUDED.healthy,
                updated_at = NOW()",
            &[
                &rule_id,
                &empty,
                &empty,
                &module.namespace,
                &module.name,
                &module.version,
                &reg.engine_id,
                &reg.address,
                &reg.peer_address,
                &healthy,
            ],
        )
        .await
        .internal()?;
        txn.execute(
            "INSERT INTO wr_module_heartbeats (engine_id, namespace, module_name, version)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (engine_id, namespace, module_name, version)
             DO UPDATE SET last_healthy = NOW()",
            &[
                &reg.engine_id,
                &module.namespace,
                &module.name,
                &module.version,
            ],
        )
        .await
        .internal()?;
        desired_rule_ids.push(rule_id);
    }
    let rules_written = desired_rule_ids.len() as u64;

    // Reconcile: remove engine-owned DEFAULT rules this registration no longer
    // advertises. Only rows with this engine's canonical default prefix
    // "{engine_id}/" are touched; admin rules from UpsertRoutingRule use a
    // different rule_id shape and are left intact. When desired_rule_ids is empty
    // (no schema-bearing modules), `<> ALL('{}')` is true for every prefixed row,
    // so all of this engine's default rules are removed — the authoritative result.
    let engine_prefix = format!("{}/", reg.engine_id);
    let deleted_rules = txn
        .execute(
            "DELETE FROM wr_routing_rules
             WHERE engine_id = $1
               AND starts_with(rule_id, $2)
               AND rule_id <> ALL($3::text[])",
            &[&reg.engine_id, &engine_prefix, &desired_rule_ids],
        )
        .await
        .internal()?;

    // Drop per-module heartbeats for modules this engine no longer advertises,
    // so a removed module cannot be resurrected as healthy by the health sweep.
    // Keep-set is every advertised tuple; NOT IN over an empty unnest deletes all
    // of this engine's heartbeat rows (zero-module re-registration).
    txn.execute(
        "DELETE FROM wr_module_heartbeats
         WHERE engine_id = $1
           AND (namespace, module_name, version) NOT IN (
             SELECT n, m, v FROM unnest($2::text[], $3::text[], $4::text[]) AS t(n, m, v)
           )",
        &[
            &reg.engine_id,
            &advertised_ns,
            &advertised_name,
            &advertised_ver,
        ],
    )
    .await
    .internal()?;

    if rules_written > 0 || deleted_rules > 0 {
        increment_version(&txn).await?;
    }

    txn.commit().await.internal()?;
    Ok(())
}

/// Deregister an engine: mark its routing rules unhealthy, delete the engine
/// row, and bump the routing table version.
pub async fn deregister_engine(pool: &Pool, engine_id: &str) -> Result<(), Status> {
    let mut client = pool.get().await.internal()?;
    let txn = client.transaction().await.internal()?;

    acquire_global_lock_wait(&txn).await?;

    let changed = txn
        .execute(
            "UPDATE wr_routing_rules SET healthy = FALSE, updated_at = NOW()
             WHERE engine_id = $1 AND healthy = TRUE",
            &[&engine_id],
        )
        .await
        .internal()?;

    txn.execute("DELETE FROM wr_engines WHERE engine_id = $1", &[&engine_id])
        .await
        .internal()?;

    txn.execute(
        "DELETE FROM wr_module_heartbeats WHERE engine_id = $1",
        &[&engine_id],
    )
    .await
    .internal()?;

    if changed > 0 {
        increment_version(&txn).await?;
    }

    txn.commit().await.internal()?;
    Ok(())
}

/// Update an engine's heartbeat timestamp.
pub async fn heartbeat_engine(pool: &Pool, engine_id: &str) -> Result<(), Status> {
    let client = pool.get().await.internal()?;
    let updated = client
        .execute(
            "UPDATE wr_engines SET last_heartbeat = NOW() WHERE engine_id = $1",
            &[&engine_id],
        )
        .await
        .internal()?;
    if updated == 0 {
        return Err(Status::not_found(format!(
            "engine {engine_id} not registered"
        )));
    }
    Ok(())
}

/// Upsert a per-module heartbeat (`last_healthy = NOW()`) for each module an
/// engine reports healthy. Idempotent per
/// (engine_id, namespace, module_name, version). Does not bump the routing
/// version — route health is recomputed by the background monitor.
pub async fn upsert_module_heartbeats(
    pool: &Pool,
    engine_id: &str,
    modules: &[ModuleDescriptor],
) -> Result<(), Status> {
    if modules.is_empty() {
        return Ok(());
    }
    let client = pool.get().await.internal()?;
    for module in modules {
        client
            .execute(
                "INSERT INTO wr_module_heartbeats (engine_id, namespace, module_name, version)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT (engine_id, namespace, module_name, version)
                 DO UPDATE SET last_healthy = NOW()",
                &[&engine_id, &module.namespace, &module.name, &module.version],
            )
            .await
            .internal()?;
    }
    Ok(())
}

/// Recompute routing-rule health from BOTH engine and per-module heartbeats.
///
/// A rule is healthy iff its engine's heartbeat is fresh (within
/// `engine_timeout_secs`) AND a matching `wr_module_heartbeats` row for the
/// rule's destination `(namespace, module, version)` on that engine is fresh
/// (within `module_timeout_secs`). This subsumes the old engine-only logic: a
/// stale engine fails the join for all its rules, and additionally a single
/// stale or missing module takes only its own routes out of rotation.
///
/// The health UPDATEs run without the global lock (they are idempotent). The
/// global lock is acquired only briefly to bump the routing table version when
/// changes occurred.
///
/// Returns `(stale_rule_ids, recovered_rule_ids)`.
pub async fn update_route_health(
    pool: &Pool,
    engine_timeout_secs: f64,
    module_timeout_secs: f64,
) -> Result<(Vec<String>, Vec<String>), Status> {
    let mut client = pool.get().await.internal()?;

    // Mark unhealthy: currently healthy but no longer backed by BOTH a fresh
    // engine heartbeat and a fresh matching module heartbeat.
    let stale_rows = client
        .query(
            "UPDATE wr_routing_rules r SET healthy = FALSE, updated_at = NOW()
             WHERE r.healthy = TRUE
               AND NOT EXISTS (
                 SELECT 1 FROM wr_engines e
                 JOIN wr_module_heartbeats m
                   ON m.engine_id   = e.engine_id
                  AND m.namespace   = r.destination_namespace
                  AND m.module_name = r.destination_module
                  AND m.version     = r.destination_version
                 WHERE e.engine_id = r.engine_id
                   AND e.last_heartbeat >= NOW() - make_interval(secs => $1::double precision)
                   AND m.last_healthy   >= NOW() - make_interval(secs => $2::double precision)
               )
             RETURNING rule_id",
            &[&engine_timeout_secs, &module_timeout_secs],
        )
        .await
        .internal()?;

    // Mark healthy: currently unhealthy but now backed by BOTH fresh signals.
    let recovered_rows = client
        .query(
            "UPDATE wr_routing_rules r SET healthy = TRUE, updated_at = NOW()
             WHERE r.healthy = FALSE
               AND EXISTS (
                 SELECT 1 FROM wr_engines e
                 JOIN wr_module_heartbeats m
                   ON m.engine_id   = e.engine_id
                  AND m.namespace   = r.destination_namespace
                  AND m.module_name = r.destination_module
                  AND m.version     = r.destination_version
                 WHERE e.engine_id = r.engine_id
                   AND e.last_heartbeat >= NOW() - make_interval(secs => $1::double precision)
                   AND m.last_healthy   >= NOW() - make_interval(secs => $2::double precision)
               )
             RETURNING rule_id",
            &[&engine_timeout_secs, &module_timeout_secs],
        )
        .await
        .internal()?;

    let stale: Vec<String> = stale_rows.iter().map(|r| r.get(0)).collect();
    let recovered: Vec<String> = recovered_rows.iter().map(|r| r.get(0)).collect();

    // Only acquire the lock briefly to bump the version
    if !stale.is_empty() || !recovered.is_empty() {
        let txn = client.transaction().await.internal()?;
        acquire_global_lock_wait(&txn).await?;
        increment_version(&txn).await?;
        txn.commit().await.internal()?;
    }

    Ok((stale, recovered))
}

/// List all registered engines (decoded from protobuf BYTEA).
pub async fn list_engines(pool: &Pool) -> Result<Vec<EngineRegistration>, Status> {
    let client = pool.get().await.internal()?;
    let rows = client
        .query("SELECT registration FROM wr_engines", &[])
        .await
        .internal()?;

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
/// Retries automatically on NOWAIT lock contention with exponential backoff.
pub async fn upsert_routing_rule(pool: &Pool, rule: &RoutingRule) -> Result<(), Status> {
    if rule.peer_address.is_empty() {
        return Err(Status::invalid_argument(
            "routing rule peer_address must be non-empty",
        ));
    }
    RetryIf::start(
        lock_retry_strategy(),
        || upsert_routing_rule_once(pool, rule),
        is_lock_contention,
    )
    .await
}

async fn upsert_routing_rule_once(pool: &Pool, rule: &RoutingRule) -> Result<(), Status> {
    let mut client = pool.get().await.internal()?;
    let txn = client.transaction().await.internal()?;

    acquire_global_lock(&txn, "upsert_routing_rule").await?;

    txn.execute(
        "INSERT INTO wr_routing_rules (
            rule_id, source_namespace, source_module,
            destination_namespace, destination_module, destination_version,
            engine_id, engine_address, peer_address, healthy
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
         ON CONFLICT (rule_id) DO UPDATE SET
            source_namespace = EXCLUDED.source_namespace,
            source_module = EXCLUDED.source_module,
            destination_namespace = EXCLUDED.destination_namespace,
            destination_module = EXCLUDED.destination_module,
            destination_version = EXCLUDED.destination_version,
            engine_id = EXCLUDED.engine_id,
            engine_address = EXCLUDED.engine_address,
            peer_address = EXCLUDED.peer_address,
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
            &rule.peer_address,
            &rule.healthy,
        ],
    )
    .await
    .internal()?;

    increment_version(&txn).await?;
    txn.commit().await.internal()?;
    Ok(())
}

/// Delete a routing rule by ID. Returns true if a rule was actually deleted.
/// Retries automatically on NOWAIT lock contention with exponential backoff.
pub async fn delete_routing_rule(pool: &Pool, rule_id: &str) -> Result<bool, Status> {
    RetryIf::start(
        lock_retry_strategy(),
        || delete_routing_rule_once(pool, rule_id),
        is_lock_contention,
    )
    .await
}

async fn delete_routing_rule_once(pool: &Pool, rule_id: &str) -> Result<bool, Status> {
    let mut client = pool.get().await.internal()?;
    let txn = client.transaction().await.internal()?;

    acquire_global_lock(&txn, "delete_routing_rule").await?;

    let deleted = txn
        .execute(
            "DELETE FROM wr_routing_rules WHERE rule_id = $1",
            &[&rule_id],
        )
        .await
        .internal()?;

    if deleted > 0 {
        increment_version(&txn).await?;
    }

    txn.commit().await.internal()?;
    Ok(deleted > 0)
}

/// Read the routing table from the database.
/// If `known_version` is non-zero and matches the current version, returns `None`
/// (the caller's copy is up to date). Otherwise returns the full table.
pub async fn get_routing_table(
    pool: &Pool,
    known_version: u64,
) -> Result<Option<RoutingTable>, Status> {
    let client = pool.get().await.internal()?;

    let version: i64 = client
        .query_one("SELECT version FROM wr_manager_lock WHERE id = 1", &[])
        .await
        .internal()?
        .get(0);

    if known_version != 0 && known_version == version as u64 {
        return Ok(None);
    }

    let rows = client
        .query(
            "SELECT rule_id, source_namespace, source_module,
                    destination_namespace, destination_module, destination_version,
                    engine_id, engine_address, healthy, peer_address
             FROM wr_routing_rules",
            &[],
        )
        .await
        .internal()?;

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
            healthy: row.get(8),
            peer_address: row.get(9),
        })
        .collect();

    Ok(Some(RoutingTable {
        rules,
        version: version as u64,
    }))
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
    let client = pool.get().await.internal()?;
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
        .internal()?;
    Ok(())
}

/// Insert an encrypted secret only if no row exists for (namespace, key).
/// Uses `ON CONFLICT DO NOTHING` so racing callers converge on a single stored
/// row; the caller must re-read (via `get_secrets`) to obtain the winning value.
pub async fn insert_secret_if_absent(
    pool: &Pool,
    namespace: &str,
    key: &str,
    ciphertext: &[u8],
    nonce: &[u8],
) -> Result<(), Status> {
    let client = pool.get().await.internal()?;
    client
        .execute(
            "INSERT INTO wr_secrets (namespace, key, ciphertext, nonce)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (namespace, key) DO NOTHING",
            &[&namespace, &key, &ciphertext, &nonce],
        )
        .await
        .internal()?;
    Ok(())
}

/// Delete a secret by (namespace, key). Returns true if a row was deleted.
pub async fn delete_secret(pool: &Pool, namespace: &str, key: &str) -> Result<bool, Status> {
    let client = pool.get().await.internal()?;
    let deleted = client
        .execute(
            "DELETE FROM wr_secrets WHERE namespace = $1 AND key = $2",
            &[&namespace, &key],
        )
        .await
        .internal()?;
    Ok(deleted > 0)
}

/// List secret metadata (namespace + key only, no values).
/// If namespace is empty, return all secrets across all namespaces.
pub async fn list_secrets(pool: &Pool, namespace: &str) -> Result<Vec<(String, String)>, Status> {
    let client = pool.get().await.internal()?;
    let rows = if namespace.is_empty() {
        client
            .query(
                "SELECT namespace, key FROM wr_secrets ORDER BY namespace, key",
                &[],
            )
            .await
            .internal()?
    } else {
        client
            .query(
                "SELECT namespace, key FROM wr_secrets WHERE namespace = $1 ORDER BY key",
                &[&namespace],
            )
            .await
            .internal()?
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
    let client = pool.get().await.internal()?;
    let mut results = Vec::with_capacity(requests.len());
    for (namespace, key) in requests {
        let row = client
            .query_opt(
                "SELECT namespace, key, ciphertext, nonce FROM wr_secrets
                 WHERE namespace = $1 AND key = $2",
                &[namespace, key],
            )
            .await
            .internal()?;
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
    let client = pool.get().await.internal()?;
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
        .internal()?;
    Ok(())
}

/// Remove this manager from the cluster.
pub async fn deregister_manager(pool: &Pool, manager_id: &str) -> Result<(), Status> {
    let client = pool.get().await.internal()?;
    client
        .execute(
            "DELETE FROM wr_managers WHERE manager_id = $1",
            &[&manager_id],
        )
        .await
        .internal()?;
    Ok(())
}

/// List all managers that have heartbeated within the given threshold.
pub async fn list_managers(pool: &Pool) -> Result<Vec<ManagerRecord>, Status> {
    let client = pool.get().await.internal()?;
    let rows = client
        .query(
            "SELECT manager_id, grpc_address, gossip_address FROM wr_managers
             WHERE last_heartbeat > NOW() - INTERVAL '60 seconds'",
            &[],
        )
        .await
        .internal()?;
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
    let client = pool.get().await.internal()?;
    client
        .execute(
            "UPDATE wr_managers SET last_heartbeat = NOW() WHERE manager_id = $1",
            &[&manager_id],
        )
        .await
        .internal()?;
    Ok(())
}

/// Remove managers that haven't heartbeated within the threshold. Returns count deleted.
pub async fn cleanup_stale_managers(pool: &Pool, stale_threshold_secs: i64) -> Result<u64, Status> {
    let client = pool.get().await.internal()?;
    let deleted = client
        .execute(
            "DELETE FROM wr_managers WHERE last_heartbeat < NOW() - make_interval(secs => $1::double precision)",
            &[&(stale_threshold_secs as f64)],
        )
        .await
        .internal()?;
    Ok(deleted)
}

/// Get a schema by (namespace, module, version).
pub async fn get_schema(
    pool: &Pool,
    namespace: &str,
    module: &str,
    version: &str,
) -> Result<Vec<u8>, Status> {
    let client = pool.get().await.internal()?;
    let row = client
        .query_opt(
            "SELECT proto_schema FROM wr_schemas
             WHERE namespace = $1 AND module_name = $2 AND version = $3",
            &[&namespace, &module, &version],
        )
        .await
        .internal()?
        .ok_or_else(|| {
            Status::not_found(format!("no schema for {namespace}/{module}/{version}"))
        })?;
    Ok(row.get(0))
}

// ── Schedule operations ────────────────────────────────────────────────────

const SCHEDULE_COLUMNS: &str = "schedule_id, worker_namespace, worker_name, worker_version, \
    job_type, interval_secs, immediate, payload, timeout_secs, max_attempts, enabled, \
    last_fired_at, next_fire_at, last_error, consecutive_failures, claim_id::text AS claim_id";

pub struct ScheduleRow {
    pub schedule_id: String,
    pub worker_namespace: String,
    pub worker_name: String,
    pub worker_version: String,
    pub job_type: String,
    pub interval_secs: i32,
    pub immediate: bool,
    pub payload: Vec<u8>,
    pub timeout_secs: i32,
    pub max_attempts: i32,
    pub enabled: bool,
    pub last_fired_at: Option<chrono::DateTime<chrono::Utc>>,
    pub next_fire_at: Option<chrono::DateTime<chrono::Utc>>,
    pub last_error: Option<String>,
    pub consecutive_failures: i32,
    pub claim_id: Option<String>,
}

fn row_to_schedule(row: &tokio_postgres::Row) -> ScheduleRow {
    ScheduleRow {
        schedule_id: row.get(0),
        worker_namespace: row.get(1),
        worker_name: row.get(2),
        worker_version: row.get(3),
        job_type: row.get(4),
        interval_secs: row.get(5),
        immediate: row.get(6),
        payload: row.get(7),
        timeout_secs: row.get(8),
        max_attempts: row.get(9),
        enabled: row.get(10),
        last_fired_at: row.get(11),
        next_fire_at: row.get(12),
        last_error: row.get(13),
        consecutive_failures: row.get(14),
        claim_id: row.get(15),
    }
}

/// Upsert a schedule by its natural key (namespace, name, version, job_type).
/// Returns the schedule_id.
#[allow(clippy::too_many_arguments)]
pub async fn upsert_schedule(
    pool: &Pool,
    worker_namespace: &str,
    worker_name: &str,
    worker_version: &str,
    job_type: &str,
    interval_secs: i32,
    immediate: bool,
    payload: &[u8],
    timeout_secs: i32,
    max_attempts: i32,
) -> Result<String, Status> {
    let client = pool.get().await.internal()?;
    let row = client
        .query_one(
            "INSERT INTO wr_schedules
                (worker_namespace, worker_name, worker_version, job_type,
                 interval_secs, immediate, payload, timeout_secs, max_attempts, enabled, next_fire_at)
             VALUES ($1, $2, $3, $4, $5::int, $6, $7, $8, $9, TRUE,
                 CASE WHEN $6 THEN NOW()
                      ELSE NOW() + make_interval(secs => $5::double precision) END)
             ON CONFLICT (worker_namespace, worker_name, worker_version, job_type)
             DO UPDATE SET
                interval_secs = EXCLUDED.interval_secs,
                immediate     = EXCLUDED.immediate,
                payload       = EXCLUDED.payload,
                timeout_secs  = EXCLUDED.timeout_secs,
                max_attempts  = EXCLUDED.max_attempts,
                enabled       = TRUE,
                next_fire_at  = CASE
                    WHEN wr_schedules.last_fired_at IS NULL AND EXCLUDED.immediate THEN NOW()
                    WHEN wr_schedules.last_fired_at IS NULL
                        THEN NOW() + make_interval(secs => EXCLUDED.interval_secs::double precision)
                    ELSE wr_schedules.last_fired_at + make_interval(secs => EXCLUDED.interval_secs::double precision)
                  END,
                claimed_by    = NULL,
                claimed_until = NULL,
                claim_id      = NULL,
                updated_at    = NOW()
             RETURNING schedule_id",
            &[
                &worker_namespace,
                &worker_name,
                &worker_version,
                &job_type,
                &interval_secs,
                &immediate,
                &payload,
                &timeout_secs,
                &max_attempts,
            ],
        )
        .await
        .internal()?;
    Ok(row.get(0))
}

/// Delete a schedule by natural key. Returns true if a row was deleted.
pub async fn delete_schedule(
    pool: &Pool,
    worker_namespace: &str,
    worker_name: &str,
    worker_version: &str,
    job_type: &str,
) -> Result<bool, Status> {
    let client = pool.get().await.internal()?;
    let deleted = client
        .execute(
            "DELETE FROM wr_schedules
             WHERE worker_namespace = $1 AND worker_name = $2
               AND worker_version = $3 AND job_type = $4",
            &[&worker_namespace, &worker_name, &worker_version, &job_type],
        )
        .await
        .internal()?;
    Ok(deleted > 0)
}

/// List schedules, optionally filtered by namespace. Empty namespace returns all.
pub async fn list_schedules(
    pool: &Pool,
    worker_namespace: &str,
) -> Result<Vec<ScheduleRow>, Status> {
    let client = pool.get().await.internal()?;
    let rows = if worker_namespace.is_empty() {
        let sql = format!(
            "SELECT {SCHEDULE_COLUMNS} FROM wr_schedules
             ORDER BY worker_namespace, worker_name, job_type"
        );
        client.query(&sql, &[]).await.internal()?
    } else {
        let sql = format!(
            "SELECT {SCHEDULE_COLUMNS} FROM wr_schedules
             WHERE worker_namespace = $1
             ORDER BY worker_name, job_type"
        );
        client.query(&sql, &[&worker_namespace]).await.internal()?
    };
    Ok(rows.iter().map(row_to_schedule).collect())
}

/// Claim due, unleased (or lease-expired) schedules atomically, stamping a fresh
/// lease + fencing `claim_id`. Runs in a short transaction the caller commits
/// immediately. `SKIP LOCKED` keeps concurrent managers from double-claiming.
pub async fn claim_due_schedules(
    txn: &deadpool_postgres::Transaction<'_>,
    claimer_id: &str,
    lease_secs: f64,
) -> Result<Vec<ScheduleRow>, Status> {
    let sql = format!(
        "UPDATE wr_schedules
         SET claimed_by      = $1,
             claimed_until   = NOW() + make_interval(secs => $2::double precision),
             claim_id        = gen_random_uuid(),
             last_attempt_at = NOW()
         WHERE schedule_id IN (
             SELECT schedule_id FROM wr_schedules
             WHERE enabled = TRUE
               AND next_fire_at <= NOW()
               AND (claimed_until IS NULL OR claimed_until < NOW())
             FOR UPDATE SKIP LOCKED
         )
         RETURNING {SCHEDULE_COLUMNS}"
    );
    let rows = txn
        .query(&sql, &[&claimer_id, &lease_secs])
        .await
        .internal()?;
    Ok(rows.iter().map(row_to_schedule).collect())
}

/// Fenced success finalize. Advances next_fire_at by one interval, clears the
/// lease/error/claim. The `claim_id` guard drops stale (reclaimed) attempts.
/// Returns rows affected (0 == fenced out).
pub async fn mark_schedule_succeeded(
    pool: &Pool,
    schedule_id: &str,
    claim_id: &str,
) -> Result<u64, Status> {
    let client = pool.get().await.internal()?;
    let n = client
        .execute(
            "UPDATE wr_schedules
             SET last_fired_at        = NOW(),
                 next_fire_at         = NOW() + make_interval(secs => interval_secs::double precision),
                 last_error           = NULL,
                 consecutive_failures = 0,
                 claimed_by           = NULL,
                 claimed_until        = NULL,
                 claim_id             = NULL,
                 updated_at           = NOW()
             WHERE schedule_id = $1 AND claim_id::text = $2",
            &[&schedule_id, &claim_id],
        )
        .await
        .internal()?;
    Ok(n)
}

/// Fenced failure finalize. Records the error, bumps consecutive_failures, sets a
/// backed-off next_fire_at, clears the lease/claim. `claim_id` guard as above.
/// Returns rows affected (0 == fenced out).
pub async fn mark_schedule_failed(
    pool: &Pool,
    schedule_id: &str,
    claim_id: &str,
    error: &str,
    backoff_secs: f64,
) -> Result<u64, Status> {
    let client = pool.get().await.internal()?;
    let n = client
        .execute(
            "UPDATE wr_schedules
             SET last_error           = $3,
                 consecutive_failures = consecutive_failures + 1,
                 next_fire_at         = NOW() + make_interval(secs => $4::double precision),
                 claimed_by           = NULL,
                 claimed_until        = NULL,
                 claim_id             = NULL,
                 updated_at           = NOW()
             WHERE schedule_id = $1 AND claim_id::text = $2",
            &[&schedule_id, &claim_id, &error, &backoff_secs],
        )
        .await
        .internal()?;
    Ok(n)
}
