use anyhow::{Context, Result};
use deadpool_postgres::{GenericClient as _, Pool};

/// Complete target-database contract for one namespace.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamespaceProvisioning {
    pub namespace: String,
    pub role: String,
    pub password: String,
    pub schemas: Vec<String>,
}

const PROVISIONING_LOCK_DOMAIN: i32 = 0x5752_4442;

fn namespace_lock_key(namespace: &str) -> i32 {
    let mut hash: u32 = 0x811c9dc5;
    for byte in namespace.as_bytes() {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    hash as i32
}

async fn quote_identifier_and_literal(
    client: &deadpool_postgres::ClientWrapper,
    identifier: &str,
    literal: &str,
) -> Result<(String, String)> {
    let row = client
        .query_one(
            "SELECT quote_ident($1::text), quote_literal($2::text)",
            &[&identifier, &literal],
        )
        .await
        .context("failed to quote database provisioning values")?;
    Ok((row.get(0), row.get(1)))
}

/// Provision namespace roles and schemas on the engine's target database.
/// Each namespace uses a detached physical connection and one transaction.
pub async fn provision_namespaces(
    pool: &Pool,
    specifications: &[NamespaceProvisioning],
) -> Result<()> {
    for specification in specifications {
        provision_namespace(pool, specification).await?;
    }
    Ok(())
}

async fn provision_namespace(pool: &Pool, specification: &NamespaceProvisioning) -> Result<()> {
    let pooled = pool
        .get()
        .await
        .context("failed to acquire database provisioning connection")?;
    let mut client = deadpool_postgres::Object::take(pooled);
    let (quoted_role, quoted_password) =
        quote_identifier_and_literal(&client, &specification.role, &specification.password).await?;
    let mut quoted_schemas = Vec::with_capacity(specification.schemas.len());
    for schema in &specification.schemas {
        let row = client
            .query_one("SELECT quote_ident($1::text)", &[schema])
            .await
            .context("failed to quote database schema identifier")?;
        quoted_schemas.push(row.get::<_, String>(0));
    }

    let transaction = client
        .transaction()
        .await
        .context("failed to begin database provisioning transaction")?;
    transaction
        .execute(
            "SELECT pg_advisory_xact_lock($1, $2)",
            &[
                &PROVISIONING_LOCK_DOMAIN,
                &namespace_lock_key(&specification.namespace),
            ],
        )
        .await
        .context("failed to acquire namespace provisioning lock")?;

    let create_role = format!(
        "DO $wr_provision$ BEGIN BEGIN CREATE ROLE {quoted_role} LOGIN PASSWORD {quoted_password}; \
         EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL; END; END $wr_provision$"
    );
    transaction
        .batch_execute(&create_role)
        .await
        .map_err(|_| anyhow::anyhow!("failed to converge namespace database role"))?;
    transaction
        .batch_execute(&format!(
            "ALTER ROLE {quoted_role} LOGIN PASSWORD {quoted_password}"
        ))
        .await
        .map_err(|_| anyhow::anyhow!("failed to synchronize namespace database credential"))?;

    for quoted_schema in quoted_schemas {
        transaction
            .batch_execute(&format!(
                "CREATE SCHEMA IF NOT EXISTS {quoted_schema}; \
                 ALTER SCHEMA {quoted_schema} OWNER TO {quoted_role}; \
                 GRANT ALL ON SCHEMA {quoted_schema} TO {quoted_role}; \
                 GRANT ALL ON ALL TABLES IN SCHEMA {quoted_schema} TO {quoted_role}; \
                 GRANT ALL ON ALL SEQUENCES IN SCHEMA {quoted_schema} TO {quoted_role}; \
                 GRANT ALL ON ALL FUNCTIONS IN SCHEMA {quoted_schema} TO {quoted_role}; \
                 ALTER DEFAULT PRIVILEGES IN SCHEMA {quoted_schema} GRANT ALL ON TABLES TO {quoted_role}; \
                 ALTER DEFAULT PRIVILEGES IN SCHEMA {quoted_schema} GRANT ALL ON SEQUENCES TO {quoted_role}; \
                 ALTER DEFAULT PRIVILEGES IN SCHEMA {quoted_schema} GRANT ALL ON FUNCTIONS TO {quoted_role};"
            ))
            .await
            .map_err(|_| anyhow::anyhow!("failed to converge namespace schema privileges"))?;
    }

    transaction
        .commit()
        .await
        .map_err(|_| anyhow::anyhow!("failed to commit namespace provisioning"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provisioning_lock_is_deterministic_and_namespaced() {
        assert_eq!(namespace_lock_key("shop"), namespace_lock_key("shop"));
        assert_ne!(namespace_lock_key("shop"), namespace_lock_key("billing"));
        assert_ne!(PROVISIONING_LOCK_DOMAIN, namespace_lock_key("shop"));
    }
}
