use sha2::{Digest, Sha256};

const POSTGRES_IDENTIFIER_MAX_BYTES: usize = 63;
const MIGRATION_EXECUTOR_SUFFIX_BYTES: usize = 8;

/// Encode a canonical identity for storage while preserving the legacy
/// hyphen-to-underscore mapping. Canonical identities reject underscores and
/// other punctuation, so this mapping is collision-free for accepted inputs.
fn storage_component(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() {
            output.push(char::from(byte));
        } else if byte == b'-' {
            output.push('_');
        } else {
            use std::fmt::Write as _;
            write!(&mut output, "_{byte:02x}").expect("writing to String cannot fail");
        }
    }
    output
}

fn identity_digest(domain: &str, components: &[&str]) -> String {
    let mut hash = Sha256::new();
    hash.update((domain.len() as u64).to_be_bytes());
    hash.update(domain.as_bytes());
    for component in components {
        hash.update((component.len() as u64).to_be_bytes());
        hash.update(component.as_bytes());
    }
    format!("{:x}", hash.finalize())
}

fn bounded_postgres_identifier(
    prefix: &str,
    domain: &str,
    components: &[&str],
    always_hash: bool,
    max_bytes: usize,
) -> String {
    let encoded = components
        .iter()
        .map(|component| storage_component(component))
        .collect::<Vec<_>>()
        .join("__");
    let readable = format!("{prefix}{encoded}");
    if !always_hash && readable.len() <= max_bytes {
        return readable;
    }

    let digest = identity_digest(domain, components);
    let suffix = format!("_{}", &digest[..12]);
    let readable_bytes = max_bytes
        .checked_sub(suffix.len())
        .expect("PostgreSQL identifier budget must fit its digest suffix");
    let mut bounded = readable;
    bounded.truncate(readable_bytes);
    bounded.push_str(&suffix);
    bounded
}

fn namespace_identifier(prefix: &str, domain: &str, namespace: &str) -> String {
    bounded_postgres_identifier(
        prefix,
        domain,
        &[namespace],
        false,
        POSTGRES_IDENTIFIER_MAX_BYTES,
    )
}

/// Returns the Postgres schema name for a module.
///
/// This remains an organization/default-resolution name. It is not an
/// authorization boundary; namespace databases and roles provide that boundary.
pub fn module_schema(namespace: &str, name: &str) -> String {
    format!(
        "wr__{}__{}",
        storage_component(namespace),
        storage_component(name)
    )
}

/// Returns the stable `NOLOGIN` runtime group for a namespace.
pub fn namespace_runtime_group(namespace: &str) -> String {
    namespace_identifier("wr_ns_", "namespace-runtime-group", namespace)
}

/// Existing generic symbol for the namespace runtime group. This does not
/// denote a login role.
pub fn namespace_role(namespace: &str) -> String {
    namespace_runtime_group(namespace)
}

/// Returns the private database name for a namespace.
pub fn namespace_database(namespace: &str) -> String {
    namespace_identifier("wr_db_", "namespace-database", namespace)
}

/// Returns the stable `NOLOGIN` owner of tenant schemas and objects.
pub fn namespace_owner(namespace: &str) -> String {
    namespace_identifier("wr_owner_", "namespace-owner", namespace)
}

/// Returns the platform-owned namespace maintenance role.
pub fn namespace_maintenance_role(namespace: &str) -> String {
    namespace_identifier("wr_maint_", "namespace-maintenance", namespace)
}

/// Returns the bounded LOGIN role used by one node for one namespace.
pub fn namespace_runtime_login(node: &str, namespace: &str) -> String {
    bounded_postgres_identifier(
        "wr_runtime_",
        "namespace-runtime-login",
        &[node, namespace],
        true,
        POSTGRES_IDENTIFIER_MAX_BYTES,
    )
}

/// Returns the read-only readiness LOGIN role used by one node for one namespace.
pub fn namespace_readiness_verifier(node: &str, namespace: &str) -> String {
    bounded_postgres_identifier(
        "wr_ready_",
        "namespace-readiness-verifier",
        &[node, namespace],
        true,
        POSTGRES_IDENTIFIER_MAX_BYTES,
    )
}

/// Returns a bounded prefix for disposable migration executor role names.
/// Callers may append an eight-character hexadecimal nonce without exceeding the
/// PostgreSQL 63-byte identifier limit.
pub fn migration_executor_prefix(namespace: &str) -> String {
    let mut prefix = bounded_postgres_identifier(
        "wr_migrate_",
        "migration-executor",
        &[namespace],
        true,
        POSTGRES_IDENTIFIER_MAX_BYTES - MIGRATION_EXECUTOR_SUFFIX_BYTES - 1,
    );
    prefix.push('_');
    prefix
}

/// Returns the S3 key prefix for a module's blobstore namespace.
pub fn blob_key_prefix(namespace: &str) -> String {
    format!("wr/{}/", storage_component(namespace))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derived_names_are_injective_for_previous_collisions() {
        assert_ne!(
            module_schema("foo-bar", "a.b"),
            module_schema("foo_bar", "a/b")
        );
        assert_ne!(namespace_role("foo-bar"), namespace_role("foo_bar"));
        assert_ne!(blob_key_prefix("foo-bar"), blob_key_prefix("foo_bar"));
    }

    #[test]
    fn canonical_names_remain_stable() {
        assert_eq!(
            module_schema("ecommerce", "order-service"),
            "wr__ecommerce__order_service"
        );
        assert_eq!(namespace_role("my-ns"), "wr_ns_my_ns");
        assert_eq!(namespace_database("my-ns"), "wr_db_my_ns");
        assert_eq!(namespace_owner("my-ns"), "wr_owner_my_ns");
        assert_eq!(namespace_maintenance_role("my-ns"), "wr_maint_my_ns");
        assert_eq!(blob_key_prefix("my-ns"), "wr/my_ns/");
    }

    #[test]
    fn postgres_names_are_bounded_stable_and_collision_resistant() {
        let namespace_a = format!("{}a", "very-long-namespace-".repeat(8));
        let namespace_b = format!("{}b", "very-long-namespace-".repeat(8));
        let node_a = format!("{}a", "very-long-node-".repeat(8));
        let node_b = format!("{}b", "very-long-node-".repeat(8));

        let names_a = [
            namespace_database(&namespace_a),
            namespace_owner(&namespace_a),
            namespace_runtime_group(&namespace_a),
            namespace_runtime_login(&node_a, &namespace_a),
            namespace_readiness_verifier(&node_a, &namespace_a),
            namespace_maintenance_role(&namespace_a),
            migration_executor_prefix(&namespace_a),
        ];
        assert!(names_a.iter().all(|name| name.len() <= 63));
        assert_eq!(
            namespace_runtime_login(&node_a, &namespace_a),
            namespace_runtime_login(&node_a, &namespace_a)
        );
        assert_ne!(
            namespace_database(&namespace_a),
            namespace_database(&namespace_b)
        );
        assert_ne!(
            namespace_runtime_login(&node_a, &namespace_a),
            namespace_runtime_login(&node_b, &namespace_a)
        );
        assert_ne!(
            namespace_readiness_verifier(&node_a, &namespace_a),
            namespace_readiness_verifier(&node_a, &namespace_b)
        );
        assert!(format!("{}deadbeef", migration_executor_prefix(&namespace_a)).len() <= 63);
    }

    #[test]
    fn composite_login_names_do_not_depend_on_ambiguous_separators() {
        assert_ne!(
            namespace_runtime_login("a-", "b"),
            namespace_runtime_login("a", "-b")
        );
    }
}
