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

/// Returns the Postgres schema name for a module.
pub fn module_schema(namespace: &str, name: &str) -> String {
    format!(
        "wr__{}__{}",
        storage_component(namespace),
        storage_component(name)
    )
}

/// Returns the Postgres role name for a namespace.
pub fn namespace_role(namespace: &str) -> String {
    format!("wr_ns_{}", storage_component(namespace))
}

/// Returns the S3 key prefix for a module's blobstore namespace.
pub fn blob_key_prefix(namespace: &str) -> String {
    format!("wr/{}/", storage_component(namespace))
}

#[cfg(test)]
mod tests {
    use super::{blob_key_prefix, module_schema, namespace_role};

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
        assert_eq!(blob_key_prefix("my-ns"), "wr/my_ns/");
    }
}
