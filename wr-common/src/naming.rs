/// Replace non-alphanumeric characters with `_`.
fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_alphanumeric() { c } else { '_' })
        .collect()
}

/// Returns the Postgres schema name for a module.
/// Format: `wr__{namespace}__{name}` with non-alphanumeric chars replaced by `_`.
pub fn module_schema(namespace: &str, name: &str) -> String {
    format!("wr__{}__{}", sanitize(namespace), sanitize(name))
}

/// Returns the Postgres role name for a namespace.
/// Format: `wr_ns_{namespace}` with non-alphanumeric chars replaced by `_`.
pub fn namespace_role(namespace: &str) -> String {
    format!("wr_ns_{}", sanitize(namespace))
}

/// Returns the S3 key prefix for a module's blobstore namespace.
/// Format: `wr/{namespace}/` with non-alphanumeric chars replaced by `_`.
/// Scoped to namespace only (not module name) so modules within the same
/// namespace can share blobstore data.
pub fn blob_key_prefix(namespace: &str) -> String {
    format!("wr/{}/", sanitize(namespace))
}

#[cfg(test)]
mod tests {
    use super::{blob_key_prefix, module_schema, namespace_role};

    #[test]
    fn test_blob_key_prefix_simple() {
        assert_eq!(blob_key_prefix("ecommerce"), "wr/ecommerce/");
    }

    #[test]
    fn test_blob_key_prefix_special_chars() {
        assert_eq!(blob_key_prefix("my-ns"), "wr/my_ns/");
    }

    #[test]
    fn test_namespace_role_simple() {
        assert_eq!(namespace_role("ecommerce"), "wr_ns_ecommerce");
    }

    #[test]
    fn test_namespace_role_special_chars() {
        assert_eq!(namespace_role("my-ns"), "wr_ns_my_ns");
    }

    #[test]
    fn test_module_schema_simple() {
        assert_eq!(
            module_schema("ecommerce", "inventory"),
            "wr__ecommerce__inventory"
        );
    }

    #[test]
    fn test_module_schema_hyphens_and_dots() {
        assert_eq!(module_schema("my-ns", "my.module"), "wr__my_ns__my_module");
    }

    #[test]
    fn test_module_schema_mixed_case() {
        assert_eq!(module_schema("Foo", "Bar"), "wr__Foo__Bar");
    }

    #[test]
    fn test_module_schema_special_chars() {
        assert_eq!(module_schema("a b", "c/d"), "wr__a_b__c_d");
    }
}
