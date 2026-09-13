pub use wr_common::naming::{blob_key_prefix, module_schema};
pub use wr_common::pool::build_pool;

#[cfg(test)]
pub(crate) fn build_test_guest_pool(
    database_url: &str,
    role: &str,
    password: &str,
    max_size: usize,
) -> anyhow::Result<deadpool_postgres::Pool> {
    use std::str::FromStr as _;
    let mut config = tokio_postgres::Config::from_str(database_url)?;
    config.user(role).password(password);
    wr_common::pool::build_guest_pool_with_connector(config, tokio_postgres::NoTls, max_size)
}
