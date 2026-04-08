/// Build the manager's connection pool with `search_path = wr_system` so all
/// manager tables are resolved from the private schema, not `public`.
pub fn build_pool(database_url: &str, max_size: usize) -> anyhow::Result<deadpool_postgres::Pool> {
    wr_common::pool::build_pool_with_search_path(database_url, max_size, "wr_system")
}
