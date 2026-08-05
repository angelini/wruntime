use wr_sdk::db::FromRow;

#[derive(FromRow)]
struct DuplicateRename {
    #[wr_db(rename = "first", rename = "second")]
    id: i64,
}

#[derive(FromRow)]
struct DuplicateFlatten {
    #[wr_db(flatten, flatten)]
    nested: Nested,
}

struct Nested;

fn main() {}
