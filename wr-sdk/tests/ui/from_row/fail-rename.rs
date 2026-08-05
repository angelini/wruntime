use wr_sdk::db::FromRow;

#[derive(FromRow)]
struct InvalidRename {
    #[wr_db(rename = 42)]
    id: i64,
}

#[derive(FromRow)]
struct ConflictingRename {
    #[wr_db(rename = "id", flatten)]
    nested: Nested,
}

struct Nested;

fn main() {}
