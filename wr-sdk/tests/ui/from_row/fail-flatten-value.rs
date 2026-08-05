use wr_sdk::db::FromRow;

#[derive(FromRow)]
struct FlattenValue {
    #[wr_db(flatten = true)]
    nested: Nested,
}

struct Nested;

fn main() {}
