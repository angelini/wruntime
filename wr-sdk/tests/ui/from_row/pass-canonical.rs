#![allow(dead_code)]

#[derive(wr_sdk::db::FromRow)]
struct CanonicalPath {
    id: i64,
}

fn assert_from_row<T: wr_sdk::db::FromRow>() {}

fn main() {
    assert_from_row::<CanonicalPath>();
}
