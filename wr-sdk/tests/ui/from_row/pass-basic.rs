#![allow(dead_code)]

use wr_sdk::db::FromRow;

#[derive(FromRow)]
struct BasicRow {
    id: i64,
    #[wr_db(rename = "display_name")]
    name: String,
    active: Option<bool>,
}

fn assert_from_row<T: wr_sdk::db::FromRow>() {}

fn main() {
    assert_from_row::<BasicRow>();
}
