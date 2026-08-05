#![allow(dead_code)]

use serde::Deserialize;
use wr_sdk::db::{FromRow, Json};

#[derive(Deserialize)]
struct Payload {
    value: String,
}

#[derive(FromRow)]
struct JsonRow {
    payload: Option<Json<Payload>>,
}

fn assert_from_row<T: wr_sdk::db::FromRow>() {}

fn main() {
    assert_from_row::<JsonRow>();
}
