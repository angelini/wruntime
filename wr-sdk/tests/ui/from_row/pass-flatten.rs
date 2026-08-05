#![allow(dead_code)]

use wr_sdk::db::FromRow;

#[derive(FromRow)]
struct Identity {
    id: i64,
}

#[derive(FromRow)]
struct State {
    active: bool,
}

#[derive(FromRow)]
struct FlattenedRow {
    #[wr_db(flatten)]
    identity: Identity,
    #[wr_db(flatten)]
    state: State,
}

fn assert_from_row<T: wr_sdk::db::FromRow>() {}

fn main() {
    assert_from_row::<FlattenedRow>();
}
