use wr_sdk::db::FromRow;

#[derive(FromRow)]
struct Tuple(i64);

#[derive(FromRow)]
struct Unit;

fn main() {}
