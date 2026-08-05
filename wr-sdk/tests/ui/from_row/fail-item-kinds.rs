use wr_sdk::db::FromRow;

#[derive(FromRow)]
enum Choice {
    One,
}

#[derive(FromRow)]
union Either {
    integer: i64,
    float: f64,
}

fn main() {}
