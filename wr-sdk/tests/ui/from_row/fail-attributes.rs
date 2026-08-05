use wr_sdk::db::FromRow;

#[derive(FromRow)]
#[wr_db(rename = "container")]
struct ContainerAttribute {
    id: i64,
}

#[derive(FromRow)]
struct UnknownAttribute {
    #[wr_db(unknown)]
    id: i64,
}

#[derive(FromRow)]
struct DefaultAttribute {
    #[wr_db(default)]
    id: i64,
}

#[derive(FromRow)]
struct BareAttribute {
    #[wr_db]
    id: i64,
}

#[derive(FromRow)]
struct EmptyAttribute {
    #[wr_db()]
    id: i64,
}

fn main() {}
