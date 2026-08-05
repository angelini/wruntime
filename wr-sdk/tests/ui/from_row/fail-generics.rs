use wr_sdk::db::FromRow;

#[derive(FromRow)]
struct TypeGeneric<T> {
    value: T,
}

#[derive(FromRow)]
struct LifetimeGeneric<'a> {
    value: &'a str,
}

#[derive(FromRow)]
struct ConstGeneric<const N: usize> {
    value: [u8; N],
}

#[derive(FromRow)]
struct WhereOnly
where
    i64: Copy,
{
    value: i64,
}

fn main() {}
