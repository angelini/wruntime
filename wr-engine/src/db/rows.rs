use chrono::Timelike as _;

use super::params::PgIntervalRaw;
use super::wruntime::db::database::{Column, PgInterval, PgValue, Row};

// ── Row conversion ───────────────────────────────────────────────────────────

pub(crate) fn pg_row_to_wit(row: &tokio_postgres::Row) -> Row {
    let columns = row
        .columns()
        .iter()
        .enumerate()
        .map(|(i, col)| Column {
            name: col.name().to_string(),
            value: pg_col_to_wit(row, i, col.type_()),
        })
        .collect();
    Row { columns }
}

/// Maps a single Postgres column value to a WIT `PgValue`.
///
/// The `pg_col!` macro reduces the per-arm boilerplate. Each invocation
/// generates: `opt(row.get::<_, Option<$rust_ty>>(i), $map_fn)`.
macro_rules! pg_col {
    // Simple: Type → PgValue variant, no transform needed
    ($row:ident, $i:ident, $rust_ty:ty, $variant:expr) => {
        opt($row.get::<_, Option<$rust_ty>>($i), $variant)
    };
    // Transform: Type → PgValue via closure
    ($row:ident, $i:ident, $rust_ty:ty, |$v:ident| $body:expr) => {
        opt($row.get::<_, Option<$rust_ty>>($i), |$v| $body)
    };
}

fn pg_col_to_wit(row: &tokio_postgres::Row, i: usize, ty: &tokio_postgres::types::Type) -> PgValue {
    use tokio_postgres::types::Type;

    match *ty {
        // ── Scalars ─────────────────────────────────────────────────────
        Type::BOOL => pg_col!(row, i, bool, PgValue::Boolean),
        Type::INT2 => pg_col!(row, i, i16, PgValue::Int2),
        Type::INT4 => pg_col!(row, i, i32, PgValue::Int4),
        Type::INT8 => pg_col!(row, i, i64, PgValue::Int8),
        Type::FLOAT4 => pg_col!(row, i, f32, PgValue::Float4),
        Type::FLOAT8 => pg_col!(row, i, f64, PgValue::Float8),
        Type::OID => pg_col!(row, i, u32, PgValue::Oid),
        Type::BYTEA => pg_col!(row, i, Vec<u8>, PgValue::Bytea),
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => {
            pg_col!(row, i, String, PgValue::Text)
        }

        // ── Scalars with transforms ─────────────────────────────────────
        Type::TIMESTAMPTZ => pg_col!(row, i, chrono::DateTime<chrono::Utc>, |dt| {
            PgValue::Timestamptz(dt.timestamp_micros())
        }),
        Type::TIMESTAMP => pg_col!(row, i, chrono::NaiveDateTime, |dt| PgValue::Timestamp(
            dt.and_utc().timestamp_micros()
        )),
        Type::DATE => pg_col!(row, i, chrono::NaiveDate, |d| {
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            PgValue::Date((d - epoch).num_days() as i32)
        }),
        Type::TIME => pg_col!(row, i, chrono::NaiveTime, |t| {
            let micros =
                t.num_seconds_from_midnight() as i64 * 1_000_000 + t.nanosecond() as i64 / 1_000;
            PgValue::Time(micros)
        }),
        Type::NUMERIC => pg_col!(row, i, rust_decimal::Decimal, |d| PgValue::Numeric(
            d.to_string()
        )),
        Type::UUID => pg_col!(row, i, uuid::Uuid, |u| {
            let n = u.as_u128();
            PgValue::Uuid(((n >> 64) as u64, n as u64))
        }),
        Type::JSON | Type::JSONB => {
            pg_col!(row, i, serde_json::Value, |v| PgValue::Jsonb(v.to_string()))
        }
        Type::INTERVAL => pg_col!(row, i, PgIntervalRaw, |iv| PgValue::Interval(PgInterval {
            months: iv.months,
            days: iv.days,
            microseconds: iv.microseconds,
        })),

        // ── Simple arrays ─────────────────────────────────────────────────
        Type::BOOL_ARRAY => pg_col!(row, i, Vec<Option<bool>>, PgValue::BoolArray),
        Type::INT2_ARRAY => pg_col!(row, i, Vec<Option<i16>>, PgValue::Int2Array),
        Type::INT4_ARRAY => pg_col!(row, i, Vec<Option<i32>>, PgValue::Int4Array),
        Type::INT8_ARRAY => pg_col!(row, i, Vec<Option<i64>>, PgValue::Int8Array),
        Type::FLOAT4_ARRAY => pg_col!(row, i, Vec<Option<f32>>, PgValue::Float4Array),
        Type::FLOAT8_ARRAY => pg_col!(row, i, Vec<Option<f64>>, PgValue::Float8Array),
        Type::TEXT_ARRAY | Type::VARCHAR_ARRAY => {
            pg_col!(row, i, Vec<Option<String>>, PgValue::TextArray)
        }

        // ── Arrays with transforms ──────────────────────────────────────
        Type::TIMESTAMPTZ_ARRAY => {
            pg_col!(row, i, Vec<Option<chrono::DateTime<chrono::Utc>>>, |arr| {
                PgValue::TimestamptzArray(
                    arr.into_iter()
                        .map(|o| o.map(|dt| dt.timestamp_micros()))
                        .collect(),
                )
            })
        }
        Type::TIMESTAMP_ARRAY => pg_col!(row, i, Vec<Option<chrono::NaiveDateTime>>, |arr| {
            PgValue::TimestampArray(
                arr.into_iter()
                    .map(|o| o.map(|dt| dt.and_utc().timestamp_micros()))
                    .collect(),
            )
        }),
        Type::UUID_ARRAY => pg_col!(row, i, Vec<Option<uuid::Uuid>>, |arr| PgValue::UuidArray(
            arr.into_iter()
                .map(|o| o.map(|u| {
                    let n = u.as_u128();
                    ((n >> 64) as u64, n as u64)
                }))
                .collect()
        )),
        Type::JSON_ARRAY | Type::JSONB_ARRAY => {
            pg_col!(row, i, Vec<Option<serde_json::Value>>, |arr| {
                PgValue::JsonbArray(arr.into_iter().map(|o| o.map(|v| v.to_string())).collect())
            })
        }

        _ => {
            tracing::warn!(
                col  = %row.columns()[i].name(),
                pg_type = %ty,
                "unsupported column type, returning null",
            );
            PgValue::Null
        }
    }
}

#[inline]
fn opt<T, F: FnOnce(T) -> PgValue>(val: Option<T>, f: F) -> PgValue {
    val.map_or(PgValue::Null, f)
}
