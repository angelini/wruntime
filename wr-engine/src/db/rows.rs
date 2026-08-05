use chrono::Timelike as _;
use tokio_postgres::types::{FromSql, FromSqlOwned, Kind, Type};

use super::params::{postgres_type_to_pg_type, PgIntervalRaw};
use super::wruntime::db::database::{
    Column, DbError, PgInterval, PgType, PgValue, Row, UnsupportedResultType,
};

// ── Row conversion ───────────────────────────────────────────────────────────

pub(crate) fn pg_row_to_wit(row: &tokio_postgres::Row) -> Result<Row, DbError> {
    let columns = row
        .columns()
        .iter()
        .enumerate()
        .map(|(i, col)| {
            Ok(Column {
                name: col.name().to_string(),
                value: pg_col_to_wit(row, i, col)?,
            })
        })
        .collect::<Result<Vec<_>, DbError>>()?;
    Ok(Row { columns })
}

macro_rules! pg_col {
    ($row:ident, $i:ident, $ty:ident, $pg_type:ident, $rust_ty:ty, $convert:expr) => {
        extract::<$rust_ty, _>($row, $i, $ty, $pg_type, $convert)
    };
}

fn pg_col_to_wit(
    row: &tokio_postgres::Row,
    i: usize,
    col: &tokio_postgres::Column,
) -> Result<PgValue, DbError> {
    let ty = col.type_();
    let Some(pg_type) = postgres_type_to_pg_type(ty) else {
        return Err(DbError::UnsupportedResultType(UnsupportedResultType {
            column_name: Some(col.name().to_string()),
            column_index: i as u32,
            postgres_type_name: ty.name().to_string(),
            postgres_type_oid: ty.oid(),
        }));
    };

    match pg_type {
        PgType::Boolean => pg_col!(row, i, ty, pg_type, bool, PgValue::Boolean),
        PgType::Int2 => pg_col!(row, i, ty, pg_type, i16, PgValue::Int2),
        PgType::Int4 => pg_col!(row, i, ty, pg_type, i32, PgValue::Int4),
        PgType::Int8 => pg_col!(row, i, ty, pg_type, i64, PgValue::Int8),
        PgType::Float4 => pg_col!(row, i, ty, pg_type, f32, PgValue::Float4),
        PgType::Float8 => pg_col!(row, i, ty, pg_type, f64, PgValue::Float8),
        PgType::Text => pg_col!(row, i, ty, pg_type, String, PgValue::Text),
        PgType::Bytea => pg_col!(row, i, ty, pg_type, Vec<u8>, PgValue::Bytea),
        PgType::Timestamptz => {
            pg_col!(row, i, ty, pg_type, chrono::DateTime<chrono::Utc>, |dt| {
                PgValue::Timestamptz(dt.timestamp_micros())
            })
        }
        PgType::Timestamp => {
            pg_col!(row, i, ty, pg_type, chrono::NaiveDateTime, |dt| {
                PgValue::Timestamp(dt.and_utc().timestamp_micros())
            })
        }
        PgType::Date => pg_col!(row, i, ty, pg_type, chrono::NaiveDate, |date| {
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            PgValue::Date((date - epoch).num_days() as i32)
        }),
        PgType::Time => pg_col!(row, i, ty, pg_type, chrono::NaiveTime, |time| {
            let micros = time.num_seconds_from_midnight() as i64 * 1_000_000
                + time.nanosecond() as i64 / 1_000;
            PgValue::Time(micros)
        }),
        PgType::Interval => pg_col!(row, i, ty, pg_type, PgIntervalRaw, |interval| {
            PgValue::Interval(PgInterval {
                months: interval.months,
                days: interval.days,
                microseconds: interval.microseconds,
            })
        }),
        PgType::Numeric => {
            pg_col!(row, i, ty, pg_type, rust_decimal::Decimal, |numeric| {
                PgValue::Numeric(numeric.to_string())
            })
        }
        PgType::Uuid => pg_col!(row, i, ty, pg_type, uuid::Uuid, |uuid| {
            let value = uuid.as_u128();
            PgValue::Uuid(((value >> 64) as u64, value as u64))
        }),
        PgType::Jsonb => {
            pg_col!(row, i, ty, pg_type, serde_json::Value, |json| {
                PgValue::Jsonb(json.to_string())
            })
        }
        PgType::Oid => pg_col!(row, i, ty, pg_type, u32, PgValue::Oid),
        PgType::BoolArray => {
            pg_col!(row, i, ty, pg_type, Vec<Option<bool>>, PgValue::BoolArray)
        }
        PgType::Int2Array => {
            pg_col!(row, i, ty, pg_type, Vec<Option<i16>>, PgValue::Int2Array)
        }
        PgType::Int4Array => {
            pg_col!(row, i, ty, pg_type, Vec<Option<i32>>, PgValue::Int4Array)
        }
        PgType::Int8Array => {
            pg_col!(row, i, ty, pg_type, Vec<Option<i64>>, PgValue::Int8Array)
        }
        PgType::Float4Array => {
            pg_col!(row, i, ty, pg_type, Vec<Option<f32>>, PgValue::Float4Array)
        }
        PgType::Float8Array => {
            pg_col!(row, i, ty, pg_type, Vec<Option<f64>>, PgValue::Float8Array)
        }
        PgType::TextArray => {
            pg_col!(row, i, ty, pg_type, Vec<Option<String>>, PgValue::TextArray)
        }
        PgType::TimestamptzArray => pg_col!(
            row,
            i,
            ty,
            pg_type,
            Vec<Option<chrono::DateTime<chrono::Utc>>>,
            |values| {
                PgValue::TimestamptzArray(
                    values
                        .into_iter()
                        .map(|value| value.map(|dt| dt.timestamp_micros()))
                        .collect(),
                )
            }
        ),
        PgType::TimestampArray => pg_col!(
            row,
            i,
            ty,
            pg_type,
            Vec<Option<chrono::NaiveDateTime>>,
            |values| {
                PgValue::TimestampArray(
                    values
                        .into_iter()
                        .map(|value| value.map(|dt| dt.and_utc().timestamp_micros()))
                        .collect(),
                )
            }
        ),
        PgType::UuidArray => {
            pg_col!(row, i, ty, pg_type, Vec<Option<uuid::Uuid>>, |values| {
                PgValue::UuidArray(
                    values
                        .into_iter()
                        .map(|value| {
                            value.map(|uuid| {
                                let value = uuid.as_u128();
                                ((value >> 64) as u64, value as u64)
                            })
                        })
                        .collect(),
                )
            })
        }
        PgType::JsonbArray => pg_col!(
            row,
            i,
            ty,
            pg_type,
            Vec<Option<serde_json::Value>>,
            |values| {
                PgValue::JsonbArray(
                    values
                        .into_iter()
                        .map(|value| value.map(|json| json.to_string()))
                        .collect(),
                )
            }
        ),
    }
}

fn extract<T, F>(
    row: &tokio_postgres::Row,
    i: usize,
    ty: &Type,
    pg_type: PgType,
    convert: F,
) -> Result<PgValue, DbError>
where
    T: FromSqlOwned,
    F: FnOnce(T) -> PgValue,
{
    let value = if matches!(ty.kind(), Kind::Domain(_)) {
        row.try_get::<_, Option<DomainValue<T>>>(i)
            .map(|value| value.map(|value| value.0))
    } else {
        row.try_get::<_, Option<T>>(i)
    }
    .map_err(|error| {
        let col = &row.columns()[i];
        DbError::Query(format!(
            "failed to decode column {:?} at index {i} as PostgreSQL type {} (OID {}): {error}",
            col.name(),
            ty.name(),
            ty.oid(),
        ))
    })?;
    Ok(value.map_or(PgValue::Null(pg_type), convert))
}

struct DomainValue<T>(T);

impl<'a, T: FromSql<'a>> FromSql<'a> for DomainValue<T> {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        T::from_sql(domain_base_type(ty), raw).map(Self)
    }

    fn accepts(ty: &Type) -> bool {
        matches!(ty.kind(), Kind::Domain(_)) && T::accepts(domain_base_type(ty))
    }
}

fn domain_base_type(mut ty: &Type) -> &Type {
    while let Kind::Domain(inner) = ty.kind() {
        ty = inner;
    }
    ty
}
