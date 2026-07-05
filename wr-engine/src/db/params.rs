use super::wruntime::db::database::{DbError, PgValue};

// ── PgParam ──────────────────────────────────────────────────────────────────

/// Owned, typed Postgres parameter converted from the WIT `pg-value` variant.
///
/// Implements `ToSql` so a `Vec<PgParam>` can be passed directly to
/// `tokio_postgres` without boxing each concrete Rust type individually.
#[derive(Debug)]
pub(crate) enum PgParam {
    Null,
    Boolean(bool),
    Int2(i16),
    Int4(i32),
    Int8(i64),
    Float4(f32),
    Float8(f64),
    Text(String),
    Bytea(Vec<u8>),
    Timestamptz(chrono::DateTime<chrono::Utc>),
    Timestamp(chrono::NaiveDateTime),
    Date(chrono::NaiveDate),
    Time(chrono::NaiveTime),
    Interval(PgIntervalRaw),
    Numeric(rust_decimal::Decimal),
    Uuid(uuid::Uuid),
    Jsonb(serde_json::Value),
    Oid(u32),
    BoolArray(Vec<Option<bool>>),
    Int2Array(Vec<Option<i16>>),
    Int4Array(Vec<Option<i32>>),
    Int8Array(Vec<Option<i64>>),
    Float4Array(Vec<Option<f32>>),
    Float8Array(Vec<Option<f64>>),
    TextArray(Vec<Option<String>>),
    TimestamptzArray(Vec<Option<chrono::DateTime<chrono::Utc>>>),
    TimestampArray(Vec<Option<chrono::NaiveDateTime>>),
    UuidArray(Vec<Option<uuid::Uuid>>),
    JsonbArray(Vec<Option<serde_json::Value>>),
}

/// Raw Postgres INTERVAL: 8-byte microseconds + 4-byte days + 4-byte months
/// (big-endian on the wire).  Implements `ToSql`/`FromSql` directly because
/// `tokio-postgres` has no built-in mapping for INTERVAL.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PgIntervalRaw {
    pub(crate) microseconds: i64,
    pub(crate) days: i32,
    pub(crate) months: i32,
}

impl tokio_postgres::types::FromSql<'_> for PgIntervalRaw {
    fn from_sql(
        _ty: &tokio_postgres::types::Type,
        raw: &[u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        if raw.len() != 16 {
            return Err("invalid INTERVAL length".into());
        }
        let microseconds = i64::from_be_bytes(raw[0..8].try_into().unwrap());
        let days = i32::from_be_bytes(raw[8..12].try_into().unwrap());
        let months = i32::from_be_bytes(raw[12..16].try_into().unwrap());
        Ok(PgIntervalRaw {
            microseconds,
            days,
            months,
        })
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        *ty == tokio_postgres::types::Type::INTERVAL
    }
}

impl tokio_postgres::types::ToSql for PgIntervalRaw {
    fn to_sql(
        &self,
        _ty: &tokio_postgres::types::Type,
        buf: &mut bytes::BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        use bytes::BufMut;
        buf.put_i64(self.microseconds);
        buf.put_i32(self.days);
        buf.put_i32(self.months);
        Ok(tokio_postgres::types::IsNull::No)
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        *ty == tokio_postgres::types::Type::INTERVAL
    }

    tokio_postgres::types::to_sql_checked!();
}

// ── PgValue → PgParam conversion helpers ────────────────────────────────────

fn pg_epoch() -> chrono::NaiveDate {
    chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()
}

fn micros_to_timestamptz(micros: i64) -> Result<chrono::DateTime<chrono::Utc>, DbError> {
    chrono::DateTime::from_timestamp_micros(micros).ok_or_else(|| {
        DbError::Query(format!(
            "timestamptz out of range: {micros} microseconds since Unix epoch"
        ))
    })
}

fn micros_to_timestamp(micros: i64) -> Result<chrono::NaiveDateTime, DbError> {
    Ok(micros_to_timestamptz(micros)?.naive_utc())
}

fn days_to_date(days: i32) -> Result<chrono::NaiveDate, DbError> {
    pg_epoch()
        .checked_add_signed(chrono::Duration::days(days as i64))
        .ok_or_else(|| DbError::Query(format!("date out of range: {days} days since 1970-01-01")))
}

fn micros_to_time(micros: i64) -> Result<chrono::NaiveTime, DbError> {
    if !(0..86_400_000_000).contains(&micros) {
        return Err(DbError::Query(format!(
            "time out of range: {micros} microseconds since midnight"
        )));
    }
    let secs = (micros / 1_000_000) as u32;
    let nano = ((micros % 1_000_000) * 1_000) as u32;
    chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, nano).ok_or_else(|| {
        DbError::Query(format!(
            "time out of range: {micros} microseconds since midnight"
        ))
    })
}

fn uuid_from_hilo((hi, lo): (u64, u64)) -> uuid::Uuid {
    uuid::Uuid::from_u128((hi as u128) << 64 | lo as u128)
}

fn parse_jsonb(s: String) -> Result<serde_json::Value, DbError> {
    serde_json::from_str(&s)
        .map_err(|e| DbError::Query(format!("invalid JSON for jsonb parameter: {e}")))
}

/// Generates pass-through arms where both variants have the same name and
/// the inner value is forwarded without transformation.
macro_rules! passthrough_variants {
    ($v:expr, [ $($variant:ident),+ $(,)? ]) => {
        match $v {
            PgValue::Null => Ok(PgParam::Null),
            $(PgValue::$variant(inner) => Ok(PgParam::$variant(inner)),)+
            other => convert_complex(other),
        }
    };
}

/// Handles variants that require type conversion (dates, times, json, etc.).
fn convert_complex(v: PgValue) -> Result<PgParam, DbError> {
    Ok(match v {
        PgValue::Timestamptz(micros) => PgParam::Timestamptz(micros_to_timestamptz(micros)?),
        PgValue::Timestamp(micros) => PgParam::Timestamp(micros_to_timestamp(micros)?),
        PgValue::Date(days) => PgParam::Date(days_to_date(days)?),
        PgValue::Time(micros) => PgParam::Time(micros_to_time(micros)?),
        PgValue::Numeric(s) => PgParam::Numeric(
            s.parse()
                .map_err(|e| DbError::Query(format!("invalid numeric parameter {s:?}: {e}")))?,
        ),
        PgValue::Uuid(pair) => PgParam::Uuid(uuid_from_hilo(pair)),
        PgValue::Interval(iv) => PgParam::Interval(PgIntervalRaw {
            microseconds: iv.microseconds,
            days: iv.days,
            months: iv.months,
        }),
        PgValue::Jsonb(s) => PgParam::Jsonb(parse_jsonb(s)?),
        PgValue::TimestamptzArray(a) => PgParam::TimestamptzArray(
            a.into_iter()
                .map(|o| o.map(micros_to_timestamptz).transpose())
                .collect::<Result<Vec<_>, _>>()?,
        ),
        PgValue::TimestampArray(a) => PgParam::TimestampArray(
            a.into_iter()
                .map(|o| o.map(micros_to_timestamp).transpose())
                .collect::<Result<Vec<_>, _>>()?,
        ),
        PgValue::UuidArray(a) => {
            PgParam::UuidArray(a.into_iter().map(|o| o.map(uuid_from_hilo)).collect())
        }
        PgValue::JsonbArray(a) => PgParam::JsonbArray(
            a.into_iter()
                .map(|o| o.map(parse_jsonb).transpose())
                .collect::<Result<Vec<_>, _>>()?,
        ),
        other => {
            return Err(DbError::Query(format!(
                "unhandled pg-value variant in parameter conversion: {other:?}"
            )));
        }
    })
}

fn prepare_param(v: PgValue) -> Result<PgParam, DbError> {
    passthrough_variants!(
        v,
        [
            Boolean,
            Int2,
            Int4,
            Int8,
            Float4,
            Float8,
            Text,
            Bytea,
            Oid,
            BoolArray,
            Int2Array,
            Int4Array,
            Int8Array,
            Float4Array,
            Float8Array,
            TextArray,
        ]
    )
}

pub(crate) fn prepare_params(params: Vec<PgValue>) -> Result<Vec<PgParam>, DbError> {
    params.into_iter().map(prepare_param).collect()
}

/// Generates the `to_sql` match: every non-Null variant delegates to its
/// inner value's `ToSql` implementation.
macro_rules! delegate_to_sql {
    ($self:expr, $ty:expr, $buf:expr, [ $($variant:ident),+ $(,)? ]) => {
        match $self {
            PgParam::Null => Ok(tokio_postgres::types::IsNull::Yes),
            $(PgParam::$variant(v) => v.to_sql($ty, $buf),)+
        }
    };
}

impl tokio_postgres::types::ToSql for PgParam {
    fn to_sql(
        &self,
        ty: &tokio_postgres::types::Type,
        buf: &mut bytes::BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        delegate_to_sql!(
            self,
            ty,
            buf,
            [
                Boolean,
                Int2,
                Int4,
                Int8,
                Float4,
                Float8,
                Text,
                Bytea,
                Timestamptz,
                Date,
                Time,
                Numeric,
                Uuid,
                Timestamp,
                Interval,
                Jsonb,
                Oid,
                BoolArray,
                Int2Array,
                Int4Array,
                Int8Array,
                Float4Array,
                Float8Array,
                TextArray,
                TimestamptzArray,
                TimestampArray,
                UuidArray,
                JsonbArray,
            ]
        )
    }

    /// Always returns `true`; each variant delegates to its inner type's
    /// `to_sql`, which handles type compatibility at serialisation time.
    fn accepts(_: &tokio_postgres::types::Type) -> bool {
        true
    }

    fn to_sql_checked(
        &self,
        ty: &tokio_postgres::types::Type,
        out: &mut bytes::BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        self.to_sql(ty, out)
    }
}

#[cfg(test)]
mod tests {
    use super::prepare_param;
    use crate::db::wruntime::db::database::{DbError, PgValue};

    fn assert_rejected(v: PgValue) {
        match prepare_param(v.clone()) {
            Err(DbError::Query(_)) => {}
            other => panic!("expected DbError::Query for {v:?}, got {other:?}"),
        }
    }

    #[test]
    fn rejects_lossy_inputs_with_query_error() {
        // (bad JSON, bad numeric, out-of-range timestamptz/timestamp/time,
        //  and the array variants that previously coerced silently)
        let cases = vec![
            PgValue::Jsonb("{not valid json".into()),
            PgValue::Numeric("not-a-number".into()),
            PgValue::Timestamptz(i64::MAX),
            PgValue::Timestamp(i64::MAX),
            PgValue::Time(86_400_000_000), // 86400s == exactly one past the last valid second
            // Would wrap `secs` from 4_294_967_296 to 0 under the old `as u32` cast,
            // silently accepting midnight instead of rejecting the out-of-range input.
            PgValue::Time(4_294_967_296_000_000),
            PgValue::Date(i32::MAX),
            PgValue::JsonbArray(vec![Some("{bad".into())]),
            PgValue::TimestamptzArray(vec![Some(i64::MAX)]),
            PgValue::TimestampArray(vec![Some(i64::MAX)]),
        ];
        for c in cases {
            assert_rejected(c);
        }
    }

    #[test]
    fn accepts_valid_inputs() {
        for v in [
            PgValue::Jsonb(r#"{"ok":true}"#.into()),
            PgValue::Numeric("3.14".into()),
            PgValue::Timestamptz(0),
            PgValue::JsonbArray(vec![Some("1".into()), None]),
            PgValue::TimestamptzArray(vec![Some(0), None]),
        ] {
            assert!(prepare_param(v.clone()).is_ok(), "should accept {v:?}");
        }
    }
}
