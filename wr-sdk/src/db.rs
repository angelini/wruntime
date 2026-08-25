use std::collections::{hash_map::Entry, HashMap, VecDeque};
use std::fmt;
use std::marker::PhantomData;

use crate::bindings::wruntime::db::database::{
    self, Column as RawColumn, DbError as RawDbError, Row as RawRow, RowCursor as RawRowCursor,
    Transaction as RawTransaction,
};
pub use crate::bindings::wruntime::db::database::{PgType, PgValue};
use crate::ServiceError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ColumnRef {
    Name(String),
    Index(usize),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Cardinality {
    AtLeastOne,
    ZeroOrOne,
    ExactlyOne,
}

#[derive(Clone, Debug, PartialEq)]
pub enum DbError {
    Connection(String),
    Query(String),
    UnsupportedResultType {
        column_name: Option<String>,
        column_index: u32,
        postgres_type_name: String,
        postgres_type_oid: u32,
    },
    Encode {
        parameter: Option<usize>,
        pg_type: PgType,
        message: String,
    },
    MissingColumn {
        column: ColumnRef,
    },
    DuplicateColumn {
        name: String,
    },
    TypeMismatch {
        column: ColumnRef,
        expected: PgType,
        actual: PgType,
    },
    UnexpectedNull {
        column: ColumnRef,
        expected: PgType,
        null_type: PgType,
    },
    InvalidValue {
        column: ColumnRef,
        expected: PgType,
        message: String,
    },
    Cardinality {
        expected: Cardinality,
        actual: usize,
    },
    InvalidBatchSize(u32),
    Field {
        field: &'static str,
        source: Box<DbError>,
    },
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
              Self::Connection(message) => write!(f, "database connection: {message}"),
              Self::Query(message) => write!(f, "database query: {message}"),
              Self::UnsupportedResultType {
                  column_name,
                  column_index,
                  postgres_type_name,
                  postgres_type_oid,
              } => write!(
                  f,
                  "unsupported result type {postgres_type_name} (OID {postgres_type_oid}) at column {} ({column_name:?})",
                  column_index
              ),
              Self::Encode {
                  parameter,
                  pg_type,
                  message,
              } => match parameter {
                  Some(index) => write!(
                      f,
                      "failed to encode bind parameter {index} as {pg_type:?}: {message}"
                  ),
                  None => write!(f, "failed to encode {pg_type:?}: {message}"),
              },
              Self::MissingColumn { column } => write!(f, "missing column {column:?}"),
              Self::DuplicateColumn { name } => write!(f, "duplicate column name {name:?}"),
              Self::TypeMismatch {
                  column,
                  expected,
                  actual,
              } => write!(
                  f,
                  "column {column:?}: expected {expected:?}, got {actual:?}"
              ),
              Self::UnexpectedNull {
                  column,
                  expected,
                  null_type,
              } => write!(
                  f,
                  "column {column:?}: unexpected NULL typed {null_type:?}, expected {expected:?}"
              ),
              Self::InvalidValue {
                  column,
                  expected,
                  message,
              } => write!(
                  f,
                  "column {column:?}: invalid {expected:?} value: {message}"
              ),
              Self::Cardinality { expected, actual } => {
                  write!(f, "expected {expected:?} row cardinality, got {actual}")
              }
Self::InvalidBatchSize(value) => write!(
    f,
    "batch size must be between 1 and {MAX_CURSOR_BATCH_ROWS}, got {value}"
),
Self::Field { field, source } => write!(f, "field `{field}`: {source}"),
}
    }
}

impl std::error::Error for DbError {}

impl From<RawDbError> for DbError {
    fn from(error: RawDbError) -> Self {
        match error {
            RawDbError::Connection(message) => Self::Connection(message),
            RawDbError::Query(message) => Self::Query(message),
            RawDbError::UnsupportedResultType(value) => Self::UnsupportedResultType {
                column_name: value.column_name,
                column_index: value.column_index,
                postgres_type_name: value.postgres_type_name,
                postgres_type_oid: value.postgres_type_oid,
            },
        }
    }
}

impl From<DbError> for ServiceError {
    fn from(error: DbError) -> Self {
        let message = error.to_string();
        match error {
            DbError::Encode { .. } | DbError::InvalidBatchSize(_) => {
                ServiceError::bad_request(message)
            }
            DbError::Cardinality { actual: 0, .. } => ServiceError::not_found(message),
            DbError::Cardinality { .. } => ServiceError::conflict(message),
            DbError::Field { field, source } => {
                let mut error = ServiceError::from(*source);
                error.message = format!("database field `{field}`: {}", error.message);
                error
            }
            DbError::Connection(_)
            | DbError::Query(_)
            | DbError::UnsupportedResultType { .. }
            | DbError::MissingColumn { .. }
            | DbError::DuplicateColumn { .. }
            | DbError::TypeMismatch { .. }
            | DbError::UnexpectedNull { .. }
            | DbError::InvalidValue { .. } => ServiceError::internal(message),
        }
    }
}

fn encode_error(pg_type: PgType, message: impl Into<String>) -> DbError {
    DbError::Encode {
        parameter: None,
        pg_type,
        message: message.into(),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Timestamptz(i64);

impl Timestamptz {
    pub fn from_micros(value: i64) -> Result<Self, DbError> {
        chrono::DateTime::from_timestamp_micros(value)
            .map(|_| Self(value))
            .ok_or_else(|| encode_error(PgType::Timestamptz, "microseconds out of range"))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Timestamp(i64);

impl Timestamp {
    pub fn from_micros(value: i64) -> Result<Self, DbError> {
        chrono::DateTime::from_timestamp_micros(value)
            .map(|_| Self(value))
            .ok_or_else(|| encode_error(PgType::Timestamp, "microseconds out of range"))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Date(i32);

impl Date {
    pub fn from_days(value: i32) -> Result<Self, DbError> {
        let ce_days = value
            .checked_add(719_163)
            .ok_or_else(|| encode_error(PgType::Date, "days out of range"))?;
        chrono::NaiveDate::from_num_days_from_ce_opt(ce_days)
            .map(|_| Self(value))
            .ok_or_else(|| encode_error(PgType::Date, "days out of range"))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Time(i64);

impl Time {
    pub fn from_micros(value: i64) -> Result<Self, DbError> {
        if (0..86_400_000_000).contains(&value) {
            Ok(Self(value))
        } else {
            Err(encode_error(
                PgType::Time,
                "microseconds must be within one day",
            ))
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Interval {
    months: i32,
    days: i32,
    microseconds: i64,
}

impl Interval {
    pub fn new(months: i32, days: i32, microseconds: i64) -> Self {
        Self {
            months,
            days,
            microseconds,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Numeric(String);

impl Numeric {
    pub fn parse(value: &str) -> Result<Self, DbError> {
        value
            .parse::<rust_decimal::Decimal>()
            .map_err(|error| encode_error(PgType::Numeric, error.to_string()))?;
        Ok(Self(value.to_owned()))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Uuid(u64, u64);

impl Uuid {
    pub fn from_u128(value: u128) -> Self {
        Self((value >> 64) as u64, value as u64)
    }

    pub fn from_parts(high: u64, low: u64) -> Self {
        Self(high, low)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Oid(u32);

impl Oid {
    pub fn new(value: u32) -> Self {
        Self(value)
    }
}

pub const MAX_CURSOR_BATCH_ROWS: u32 = 1_024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BatchSize(std::num::NonZeroU32);

impl BatchSize {
    /// Creates a cursor batch size in the host-supported range `1..=1024`.
    pub fn new(value: u32) -> Result<Self, DbError> {
        if !(1..=MAX_CURSOR_BATCH_ROWS).contains(&value) {
            return Err(DbError::InvalidBatchSize(value));
        }
        let Some(value) = std::num::NonZeroU32::new(value) else {
            return Err(DbError::InvalidBatchSize(0));
        };
        Ok(Self(value))
    }

    pub fn get(self) -> u32 {
        self.0.get()
    }
}

#[cfg(feature = "serde")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Json<T>(pub T);

/// Exact PostgreSQL parameter encoding. Ambiguous integers and heterogeneous arrays
/// intentionally have no implementation.
///
/// ```compile_fail
/// wr_sdk::db::query("SELECT $1").bind(1_u32);
/// ```
///
/// ```compile_fail
/// use wr_sdk::db::{query, PgType, PgValue};
/// query("SELECT $1").bind(vec![PgValue::Null(PgType::Text)]);
/// ```
pub trait EncodePg {
    const PG_TYPE: PgType;
    fn encode_pg(self) -> Result<PgValue, DbError>;
}

pub trait DecodePg: Sized {
    const PG_TYPE: PgType;
    fn decode_pg(value: PgValue, column: ColumnRef) -> Result<Self, DbError>;
}

pub use wr_sdk_macros::FromRow;

pub trait FromRow: Sized {
    fn from_row(row: Row) -> Result<Self, DbError>;
}

#[doc(hidden)]
pub mod __private {
    use super::{DbError, DecodePg, Row};

    pub struct RowDecoder {
        columns: Vec<Option<super::RawColumn>>,
        names: std::collections::HashMap<String, Option<usize>>,
    }

    impl RowDecoder {
        pub fn new(row: Row) -> Self {
            Self {
                columns: row.columns.into_iter().map(Some).collect(),
                names: row.names,
            }
        }

        pub fn take<T: DecodePg>(&mut self, column: &str) -> Result<T, DbError> {
            let index = match self.names.get(column) {
                Some(Some(index)) => *index,
                Some(None) => {
                    return Err(DbError::DuplicateColumn {
                        name: column.to_owned(),
                    });
                }
                None => {
                    return Err(DbError::MissingColumn {
                        column: super::ColumnRef::Name(column.to_owned()),
                    });
                }
            };

            let source_column =
                self.columns[index]
                    .take()
                    .ok_or_else(|| DbError::DuplicateColumn {
                        name: column.to_owned(),
                    })?;
            T::decode_pg(
                source_column.value,
                super::ColumnRef::Name(column.to_owned()),
            )
        }
    }

    pub trait FromRowDecoder: Sized {
        fn from_row_decoder(row: &mut RowDecoder) -> Result<Self, DbError>;
    }
}

fn actual_pg_type(value: &PgValue) -> PgType {
    match value {
        PgValue::Null(value) => *value,
        PgValue::Boolean(_) => PgType::Boolean,
        PgValue::Int2(_) => PgType::Int2,
        PgValue::Int4(_) => PgType::Int4,
        PgValue::Int8(_) => PgType::Int8,
        PgValue::Float4(_) => PgType::Float4,
        PgValue::Float8(_) => PgType::Float8,
        PgValue::Text(_) => PgType::Text,
        PgValue::Bytea(_) => PgType::Bytea,
        PgValue::Timestamptz(_) => PgType::Timestamptz,
        PgValue::Timestamp(_) => PgType::Timestamp,
        PgValue::Date(_) => PgType::Date,
        PgValue::Time(_) => PgType::Time,
        PgValue::Interval(_) => PgType::Interval,
        PgValue::Numeric(_) => PgType::Numeric,
        PgValue::Uuid(_) => PgType::Uuid,
        PgValue::Jsonb(_) => PgType::Jsonb,
        PgValue::Oid(_) => PgType::Oid,
        PgValue::BoolArray(_) => PgType::BoolArray,
        PgValue::Int2Array(_) => PgType::Int2Array,
        PgValue::Int4Array(_) => PgType::Int4Array,
        PgValue::Int8Array(_) => PgType::Int8Array,
        PgValue::Float4Array(_) => PgType::Float4Array,
        PgValue::Float8Array(_) => PgType::Float8Array,
        PgValue::TextArray(_) => PgType::TextArray,
        PgValue::TimestamptzArray(_) => PgType::TimestamptzArray,
        PgValue::TimestampArray(_) => PgType::TimestampArray,
        PgValue::UuidArray(_) => PgType::UuidArray,
        PgValue::JsonbArray(_) => PgType::JsonbArray,
    }
}

fn mismatch(value: PgValue, column: ColumnRef, expected: PgType) -> DbError {
    match value {
        PgValue::Null(null_type) => DbError::UnexpectedNull {
            column,
            expected,
            null_type,
        },
        value => DbError::TypeMismatch {
            column,
            expected,
            actual: actual_pg_type(&value),
        },
    }
}

macro_rules! scalar_codec {
    ($rust:ty, $pg_type:ident, $variant:ident) => {
        impl EncodePg for $rust {
            const PG_TYPE: PgType = PgType::$pg_type;

            fn encode_pg(self) -> Result<PgValue, DbError> {
                Ok(PgValue::$variant(self))
            }
        }

        impl DecodePg for $rust {
            const PG_TYPE: PgType = PgType::$pg_type;

            fn decode_pg(value: PgValue, column: ColumnRef) -> Result<Self, DbError> {
                match value {
                    PgValue::$variant(value) => Ok(value),
                    value => Err(mismatch(value, column, <Self as DecodePg>::PG_TYPE)),
                }
            }
        }
    };
}

scalar_codec!(bool, Boolean, Boolean);
scalar_codec!(i16, Int2, Int2);
scalar_codec!(i32, Int4, Int4);
scalar_codec!(i64, Int8, Int8);
scalar_codec!(f32, Float4, Float4);
scalar_codec!(f64, Float8, Float8);

impl EncodePg for String {
    const PG_TYPE: PgType = PgType::Text;

    fn encode_pg(self) -> Result<PgValue, DbError> {
        Ok(PgValue::Text(self))
    }
}

impl EncodePg for &str {
    const PG_TYPE: PgType = PgType::Text;

    fn encode_pg(self) -> Result<PgValue, DbError> {
        Ok(PgValue::Text(self.to_owned()))
    }
}

impl DecodePg for String {
    const PG_TYPE: PgType = PgType::Text;

    fn decode_pg(value: PgValue, column: ColumnRef) -> Result<Self, DbError> {
        match value {
            PgValue::Text(value) => Ok(value),
            value => Err(mismatch(value, column, <Self as DecodePg>::PG_TYPE)),
        }
    }
}

impl EncodePg for Vec<u8> {
    const PG_TYPE: PgType = PgType::Bytea;

    fn encode_pg(self) -> Result<PgValue, DbError> {
        Ok(PgValue::Bytea(self))
    }
}

impl EncodePg for &[u8] {
    const PG_TYPE: PgType = PgType::Bytea;

    fn encode_pg(self) -> Result<PgValue, DbError> {
        Ok(PgValue::Bytea(self.to_vec()))
    }
}

impl DecodePg for Vec<u8> {
    const PG_TYPE: PgType = PgType::Bytea;

    fn decode_pg(value: PgValue, column: ColumnRef) -> Result<Self, DbError> {
        match value {
            PgValue::Bytea(value) => Ok(value),
            value => Err(mismatch(value, column, <Self as DecodePg>::PG_TYPE)),
        }
    }
}

macro_rules! wrapper_codec {
    ($rust:ty, $pg_type:ident, $variant:ident, $encode:expr, $decode:expr) => {
        impl EncodePg for $rust {
            const PG_TYPE: PgType = PgType::$pg_type;

            fn encode_pg(self) -> Result<PgValue, DbError> {
                Ok(PgValue::$variant(($encode)(self)))
            }
        }

        impl DecodePg for $rust {
            const PG_TYPE: PgType = PgType::$pg_type;

            fn decode_pg(value: PgValue, column: ColumnRef) -> Result<Self, DbError> {
                match value {
                    PgValue::$variant(value) => {
                        ($decode)(value).map_err(|message| DbError::InvalidValue {
                            column,
                            expected: <Self as DecodePg>::PG_TYPE,
                            message,
                        })
                    }
                    value => Err(mismatch(value, column, <Self as DecodePg>::PG_TYPE)),
                }
            }
        }
    };
}

wrapper_codec!(
    Timestamptz,
    Timestamptz,
    Timestamptz,
    |value: Timestamptz| value.0,
    |value| Timestamptz::from_micros(value).map_err(|error| error.to_string())
);
wrapper_codec!(
    Timestamp,
    Timestamp,
    Timestamp,
    |value: Timestamp| value.0,
    |value| Timestamp::from_micros(value).map_err(|error| error.to_string())
);
wrapper_codec!(Date, Date, Date, |value: Date| value.0, |value| {
    Date::from_days(value).map_err(|error| error.to_string())
});
wrapper_codec!(Time, Time, Time, |value: Time| value.0, |value| {
    Time::from_micros(value).map_err(|error| error.to_string())
});
wrapper_codec!(
    Numeric,
    Numeric,
    Numeric,
    |value: Numeric| value.0,
    |value: String| Numeric::parse(&value).map_err(|error| error.to_string())
);
wrapper_codec!(
    Uuid,
    Uuid,
    Uuid,
    |value: Uuid| (value.0, value.1),
    |value: (u64, u64)| Ok::<_, String>(Uuid::from_parts(value.0, value.1))
);
wrapper_codec!(Oid, Oid, Oid, |value: Oid| value.0, |value: u32| Ok::<
    _,
    String,
>(
    Oid::new(value)
));

impl EncodePg for Interval {
    const PG_TYPE: PgType = PgType::Interval;

    fn encode_pg(self) -> Result<PgValue, DbError> {
        Ok(PgValue::Interval(database::PgInterval {
            months: self.months,
            days: self.days,
            microseconds: self.microseconds,
        }))
    }
}

impl DecodePg for Interval {
    const PG_TYPE: PgType = PgType::Interval;

    fn decode_pg(value: PgValue, column: ColumnRef) -> Result<Self, DbError> {
        match value {
            PgValue::Interval(value) => Ok(Self::new(value.months, value.days, value.microseconds)),
            value => Err(mismatch(value, column, <Self as DecodePg>::PG_TYPE)),
        }
    }
}

#[cfg(feature = "serde")]
impl<T: serde::Serialize> EncodePg for Json<T> {
    const PG_TYPE: PgType = PgType::Jsonb;

    fn encode_pg(self) -> Result<PgValue, DbError> {
        serde_json::to_string(&self.0)
            .map(PgValue::Jsonb)
            .map_err(|error| encode_error(<Self as EncodePg>::PG_TYPE, error.to_string()))
    }
}

#[cfg(feature = "serde")]
impl<T: serde::de::DeserializeOwned> DecodePg for Json<T> {
    const PG_TYPE: PgType = PgType::Jsonb;

    fn decode_pg(value: PgValue, column: ColumnRef) -> Result<Self, DbError> {
        match value {
            PgValue::Jsonb(value) => {
                serde_json::from_str(&value)
                    .map(Self)
                    .map_err(|error| DbError::InvalidValue {
                        column,
                        expected: <Self as DecodePg>::PG_TYPE,
                        message: error.to_string(),
                    })
            }
            value => Err(mismatch(value, column, <Self as DecodePg>::PG_TYPE)),
        }
    }
}

impl<T: EncodePg> EncodePg for Option<T> {
    const PG_TYPE: PgType = T::PG_TYPE;

    fn encode_pg(self) -> Result<PgValue, DbError> {
        match self {
            Some(value) => value.encode_pg(),
            None => Ok(PgValue::Null(T::PG_TYPE)),
        }
    }
}

impl<T: DecodePg> DecodePg for Option<T> {
    const PG_TYPE: PgType = T::PG_TYPE;

    fn decode_pg(value: PgValue, column: ColumnRef) -> Result<Self, DbError> {
        match value {
            PgValue::Null(null_type) if null_type == T::PG_TYPE => Ok(None),
            PgValue::Null(null_type) => Err(DbError::UnexpectedNull {
                column,
                expected: T::PG_TYPE,
                null_type,
            }),
            value => T::decode_pg(value, column).map(Some),
        }
    }
}

trait ArrayCodec: Sized {
    const ARRAY_TYPE: PgType;
    fn encode_array(values: Vec<Option<Self>>) -> Result<PgValue, DbError>;
    fn decode_array(value: PgValue, column: ColumnRef) -> Result<Vec<Option<Self>>, DbError>;
}

macro_rules! identity_array_codec {
    ($rust:ty, $pg_type:ident, $variant:ident) => {
        impl ArrayCodec for $rust {
            const ARRAY_TYPE: PgType = PgType::$pg_type;

            fn encode_array(values: Vec<Option<Self>>) -> Result<PgValue, DbError> {
                Ok(PgValue::$variant(values))
            }

            fn decode_array(
                value: PgValue,
                column: ColumnRef,
            ) -> Result<Vec<Option<Self>>, DbError> {
                match value {
                    PgValue::$variant(values) => Ok(values),
                    value => Err(mismatch(value, column, Self::ARRAY_TYPE)),
                }
            }
        }
    };
}

identity_array_codec!(bool, BoolArray, BoolArray);
identity_array_codec!(i16, Int2Array, Int2Array);
identity_array_codec!(i32, Int4Array, Int4Array);
identity_array_codec!(i64, Int8Array, Int8Array);
identity_array_codec!(f32, Float4Array, Float4Array);
identity_array_codec!(f64, Float8Array, Float8Array);
identity_array_codec!(String, TextArray, TextArray);

macro_rules! mapped_array_codec {
    ($rust:ty, $pg_type:ident, $variant:ident, $encode:expr, $decode:expr) => {
        impl ArrayCodec for $rust {
            const ARRAY_TYPE: PgType = PgType::$pg_type;

            fn encode_array(values: Vec<Option<Self>>) -> Result<PgValue, DbError> {
                Ok(PgValue::$variant(
                    values.into_iter().map(|value| value.map($encode)).collect(),
                ))
            }

            fn decode_array(
                value: PgValue,
                column: ColumnRef,
            ) -> Result<Vec<Option<Self>>, DbError> {
                match value {
                    PgValue::$variant(values) => values
                        .into_iter()
                        .map(|value| value.map($decode).transpose())
                        .collect::<Result<_, String>>()
                        .map_err(|message| DbError::InvalidValue {
                            column,
                            expected: Self::ARRAY_TYPE,
                            message,
                        }),
                    value => Err(mismatch(value, column, Self::ARRAY_TYPE)),
                }
            }
        }
    };
}

mapped_array_codec!(
    Timestamptz,
    TimestamptzArray,
    TimestamptzArray,
    |value: Timestamptz| value.0,
    |value| Timestamptz::from_micros(value).map_err(|error| error.to_string())
);
mapped_array_codec!(
    Timestamp,
    TimestampArray,
    TimestampArray,
    |value: Timestamp| value.0,
    |value| Timestamp::from_micros(value).map_err(|error| error.to_string())
);
mapped_array_codec!(
    Uuid,
    UuidArray,
    UuidArray,
    |value: Uuid| (value.0, value.1),
    |value: (u64, u64)| Ok::<_, String>(Uuid::from_parts(value.0, value.1))
);

macro_rules! array_facade_codec {
    ($rust:ty) => {
        impl EncodePg for Vec<$rust> {
            const PG_TYPE: PgType = <$rust as ArrayCodec>::ARRAY_TYPE;

            fn encode_pg(self) -> Result<PgValue, DbError> {
                <$rust as ArrayCodec>::encode_array(self.into_iter().map(Some).collect())
            }
        }

        impl EncodePg for Vec<Option<$rust>> {
            const PG_TYPE: PgType = <$rust as ArrayCodec>::ARRAY_TYPE;

            fn encode_pg(self) -> Result<PgValue, DbError> {
                <$rust as ArrayCodec>::encode_array(self)
            }
        }

        impl DecodePg for Vec<Option<$rust>> {
            const PG_TYPE: PgType = <$rust as ArrayCodec>::ARRAY_TYPE;

            fn decode_pg(value: PgValue, column: ColumnRef) -> Result<Self, DbError> {
                <$rust as ArrayCodec>::decode_array(value, column)
            }
        }

        impl DecodePg for Vec<$rust> {
            const PG_TYPE: PgType = <$rust as ArrayCodec>::ARRAY_TYPE;

            fn decode_pg(value: PgValue, column: ColumnRef) -> Result<Self, DbError> {
                <$rust as ArrayCodec>::decode_array(value, column.clone())?
                    .into_iter()
                    .enumerate()
                    .map(|(index, value)| {
                        value.ok_or_else(|| DbError::InvalidValue {
                            column: column.clone(),
                            expected: <Self as DecodePg>::PG_TYPE,
                            message: format!("array element {index} is NULL"),
                        })
                    })
                    .collect()
            }
        }
    };
}

array_facade_codec!(bool);
array_facade_codec!(i16);
array_facade_codec!(i32);
array_facade_codec!(i64);
array_facade_codec!(f32);
array_facade_codec!(f64);
array_facade_codec!(String);
array_facade_codec!(Timestamptz);
array_facade_codec!(Timestamp);
array_facade_codec!(Uuid);

#[cfg(feature = "serde")]
impl<T: serde::Serialize> EncodePg for Vec<Json<T>> {
    const PG_TYPE: PgType = PgType::JsonbArray;

    fn encode_pg(self) -> Result<PgValue, DbError> {
        self.into_iter()
            .map(|value| serde_json::to_string(&value.0).map(Some))
            .collect::<Result<Vec<_>, _>>()
            .map(PgValue::JsonbArray)
            .map_err(|error| encode_error(<Self as EncodePg>::PG_TYPE, error.to_string()))
    }
}

#[cfg(feature = "serde")]
impl<T: serde::Serialize> EncodePg for Vec<Option<Json<T>>> {
    const PG_TYPE: PgType = PgType::JsonbArray;

    fn encode_pg(self) -> Result<PgValue, DbError> {
        self.into_iter()
            .map(|value| {
                value
                    .map(|value| serde_json::to_string(&value.0))
                    .transpose()
            })
            .collect::<Result<Vec<_>, _>>()
            .map(PgValue::JsonbArray)
            .map_err(|error| encode_error(<Self as EncodePg>::PG_TYPE, error.to_string()))
    }
}

#[cfg(feature = "serde")]
impl<T: serde::de::DeserializeOwned> DecodePg for Vec<Option<Json<T>>> {
    const PG_TYPE: PgType = PgType::JsonbArray;

    fn decode_pg(value: PgValue, column: ColumnRef) -> Result<Self, DbError> {
        match value {
            PgValue::JsonbArray(values) => values
                .into_iter()
                .map(|value| {
                    value
                        .map(|value| serde_json::from_str(&value).map(Json))
                        .transpose()
                })
                .collect::<Result<_, _>>()
                .map_err(|error| DbError::InvalidValue {
                    column,
                    expected: <Self as DecodePg>::PG_TYPE,
                    message: error.to_string(),
                }),
            value => Err(mismatch(value, column, <Self as DecodePg>::PG_TYPE)),
        }
    }
}

#[cfg(feature = "serde")]
impl<T: serde::de::DeserializeOwned> DecodePg for Vec<Json<T>> {
    const PG_TYPE: PgType = PgType::JsonbArray;

    fn decode_pg(value: PgValue, column: ColumnRef) -> Result<Self, DbError> {
        Vec::<Option<Json<T>>>::decode_pg(value, column.clone())?
            .into_iter()
            .enumerate()
            .map(|(index, value)| {
                value.ok_or_else(|| DbError::InvalidValue {
                    column: column.clone(),
                    expected: <Self as DecodePg>::PG_TYPE,
                    message: format!("array element {index} is NULL"),
                })
            })
            .collect()
    }
}

#[derive(Clone, Debug)]
pub struct Row {
    columns: Vec<RawColumn>,
    names: HashMap<String, Option<usize>>,
}

impl Row {
    fn from_raw(row: RawRow) -> Self {
        let mut names = HashMap::new();
        for (index, column) in row.columns.iter().enumerate() {
            match names.entry(column.name.clone()) {
                Entry::Vacant(entry) => {
                    entry.insert(Some(index));
                }
                Entry::Occupied(mut entry) => {
                    entry.insert(None);
                }
            }
        }
        Self {
            columns: row.columns,
            names,
        }
    }

    pub fn get<T: DecodePg>(&self, name: &str) -> Result<T, DbError> {
        let column = ColumnRef::Name(name.to_owned());
        let index = match self.names.get(name) {
            Some(Some(index)) => *index,
            Some(None) => {
                return Err(DbError::DuplicateColumn {
                    name: name.to_owned(),
                });
            }
            None => return Err(DbError::MissingColumn { column }),
        };
        T::decode_pg(self.columns[index].value.clone(), column)
    }

    pub fn get_at<T: DecodePg>(&self, index: usize) -> Result<T, DbError> {
        let column = ColumnRef::Index(index);
        let value = self
            .columns
            .get(index)
            .ok_or_else(|| DbError::MissingColumn {
                column: column.clone(),
            })?
            .value
            .clone();
        T::decode_pg(value, column)
    }
}

impl FromRow for Row {
    fn from_row(row: Row) -> Result<Self, DbError> {
        Ok(row)
    }
}

type Decoder<T> = fn(Row) -> Result<T, DbError>;

fn decode_row<T: FromRow>(row: Row) -> Result<T, DbError> {
    T::from_row(row)
}

fn decode_scalar<T: DecodePg>(row: Row) -> Result<T, DbError> {
    row.get_at(0)
}

#[derive(Debug)]
enum ParamState {
    Ready(Vec<PgValue>),
    Failed(DbError),
}

impl ParamState {
    fn bind<V: EncodePg>(self, value: V) -> Self {
        match self {
            Self::Ready(mut values) => {
                let index = values.len() + 1;
                match value.encode_pg() {
                    Ok(value) => {
                        values.push(value);
                        Self::Ready(values)
                    }
                    Err(DbError::Encode {
                        pg_type, message, ..
                    }) => Self::Failed(DbError::Encode {
                        parameter: Some(index),
                        pg_type,
                        message,
                    }),
                    Err(error) => Self::Failed(DbError::Encode {
                        parameter: Some(index),
                        pg_type: V::PG_TYPE,
                        message: error.to_string(),
                    }),
                }
            }
            Self::Failed(error) => {
                drop(value);
                Self::Failed(error)
            }
        }
    }
}

trait CursorExecution {
    fn next_batch(&self, max: u32) -> Result<Vec<RawRow>, RawDbError>;
}

impl CursorExecution for RawRowCursor {
    fn next_batch(&self, max: u32) -> Result<Vec<RawRow>, RawDbError> {
        RawRowCursor::next_batch(self, max)
    }
}

trait DbExecution {
    type Cursor: CursorExecution;

    fn query(&self, sql: &str, params: &[PgValue]) -> Result<Vec<RawRow>, RawDbError>;
    fn execute(&self, sql: &str, params: &[PgValue]) -> Result<u64, RawDbError>;
    fn query_stream(&self, sql: &str, params: &[PgValue]) -> Result<Self::Cursor, RawDbError>;
}

struct GlobalExecution;

impl DbExecution for GlobalExecution {
    type Cursor = RawRowCursor;

    fn query(&self, sql: &str, params: &[PgValue]) -> Result<Vec<RawRow>, RawDbError> {
        database::query(sql, params)
    }

    fn execute(&self, sql: &str, params: &[PgValue]) -> Result<u64, RawDbError> {
        database::execute(sql, params)
    }

    fn query_stream(&self, sql: &str, params: &[PgValue]) -> Result<Self::Cursor, RawDbError> {
        database::query_stream(sql, params)
    }
}

struct TransactionExecution<'tx>(&'tx RawTransaction);

impl DbExecution for TransactionExecution<'_> {
    type Cursor = RawRowCursor;

    fn query(&self, sql: &str, params: &[PgValue]) -> Result<Vec<RawRow>, RawDbError> {
        self.0.query(sql, params)
    }

    fn execute(&self, sql: &str, params: &[PgValue]) -> Result<u64, RawDbError> {
        self.0.execute(sql, params)
    }

    fn query_stream(&self, sql: &str, params: &[PgValue]) -> Result<Self::Cursor, RawDbError> {
        self.0.query_stream(sql, params)
    }
}

fn run_query<E: DbExecution>(
    executor: &E,
    sql: &str,
    params: ParamState,
) -> Result<Vec<RawRow>, DbError> {
    match params {
        ParamState::Ready(values) => executor.query(sql, &values).map_err(Into::into),
        ParamState::Failed(error) => Err(error),
    }
}

fn run_execute<E: DbExecution>(
    executor: &E,
    sql: &str,
    params: ParamState,
) -> Result<u64, DbError> {
    match params {
        ParamState::Ready(values) => executor.execute(sql, &values).map_err(Into::into),
        ParamState::Failed(error) => Err(error),
    }
}

fn run_stream<E: DbExecution>(
    executor: &E,
    sql: &str,
    params: ParamState,
) -> Result<E::Cursor, DbError> {
    match params {
        ParamState::Ready(values) => executor.query_stream(sql, &values).map_err(Into::into),
        ParamState::Failed(error) => Err(error),
    }
}

struct QueryState<T> {
    sql: String,
    params: ParamState,
    decode: Decoder<T>,
}

impl<T> QueryState<T> {
    fn new(sql: &str, decode: Decoder<T>) -> Self {
        Self {
            sql: sql.to_owned(),
            params: ParamState::Ready(Vec::new()),
            decode,
        }
    }

    fn bind<V: EncodePg>(mut self, value: V) -> Self {
        self.params = self.params.bind(value);
        self
    }

    fn execute_with<E: DbExecution>(self, executor: &E) -> Result<u64, DbError> {
        run_execute(executor, &self.sql, self.params)
    }

    fn decoded_with<E: DbExecution>(self, executor: &E) -> Result<Vec<T>, DbError> {
        let rows = run_query(executor, &self.sql, self.params)?;
        rows.into_iter()
            .map(|row| (self.decode)(Row::from_raw(row)))
            .collect()
    }

    fn fetch_first_with<E: DbExecution>(self, executor: &E) -> Result<T, DbError> {
        let decode = self.decode;
        let mut rows = run_query(executor, &self.sql, self.params)?.into_iter();
        let row = rows.next().ok_or(DbError::Cardinality {
            expected: Cardinality::AtLeastOne,
            actual: 0,
        })?;
        decode(Row::from_raw(row))
    }

    fn fetch_optional_with<E: DbExecution>(self, executor: &E) -> Result<Option<T>, DbError> {
        let decode = self.decode;
        let rows = run_query(executor, &self.sql, self.params)?;
        match rows.len() {
            0 => Ok(None),
            1 => decode(Row::from_raw(rows.into_iter().next().unwrap())).map(Some),
            actual => Err(DbError::Cardinality {
                expected: Cardinality::ZeroOrOne,
                actual,
            }),
        }
    }

    fn fetch_exactly_one_with<E: DbExecution>(self, executor: &E) -> Result<T, DbError> {
        let decode = self.decode;
        let rows = run_query(executor, &self.sql, self.params)?;
        if rows.len() != 1 {
            return Err(DbError::Cardinality {
                expected: Cardinality::ExactlyOne,
                actual: rows.len(),
            });
        }
        decode(Row::from_raw(rows.into_iter().next().unwrap()))
    }

    fn fetch_all_with<E: DbExecution>(self, executor: &E) -> Result<Vec<T>, DbError> {
        self.decoded_with(executor)
    }

    fn stream_with<E: DbExecution>(self, executor: &E) -> Result<(E::Cursor, Decoder<T>), DbError> {
        let cursor = run_stream(executor, &self.sql, self.params)?;
        Ok((cursor, self.decode))
    }
}

pub struct Query<T> {
    state: QueryState<T>,
}

pub fn query(sql: &str) -> Query<Row> {
    Query {
        state: QueryState::new(sql, decode_row::<Row>),
    }
}

pub fn query_as<T: FromRow>(sql: &str) -> Query<T> {
    Query {
        state: QueryState::new(sql, decode_row::<T>),
    }
}

pub fn query_scalar<T: DecodePg>(sql: &str) -> Query<T> {
    Query {
        state: QueryState::new(sql, decode_scalar::<T>),
    }
}

impl<T> Query<T> {
    pub fn bind<V: EncodePg>(mut self, value: V) -> Self {
        self.state = self.state.bind(value);
        self
    }

    pub fn execute(self) -> Result<u64, DbError> {
        self.state.execute_with(&GlobalExecution)
    }

    pub fn fetch_first(self) -> Result<T, DbError> {
        self.state.fetch_first_with(&GlobalExecution)
    }

    pub fn fetch_optional(self) -> Result<Option<T>, DbError> {
        self.state.fetch_optional_with(&GlobalExecution)
    }

    pub fn fetch_exactly_one(self) -> Result<T, DbError> {
        self.state.fetch_exactly_one_with(&GlobalExecution)
    }

    pub fn fetch_all(self) -> Result<Vec<T>, DbError> {
        self.state.fetch_all_with(&GlobalExecution)
    }

    pub fn stream(self, batch_size: BatchSize) -> Result<RowStream<'static, T>, DbError> {
        let (cursor, decode) = self.state.stream_with(&GlobalExecution)?;
        Ok(RowStream::new(cursor, batch_size, decode))
    }
}

enum TransactionState {
    Active(RawTransaction),
    Completed,
}

pub struct Transaction {
    state: TransactionState,
}

pub fn transaction() -> Result<Transaction, DbError> {
    database::begin_transaction()
        .map(|transaction| Transaction {
            state: TransactionState::Active(transaction),
        })
        .map_err(Into::into)
}

impl Transaction {
    fn inner(&self) -> &RawTransaction {
        match &self.state {
            TransactionState::Active(transaction) => transaction,
            TransactionState::Completed => unreachable!("completed transaction is consumed"),
        }
    }

    pub fn query(&self, sql: &str) -> TransactionQuery<'_, Row> {
        TransactionQuery {
            transaction: self,
            state: QueryState::new(sql, decode_row::<Row>),
        }
    }

    pub fn query_as<T: FromRow>(&self, sql: &str) -> TransactionQuery<'_, T> {
        TransactionQuery {
            transaction: self,
            state: QueryState::new(sql, decode_row::<T>),
        }
    }

    pub fn query_scalar<T: DecodePg>(&self, sql: &str) -> TransactionQuery<'_, T> {
        TransactionQuery {
            transaction: self,
            state: QueryState::new(sql, decode_scalar::<T>),
        }
    }

    pub fn commit(mut self) -> Result<(), DbError> {
        let transaction = match std::mem::replace(&mut self.state, TransactionState::Completed) {
            TransactionState::Active(transaction) => transaction,
            TransactionState::Completed => unreachable!("completed transaction is consumed"),
        };
        transaction.commit().map_err(Into::into)
    }

    pub fn rollback(mut self) -> Result<(), DbError> {
        let transaction = match std::mem::replace(&mut self.state, TransactionState::Completed) {
            TransactionState::Active(transaction) => transaction,
            TransactionState::Completed => unreachable!("completed transaction is consumed"),
        };
        transaction.rollback().map_err(Into::into)
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if let TransactionState::Active(transaction) =
            std::mem::replace(&mut self.state, TransactionState::Completed)
        {
            let _ = transaction.rollback();
        }
    }
}

pub struct TransactionQuery<'tx, T> {
    transaction: &'tx Transaction,
    state: QueryState<T>,
}

impl<'tx, T> TransactionQuery<'tx, T> {
    pub fn bind<V: EncodePg>(mut self, value: V) -> Self {
        self.state = self.state.bind(value);
        self
    }

    pub fn execute(self) -> Result<u64, DbError> {
        self.state
            .execute_with(&TransactionExecution(self.transaction.inner()))
    }

    pub fn fetch_first(self) -> Result<T, DbError> {
        self.state
            .fetch_first_with(&TransactionExecution(self.transaction.inner()))
    }

    pub fn fetch_optional(self) -> Result<Option<T>, DbError> {
        self.state
            .fetch_optional_with(&TransactionExecution(self.transaction.inner()))
    }

    pub fn fetch_exactly_one(self) -> Result<T, DbError> {
        self.state
            .fetch_exactly_one_with(&TransactionExecution(self.transaction.inner()))
    }

    pub fn fetch_all(self) -> Result<Vec<T>, DbError> {
        self.state
            .fetch_all_with(&TransactionExecution(self.transaction.inner()))
    }

    pub fn stream(self, batch_size: BatchSize) -> Result<RowStream<'tx, T>, DbError> {
        let (cursor, decode) = self
            .state
            .stream_with(&TransactionExecution(self.transaction.inner()))?;
        Ok(RowStream::new(cursor, batch_size, decode))
    }
}

pub struct RowStream<'tx, T> {
    cursor: Box<dyn CursorExecution + 'tx>,
    batch_size: BatchSize,
    buffered: VecDeque<RawRow>,
    decode: Decoder<T>,
    terminal: bool,
    transaction: PhantomData<&'tx Transaction>,
}

impl<'tx, T> RowStream<'tx, T> {
    fn new(cursor: impl CursorExecution + 'tx, batch_size: BatchSize, decode: Decoder<T>) -> Self {
        Self {
            cursor: Box::new(cursor),
            batch_size,
            buffered: VecDeque::new(),
            decode,
            terminal: false,
            transaction: PhantomData,
        }
    }
}

impl<T> Iterator for RowStream<'_, T> {
    type Item = Result<T, DbError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.terminal {
            return None;
        }
        loop {
            if let Some(row) = self.buffered.pop_front() {
                let result = (self.decode)(Row::from_raw(row));
                if result.is_err() {
                    self.terminal = true;
                }
                return Some(result);
            }
            match self.cursor.next_batch(self.batch_size.get()) {
                Ok(rows) if rows.is_empty() => {
                    self.terminal = true;
                    return None;
                }
                Ok(rows) => self.buffered.extend(rows),
                Err(error) => {
                    self.terminal = true;
                    return Some(Err(error.into()));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::rc::Rc;

    use super::*;

    fn raw_row(columns: Vec<(&str, PgValue)>) -> RawRow {
        RawRow {
            columns: columns
                .into_iter()
                .map(|(name, value)| RawColumn {
                    name: name.to_owned(),
                    value,
                })
                .collect(),
        }
    }

    fn scalar_row(value: i64) -> RawRow {
        raw_row(vec![("value", PgValue::Int8(value))])
    }

    #[test]
    fn codec_payloads_round_trip_across_conversion_families() {
        struct CodecCase {
            name: &'static str,
            encode: fn() -> PgValue,
            assert_round_trip: fn(PgValue),
        }

        let cases = [
            CodecCase {
                name: "scalar",
                encode: || 17_i32.encode_pg().unwrap(),
                assert_round_trip: |value| {
                    assert!(matches!(&value, PgValue::Int4(17)));
                    assert_eq!(i32::decode_pg(value, ColumnRef::Index(0)).unwrap(), 17);
                },
            },
            CodecCase {
                name: "text",
                encode: || String::from("owned").encode_pg().unwrap(),
                assert_round_trip: |value| {
                    assert!(matches!(&value, PgValue::Text(text) if text == "owned"));
                    assert_eq!(
                        String::decode_pg(value, ColumnRef::Index(0)).unwrap(),
                        "owned"
                    );
                },
            },
            CodecCase {
                name: "bytea",
                encode: || vec![1_u8, 2].encode_pg().unwrap(),
                assert_round_trip: |value| {
                    assert!(matches!(&value, PgValue::Bytea(bytes) if bytes == &[1, 2]));
                    assert_eq!(
                        Vec::<u8>::decode_pg(value, ColumnRef::Index(0)).unwrap(),
                        [1, 2]
                    );
                },
            },
            CodecCase {
                name: "timestamp",
                encode: || Timestamp::from_micros(123).unwrap().encode_pg().unwrap(),
                assert_round_trip: |value| {
                    assert!(matches!(&value, PgValue::Timestamp(123)));
                    assert_eq!(
                        Timestamp::decode_pg(value, ColumnRef::Index(0)).unwrap(),
                        Timestamp::from_micros(123).unwrap()
                    );
                },
            },
            CodecCase {
                name: "date",
                encode: || Date::from_days(12).unwrap().encode_pg().unwrap(),
                assert_round_trip: |value| {
                    assert!(matches!(&value, PgValue::Date(12)));
                    assert_eq!(
                        Date::decode_pg(value, ColumnRef::Index(0)).unwrap(),
                        Date::from_days(12).unwrap()
                    );
                },
            },
            CodecCase {
                name: "time",
                encode: || Time::from_micros(123).unwrap().encode_pg().unwrap(),
                assert_round_trip: |value| {
                    assert!(matches!(&value, PgValue::Time(123)));
                    assert_eq!(
                        Time::decode_pg(value, ColumnRef::Index(0)).unwrap(),
                        Time::from_micros(123).unwrap()
                    );
                },
            },
            CodecCase {
                name: "interval",
                encode: || Interval::new(1, 2, 3).encode_pg().unwrap(),
                assert_round_trip: |value| {
                    assert!(matches!(
                        &value,
                        PgValue::Interval(interval)
                            if interval.months == 1 && interval.days == 2 && interval.microseconds == 3
                    ));
                    assert_eq!(
                        Interval::decode_pg(value, ColumnRef::Index(0)).unwrap(),
                        Interval::new(1, 2, 3)
                    );
                },
            },
            CodecCase {
                name: "numeric",
                encode: || Numeric::parse("1.25").unwrap().encode_pg().unwrap(),
                assert_round_trip: |value| {
                    assert!(matches!(&value, PgValue::Numeric(numeric) if numeric == "1.25"));
                    assert_eq!(
                        Numeric::decode_pg(value, ColumnRef::Index(0)).unwrap(),
                        Numeric::parse("1.25").unwrap()
                    );
                },
            },
            CodecCase {
                name: "uuid",
                encode: || Uuid::from_parts(1, 2).encode_pg().unwrap(),
                assert_round_trip: |value| {
                    assert!(matches!(&value, PgValue::Uuid((1, 2))));
                    assert_eq!(
                        Uuid::decode_pg(value, ColumnRef::Index(0)).unwrap(),
                        Uuid::from_parts(1, 2)
                    );
                },
            },
            CodecCase {
                name: "oid",
                encode: || Oid::new(42).encode_pg().unwrap(),
                assert_round_trip: |value| {
                    assert!(matches!(&value, PgValue::Oid(42)));
                    assert_eq!(
                        Oid::decode_pg(value, ColumnRef::Index(0)).unwrap(),
                        Oid::new(42)
                    );
                },
            },
            CodecCase {
                name: "typed null",
                encode: || Option::<i64>::None.encode_pg().unwrap(),
                assert_round_trip: |value| {
                    assert!(matches!(&value, PgValue::Null(PgType::Int8)));
                    assert_eq!(
                        Option::<i64>::decode_pg(value, ColumnRef::Index(0)).unwrap(),
                        None
                    );
                },
            },
            CodecCase {
                name: "identity array",
                encode: || vec![Some(7_i32), None].encode_pg().unwrap(),
                assert_round_trip: |value| {
                    assert!(matches!(
                        &value,
                        PgValue::Int4Array(values) if values == &vec![Some(7), None]
                    ));
                    assert_eq!(
                        Vec::<Option<i32>>::decode_pg(value, ColumnRef::Index(0)).unwrap(),
                        vec![Some(7), None]
                    );
                },
            },
            CodecCase {
                name: "mapped temporal array",
                encode: || {
                    vec![Some(Timestamptz::from_micros(99).unwrap()), None]
                        .encode_pg()
                        .unwrap()
                },
                assert_round_trip: |value| {
                    assert!(matches!(
                        &value,
                        PgValue::TimestamptzArray(values) if values == &vec![Some(99), None]
                    ));
                    assert_eq!(
                        Vec::<Option<Timestamptz>>::decode_pg(value, ColumnRef::Index(0)).unwrap(),
                        vec![Some(Timestamptz::from_micros(99).unwrap()), None]
                    );
                },
            },
            CodecCase {
                name: "mapped uuid array",
                encode: || {
                    vec![Some(Uuid::from_parts(1, 2)), None]
                        .encode_pg()
                        .unwrap()
                },
                assert_round_trip: |value| {
                    assert!(matches!(
                        &value,
                        PgValue::UuidArray(values) if values == &vec![Some((1, 2)), None]
                    ));
                    assert_eq!(
                        Vec::<Option<Uuid>>::decode_pg(value, ColumnRef::Index(0)).unwrap(),
                        vec![Some(Uuid::from_parts(1, 2)), None]
                    );
                },
            },
        ];

        for case in cases {
            let value = (case.encode)();
            assert_eq!(
                actual_pg_type(&value),
                match case.name {
                    "scalar" => PgType::Int4,
                    "text" => PgType::Text,
                    "bytea" => PgType::Bytea,
                    "timestamp" => PgType::Timestamp,
                    "date" => PgType::Date,
                    "time" => PgType::Time,
                    "interval" => PgType::Interval,
                    "numeric" => PgType::Numeric,
                    "uuid" => PgType::Uuid,
                    "oid" => PgType::Oid,
                    "typed null" => PgType::Int8,
                    "identity array" => PgType::Int4Array,
                    "mapped temporal array" => PgType::TimestamptzArray,
                    "mapped uuid array" => PgType::UuidArray,
                    _ => unreachable!("known codec case"),
                }
            );
            (case.assert_round_trip)(value);
        }

        assert!(matches!(
            i32::decode_pg(PgValue::Text("wrong".into()), ColumnRef::Index(0)),
            Err(DbError::TypeMismatch {
                actual: PgType::Text,
                ..
            })
        ));
        assert!(matches!(
            Option::<i32>::decode_pg(PgValue::Null(PgType::Text), ColumnRef::Index(0)),
            Err(DbError::UnexpectedNull {
                null_type: PgType::Text,
                ..
            })
        ));
        assert!(matches!(
            Date::decode_pg(PgValue::Date(i32::MAX), ColumnRef::Index(0)),
            Err(DbError::InvalidValue {
                expected: PgType::Date,
                ..
            })
        ));
        assert!(matches!(
            Vec::<Option<Timestamptz>>::decode_pg(
                PgValue::TimestamptzArray(vec![Some(i64::MAX)]),
                ColumnRef::Index(0)
            ),
            Err(DbError::InvalidValue {
                expected: PgType::TimestamptzArray,
                ..
            })
        ));
        assert!(matches!(
            Vec::<i32>::decode_pg(PgValue::Int4Array(vec![Some(1), None]), ColumnRef::Index(0)),
            Err(DbError::InvalidValue {
                expected: PgType::Int4Array,
                ..
            })
        ));
    }

    #[test]
    fn field_errors_display_nested_context_without_reclassification() {
        let error = DbError::Field {
            field: "order",
            source: Box::new(DbError::Field {
                field: "seller",
                source: Box::new(DbError::TypeMismatch {
                    column: ColumnRef::Name("seller_id".to_owned()),
                    expected: PgType::Int8,
                    actual: PgType::Text,
                }),
            }),
        };

        assert_eq!(
            error.to_string(),
            "field `order`: field `seller`: column Name(\"seller_id\"): expected PgType::Int8, got PgType::Text"
        );

        let service_error = ServiceError::from(error);
        assert_eq!(service_error.status, 500);
        assert_eq!(
            service_error.message,
            "database field `order`: database field `seller`: column Name(\"seller_id\"): expected PgType::Int8, got PgType::Text"
        );
    }

    #[test]
    fn semantic_validation_is_preserved() {
        assert!(Timestamptz::from_micros(i64::MAX).is_err());
        assert!(Timestamp::from_micros(i64::MAX).is_err());
        assert!(Date::from_days(i32::MAX).is_err());
        assert!(Time::from_micros(-1).is_err());
        assert!(Time::from_micros(86_400_000_000).is_err());
        assert!(Numeric::parse("not-numeric").is_err());
        assert_eq!(Uuid::from_u128(1), Uuid::from_parts(0, 1));
        for value in [0, MAX_CURSOR_BATCH_ROWS + 1, u32::MAX] {
            assert!(matches!(
                BatchSize::new(value),
                Err(DbError::InvalidBatchSize(actual)) if actual == value
            ));
        }
        assert_eq!(BatchSize::new(1).unwrap().get(), 1);
        assert_eq!(
            BatchSize::new(MAX_CURSOR_BATCH_ROWS).unwrap().get(),
            MAX_CURSOR_BATCH_ROWS
        );
    }

    #[cfg(feature = "serde")]
    struct RejectJson;

    #[cfg(feature = "serde")]
    impl serde::Serialize for RejectJson {
        fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            Err(serde::ser::Error::custom("fixture serialization failure"))
        }
    }

    #[cfg(feature = "serde")]
    #[test]
    fn json_is_confined_to_jsonb_and_keeps_serde_context() {
        assert!(matches!(
            Json(RejectJson).encode_pg(),
            Err(DbError::Encode {
                parameter: None,
                pg_type: PgType::Jsonb,
                ..
            })
        ));
        let value = Json(vec![1_i32, 2]).encode_pg().unwrap();
        assert!(matches!(
            &value,
            PgValue::Jsonb(encoded) if encoded == "[1,2]"
        ));
        let decoded =
            Json::<Vec<i32>>::decode_pg(value, ColumnRef::Name("payload".into())).unwrap();
        assert_eq!(decoded.0, vec![1, 2]);
        assert!(matches!(
            vec![Some(Json(vec![1_i32])), None].encode_pg().unwrap(),
            PgValue::JsonbArray(value)
                if value == vec![Some("[1]".to_owned()), None]
        ));
        assert!(matches!(
            Json::<Vec<i32>>::decode_pg(
                PgValue::Jsonb("{".into()),
                ColumnRef::Name("payload".into())
            ),
            Err(DbError::InvalidValue {
                column: ColumnRef::Name(name),
                expected: PgType::Jsonb,
                ..
            }) if name == "payload"
        ));
    }

    #[test]
    fn strict_row_access_reports_context_and_allows_extras() {
        let large = "x".repeat(32_768);
        let row = Row::from_raw(raw_row(vec![
            ("id", PgValue::Int8(7)),
            ("payload", PgValue::Text(large.clone())),
            ("extra", PgValue::Bytea(vec![1; 32_768])),
        ]));
        assert_eq!(row.get::<i64>("id").unwrap(), 7);
        assert_eq!(row.get_at::<String>(1).unwrap(), large);
        assert!(matches!(
            row.get::<i64>("missing"),
            Err(DbError::MissingColumn {
                column: ColumnRef::Name(name)
            }) if name == "missing"
        ));
        assert!(matches!(
            row.get_at::<i64>(9),
            Err(DbError::MissingColumn {
                column: ColumnRef::Index(9)
            })
        ));
        assert!(matches!(
            decode_scalar::<i64>(Row::from_raw(raw_row(Vec::new()))),
            Err(DbError::MissingColumn {
                column: ColumnRef::Index(0)
            })
        ));
        assert!(matches!(
            row.get::<String>("id"),
            Err(DbError::TypeMismatch {
                column: ColumnRef::Name(name),
                expected: PgType::Text,
                actual: PgType::Int8,
            }) if name == "id"
        ));
    }

    #[test]
    fn duplicate_and_typed_null_rules_are_strict() {
        let duplicate = Row::from_raw(raw_row(vec![
            ("value", PgValue::Int8(1)),
            ("value", PgValue::Int8(2)),
        ]));
        assert!(matches!(
            duplicate.get::<i64>("value"),
            Err(DbError::DuplicateColumn { name }) if name == "value"
        ));

        let typed_null = Row::from_raw(raw_row(vec![("value", PgValue::Null(PgType::Int8))]));
        assert_eq!(typed_null.get::<Option<i64>>("value").unwrap(), None);
        assert!(matches!(
            typed_null.get::<i64>("value"),
            Err(DbError::UnexpectedNull {
                expected: PgType::Int8,
                null_type: PgType::Int8,
                ..
            })
        ));
        assert!(matches!(
            typed_null.get::<Option<String>>("value"),
            Err(DbError::UnexpectedNull {
                expected: PgType::Text,
                null_type: PgType::Int8,
                ..
            })
        ));
        assert!(matches!(
            Vec::<i32>::decode_pg(PgValue::Int4Array(vec![Some(1), None]), ColumnRef::Index(0)),
            Err(DbError::InvalidValue { .. })
        ));
    }

    #[test]
    fn derived_from_row_decodes_owned_composite_rows() {
        #[derive(FromRow)]
        struct Identity {
            customer_id: i64,
        }

        #[derive(FromRow)]
        struct State {
            active: bool,
        }

        #[derive(FromRow)]
        struct CompositeRow {
            ordinary: i64,
            #[wr_db(rename = "display_name")]
            name: String,
            optional: Option<bool>,
            body: String,
            #[wr_db(flatten)]
            identity: Identity,
            #[wr_db(flatten)]
            state: State,
        }

        let large_text = "x".repeat(32_768);
        let large_text_pointer = large_text.as_ptr();
        let row = CompositeRow::from_row(Row::from_raw(raw_row(vec![
            ("ordinary", PgValue::Int8(7)),
            ("display_name", PgValue::Text("Ada".to_owned())),
            ("optional", PgValue::Null(PgType::Boolean)),
            ("body", PgValue::Text(large_text)),
            ("customer_id", PgValue::Int8(11)),
            ("active", PgValue::Boolean(true)),
            ("ignored", PgValue::Text("extra".to_owned())),
        ])))
        .unwrap();

        assert_eq!(row.ordinary, 7);
        assert_eq!(row.name, "Ada");
        assert_eq!(row.optional, None);
        assert_eq!(row.body.len(), 32_768);
        assert_eq!(row.body.as_ptr(), large_text_pointer);
        assert_eq!(row.identity.customer_id, 11);
        assert!(row.state.active);

        #[cfg(feature = "serde")]
        {
            #[derive(serde::Deserialize)]
            struct Payload {
                value: String,
            }

            #[derive(FromRow)]
            struct JsonRow {
                payload: Option<Json<Payload>>,
            }

            let json = JsonRow::from_row(Row::from_raw(raw_row(vec![(
                "payload",
                PgValue::Jsonb(r#"{"value":"present"}"#.to_owned()),
            )])))
            .unwrap();
            assert_eq!(json.payload.unwrap().0.value, "present");
        }
    }

    #[test]
    fn derived_from_row_preserves_strict_failure_context() {
        #[allow(dead_code)]
        #[derive(FromRow)]
        struct StrictRow {
            id: i64,
        }

        assert!(matches!(
            StrictRow::from_row(Row::from_raw(raw_row(Vec::new()))),
            Err(DbError::Field { field: "id", source })
                if matches!(
                    source.as_ref(),
                    DbError::MissingColumn {
                        column: ColumnRef::Name(name)
                    } if name == "id"
                )
        ));
        assert!(matches!(
            StrictRow::from_row(Row::from_raw(raw_row(vec![
                ("id", PgValue::Int8(1)),
                ("id", PgValue::Int8(2)),
            ]))),
            Err(DbError::Field { field: "id", source })
                if matches!(
                    source.as_ref(),
                    DbError::DuplicateColumn { name } if name == "id"
                )
        ));
        assert!(matches!(
            StrictRow::from_row(Row::from_raw(raw_row(vec![(
                "id",
                PgValue::Text("wrong type".to_owned()),
            )]))),
            Err(DbError::Field { field: "id", source })
                if matches!(
                    source.as_ref(),
                    DbError::TypeMismatch {
                        column: ColumnRef::Name(name),
                        expected: PgType::Int8,
                        actual: PgType::Text,
                    } if name == "id"
                )
        ));
        assert!(matches!(
            StrictRow::from_row(Row::from_raw(raw_row(vec![(
                "id",
                PgValue::Null(PgType::Int8),
            )]))),
            Err(DbError::Field { field: "id", source })
                if matches!(
                    source.as_ref(),
                    DbError::UnexpectedNull {
                        column: ColumnRef::Name(name),
                        expected: PgType::Int8,
                        null_type: PgType::Int8,
                    } if name == "id"
                )
        ));

        #[cfg(feature = "serde")]
        {
            #[allow(dead_code)]
            #[derive(serde::Deserialize)]
            struct Payload {
                value: String,
            }

            #[allow(dead_code)]
            #[derive(FromRow)]
            struct JsonRow {
                payload: Json<Payload>,
            }

            assert!(matches!(
                JsonRow::from_row(Row::from_raw(raw_row(vec![(
                    "payload",
                    PgValue::Jsonb("{".to_owned()),
                )]))),
                Err(DbError::Field {
                    field: "payload",
                    source
                }) if matches!(
                    source.as_ref(),
                    DbError::InvalidValue {
                        column: ColumnRef::Name(name),
                        expected: PgType::Jsonb,
                        ..
                    } if name == "payload"
                )
            ));
        }
    }

    #[test]
    fn derived_from_row_rejects_shared_decoder_collisions() {
        #[allow(dead_code)]
        #[derive(FromRow)]
        struct Identifier {
            id: i64,
        }

        #[allow(dead_code)]
        #[derive(FromRow)]
        struct ParentAndFlatten {
            id: i64,
            #[wr_db(flatten)]
            nested: Identifier,
        }

        #[allow(dead_code)]
        #[derive(FromRow)]
        struct TwoFlattens {
            #[wr_db(flatten)]
            left: Identifier,
            #[wr_db(flatten)]
            right: Identifier,
        }

        let parent_collision =
            ParentAndFlatten::from_row(Row::from_raw(raw_row(vec![("id", PgValue::Int8(1))])));
        assert!(matches!(
            parent_collision,
            Err(DbError::Field {
                field: "nested",
                source
            }) if matches!(
                source.as_ref(),
                DbError::Field { field: "id", source }
                    if matches!(
                        source.as_ref(),
                        DbError::DuplicateColumn { name } if name == "id"
                    )
            )
        ));

        let flatten_collision =
            TwoFlattens::from_row(Row::from_raw(raw_row(vec![("id", PgValue::Int8(1))])));
        assert!(matches!(
            flatten_collision,
            Err(DbError::Field {
                field: "right",
                source
            }) if matches!(
                source.as_ref(),
                DbError::Field { field: "id", source }
                    if matches!(
                        source.as_ref(),
                        DbError::DuplicateColumn { name } if name == "id"
                    )
            )
        ));
    }

    struct FailEncode(Rc<Cell<u32>>);

    impl EncodePg for FailEncode {
        const PG_TYPE: PgType = PgType::Text;

        fn encode_pg(self) -> Result<PgValue, DbError> {
            self.0.set(self.0.get() + 1);
            Err(encode_error(<Self as EncodePg>::PG_TYPE, "rejected"))
        }
    }

    struct PanicEncode;

    impl EncodePg for PanicEncode {
        const PG_TYPE: PgType = PgType::Text;

        fn encode_pg(self) -> Result<PgValue, DbError> {
            panic!("later encoder must not run")
        }
    }

    #[derive(Default)]
    struct FakeCursor {
        batches: RefCell<VecDeque<Result<Vec<RawRow>, RawDbError>>>,
    }

    impl CursorExecution for FakeCursor {
        fn next_batch(&self, _max: u32) -> Result<Vec<RawRow>, RawDbError> {
            self.batches
                .borrow_mut()
                .pop_front()
                .unwrap_or_else(|| Ok(Vec::new()))
        }
    }

    #[derive(Default)]
    struct CountingExecution {
        query_calls: Cell<u32>,
        execute_calls: Cell<u32>,
        stream_calls: Cell<u32>,
        rows: RefCell<Vec<RawRow>>,
    }

    impl CountingExecution {
        fn with_rows(rows: Vec<RawRow>) -> Self {
            Self {
                rows: RefCell::new(rows),
                ..Self::default()
            }
        }

        fn total_calls(&self) -> u32 {
            self.query_calls.get() + self.execute_calls.get() + self.stream_calls.get()
        }
    }

    impl DbExecution for CountingExecution {
        type Cursor = FakeCursor;

        fn query(&self, _sql: &str, _params: &[PgValue]) -> Result<Vec<RawRow>, RawDbError> {
            self.query_calls.set(self.query_calls.get() + 1);
            Ok(self.rows.borrow_mut().drain(..).collect())
        }

        fn execute(&self, _sql: &str, _params: &[PgValue]) -> Result<u64, RawDbError> {
            self.execute_calls.set(self.execute_calls.get() + 1);
            Ok(1)
        }

        fn query_stream(
            &self,
            _sql: &str,
            _params: &[PgValue],
        ) -> Result<Self::Cursor, RawDbError> {
            self.stream_calls.set(self.stream_calls.get() + 1);
            Ok(FakeCursor::default())
        }
    }

    fn failed_state<T>(decode: Decoder<T>) -> QueryState<T> {
        QueryState::new("ignored", decode).bind(FailEncode(Rc::new(Cell::new(0))))
    }

    #[test]
    fn bind_keeps_first_one_based_failure_and_skips_later_encoders() {
        let calls = Rc::new(Cell::new(0));
        let state = QueryState::new("ignored", decode_row::<Row>)
            .bind(1_i32)
            .bind(FailEncode(calls.clone()))
            .bind(PanicEncode);
        assert_eq!(calls.get(), 1);
        assert!(matches!(
            state.params,
            ParamState::Failed(DbError::Encode {
                parameter: Some(2),
                pg_type: PgType::Text,
                ..
            })
        ));
    }

    fn assert_first_bind_error(error: DbError) {
        assert!(matches!(
            error,
            DbError::Encode {
                parameter: Some(1),
                pg_type: PgType::Text,
                ref message,
            } if message == "rejected"
        ));
    }

    fn assert_every_terminal_preflights(executor: &CountingExecution) {
        assert_first_bind_error(
            failed_state(decode_row::<Row>)
                .execute_with(executor)
                .unwrap_err(),
        );
        assert_first_bind_error(
            failed_state(decode_row::<Row>)
                .fetch_first_with(executor)
                .unwrap_err(),
        );
        assert_first_bind_error(
            failed_state(decode_row::<Row>)
                .fetch_optional_with(executor)
                .unwrap_err(),
        );
        assert_first_bind_error(
            failed_state(decode_row::<Row>)
                .fetch_exactly_one_with(executor)
                .unwrap_err(),
        );
        assert_first_bind_error(
            failed_state(decode_row::<Row>)
                .fetch_all_with(executor)
                .unwrap_err(),
        );
        let stream_error = failed_state(decode_row::<Row>)
            .stream_with(executor)
            .err()
            .unwrap();
        assert_first_bind_error(stream_error);
        assert_eq!(executor.total_calls(), 0);
    }

    #[test]
    fn every_terminal_preflights_before_global_and_transaction_seams() {
        let global_target = CountingExecution::default();
        assert_every_terminal_preflights(&global_target);

        let transaction_target = CountingExecution::default();
        assert_every_terminal_preflights(&transaction_target);
    }

    #[test]
    fn cardinality_terminals_are_distinct() {
        let first = CountingExecution::with_rows(vec![scalar_row(1), scalar_row(2)]);
        assert_eq!(
            QueryState::new("ignored", decode_scalar::<i64>)
                .fetch_first_with(&first)
                .unwrap(),
            1
        );

        let optional = CountingExecution::default();
        assert_eq!(
            QueryState::new("ignored", decode_scalar::<i64>)
                .fetch_optional_with(&optional)
                .unwrap(),
            None
        );

        let too_many = CountingExecution::with_rows(vec![scalar_row(1), scalar_row(2)]);
        assert!(matches!(
            QueryState::new("ignored", decode_scalar::<i64>).fetch_exactly_one_with(&too_many),
            Err(DbError::Cardinality {
                expected: Cardinality::ExactlyOne,
                actual: 2
            })
        ));

        let all = CountingExecution::with_rows(vec![scalar_row(1), scalar_row(2)]);
        assert_eq!(
            QueryState::new("ignored", decode_scalar::<i64>)
                .fetch_all_with(&all)
                .unwrap(),
            vec![1, 2]
        );
    }

    #[test]
    fn stream_batches_in_order_and_terminates_after_one_error() {
        let cursor = FakeCursor {
            batches: RefCell::new(VecDeque::from([
                Ok(vec![scalar_row(1), scalar_row(2)]),
                Ok(vec![scalar_row(3)]),
                Ok(Vec::new()),
            ])),
        };
        let mut stream = RowStream::new(cursor, BatchSize::new(2).unwrap(), decode_scalar::<i64>);
        assert_eq!(stream.next().unwrap().unwrap(), 1);
        assert_eq!(stream.next().unwrap().unwrap(), 2);
        assert_eq!(stream.next().unwrap().unwrap(), 3);
        assert!(stream.next().is_none());

        let cursor = FakeCursor {
            batches: RefCell::new(VecDeque::from([
                Err(RawDbError::Query("host failed".into())),
                Ok(vec![scalar_row(4)]),
            ])),
        };
        let mut stream = RowStream::new(cursor, BatchSize::new(1).unwrap(), decode_scalar::<i64>);
        assert!(matches!(stream.next(), Some(Err(DbError::Query(_)))));
        assert!(stream.next().is_none());

        let cursor = FakeCursor {
            batches: RefCell::new(VecDeque::from([Ok(vec![raw_row(vec![(
                "value",
                PgValue::Text("wrong".into()),
            )])])])),
        };
        let mut stream = RowStream::new(cursor, BatchSize::new(1).unwrap(), decode_scalar::<i64>);
        assert!(matches!(
            stream.next(),
            Some(Err(DbError::TypeMismatch { .. }))
        ));
        assert!(stream.next().is_none());
    }

    struct DropCursor(Rc<Cell<u32>>);

    impl CursorExecution for DropCursor {
        fn next_batch(&self, _max: u32) -> Result<Vec<RawRow>, RawDbError> {
            Ok(vec![scalar_row(1), scalar_row(2)])
        }
    }

    impl Drop for DropCursor {
        fn drop(&mut self) {
            self.0.set(self.0.get() + 1);
        }
    }

    #[test]
    fn early_stream_drop_releases_cursor_immediately() {
        let drops = Rc::new(Cell::new(0));
        {
            let mut stream = RowStream::new(
                DropCursor(drops.clone()),
                BatchSize::new(1).unwrap(),
                decode_scalar::<i64>,
            );
            assert_eq!(stream.next().unwrap().unwrap(), 1);
        }
        assert_eq!(drops.get(), 1);
    }
}
