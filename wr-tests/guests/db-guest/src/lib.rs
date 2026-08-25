#[allow(dead_code)]
mod proto {
    include!(concat!(env!("OUT_DIR"), "/test.rs"));
}

#[allow(dead_code, unused_imports)]
mod bindings {
    wit_bindgen::generate!({
        path: "wit",
        world: "db-guest",
        generate_all,
    });
}

use wr_sdk::bindings::wruntime::db::database;
use wr_sdk::prelude::*;

struct Component;
wr_sdk::export!(Component with_types_in wr_sdk::bindings);

impl wr_sdk::ServiceGuest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        proto::db_test_service_handle(&Component, request, response_out);
    }
}

fn parse_pg_type(value: &str) -> Result<PgType, ServiceError> {
    match value {
        "boolean" => Ok(PgType::Boolean),
        "int2" => Ok(PgType::Int2),
        "int4" => Ok(PgType::Int4),
        "int8" => Ok(PgType::Int8),
        "float4" => Ok(PgType::Float4),
        "float8" => Ok(PgType::Float8),
        "text" => Ok(PgType::Text),
        "bytea" => Ok(PgType::Bytea),
        "timestamptz" => Ok(PgType::Timestamptz),
        "timestamp" => Ok(PgType::Timestamp),
        "date" => Ok(PgType::Date),
        "time" => Ok(PgType::Time),
        "interval" => Ok(PgType::Interval),
        "numeric" => Ok(PgType::Numeric),
        "uuid" => Ok(PgType::Uuid),
        "jsonb" => Ok(PgType::Jsonb),
        "oid" => Ok(PgType::Oid),
        "bool-array" => Ok(PgType::BoolArray),
        "int2-array" => Ok(PgType::Int2Array),
        "int4-array" => Ok(PgType::Int4Array),
        "int8-array" => Ok(PgType::Int8Array),
        "float4-array" => Ok(PgType::Float4Array),
        "float8-array" => Ok(PgType::Float8Array),
        "text-array" => Ok(PgType::TextArray),
        "timestamptz-array" => Ok(PgType::TimestamptzArray),
        "timestamp-array" => Ok(PgType::TimestampArray),
        "uuid-array" => Ok(PgType::UuidArray),
        "jsonb-array" => Ok(PgType::JsonbArray),
        other => Err(ServiceError::bad_request(format!(
            "unsupported PostgreSQL null type: {other}"
        ))),
    }
}

fn pg_type_name(value: PgType) -> &'static str {
    match value {
        PgType::Boolean => "boolean",
        PgType::Int2 => "int2",
        PgType::Int4 => "int4",
        PgType::Int8 => "int8",
        PgType::Float4 => "float4",
        PgType::Float8 => "float8",
        PgType::Text => "text",
        PgType::Bytea => "bytea",
        PgType::Timestamptz => "timestamptz",
        PgType::Timestamp => "timestamp",
        PgType::Date => "date",
        PgType::Time => "time",
        PgType::Interval => "interval",
        PgType::Numeric => "numeric",
        PgType::Uuid => "uuid",
        PgType::Jsonb => "jsonb",
        PgType::Oid => "oid",
        PgType::BoolArray => "bool-array",
        PgType::Int2Array => "int2-array",
        PgType::Int4Array => "int4-array",
        PgType::Int8Array => "int8-array",
        PgType::Float4Array => "float4-array",
        PgType::Float8Array => "float8-array",
        PgType::TextArray => "text-array",
        PgType::TimestamptzArray => "timestamptz-array",
        PgType::TimestampArray => "timestamp-array",
        PgType::UuidArray => "uuid-array",
        PgType::JsonbArray => "jsonb-array",
    }
}

fn parse_params(params: &[proto::DbParam]) -> Result<Vec<PgValue>, ServiceError> {
    params
        .iter()
        .map(|param| match param.value.as_ref() {
            Some(proto::db_param::Value::NullType(pg_type)) => {
                Ok(PgValue::Null(parse_pg_type(pg_type)?))
            }
            None => Err(ServiceError::bad_request("DB parameter value is required")),
            Some(proto::db_param::Value::Text(value)) => Ok(PgValue::Text(value.clone())),
            Some(proto::db_param::Value::Int4(value)) => Ok(PgValue::Int4(*value)),
            Some(proto::db_param::Value::Int8(value)) => Ok(PgValue::Int8(*value)),
            Some(proto::db_param::Value::Boolean(value)) => Ok(PgValue::Boolean(*value)),
            Some(proto::db_param::Value::Float8(value)) => Ok(PgValue::Float8(*value)),
            Some(proto::db_param::Value::Numeric(value)) => Ok(PgValue::Numeric(value.clone())),
        })
        .collect()
}

fn column_to_proto(name: &str, col: &database::Column) -> proto::DbColumn {
    use proto::db_column::Value;

    let (type_name, value) = match &col.value {
        PgValue::Null(pg_type) => ("null", Value::NullType(pg_type_name(*pg_type).to_string())),
        PgValue::Boolean(value) => ("boolean", Value::Boolean(*value)),
        PgValue::Int2(value) => ("int2", Value::Integer(i64::from(*value))),
        PgValue::Int4(value) => ("int4", Value::Integer(i64::from(*value))),
        PgValue::Int8(value) => ("int8", Value::Integer(*value)),
        PgValue::Float4(value) => ("float4", Value::Float(f64::from(*value))),
        PgValue::Float8(value) => ("float8", Value::Float(*value)),
        PgValue::Text(value) => ("text", Value::Text(value.clone())),
        PgValue::Bytea(value) => ("bytea", Value::Bytea(value.clone())),
        PgValue::Numeric(value) => ("numeric", Value::Text(value.clone())),
        PgValue::Jsonb(value) => ("jsonb", Value::Text(value.clone())),
        PgValue::Timestamptz(value) => ("timestamptz", Value::Integer(*value)),
        PgValue::Timestamp(value) => ("timestamp", Value::Integer(*value)),
        PgValue::Date(value) => ("date", Value::Integer(i64::from(*value))),
        PgValue::Time(value) => ("time", Value::Integer(*value)),
        PgValue::Oid(value) => ("oid", Value::Integer(i64::from(*value))),
        other => ("other", Value::Display(format!("{other:?}"))),
    };
    proto::DbColumn {
        name: name.to_string(),
        type_name: type_name.to_string(),
        value: Some(value),
    }
}

fn db_error_message(error: database::DbError) -> String {
    match error {
        database::DbError::Connection(message) | database::DbError::Query(message) => message,
        database::DbError::UnsupportedResultType(details) => format!(
            "unsupported result type at column {:?} index {}: PostgreSQL type {} (OID {})",
            details.column_name,
            details.column_index,
            details.postgres_type_name,
            details.postgres_type_oid,
        ),
    }
}

fn error_response(error: database::DbError) -> proto::ErrorResponse {
    match error {
        database::DbError::Connection(message) => proto::ErrorResponse {
            error_kind: "connection".into(),
            error_message: message,
            ..Default::default()
        },
        database::DbError::Query(message) => proto::ErrorResponse {
            error_kind: "query".into(),
            error_message: message,
            ..Default::default()
        },
        database::DbError::UnsupportedResultType(details) => proto::ErrorResponse {
            error_kind: "unsupported-result-type".into(),
            error_message: format!(
                "unsupported result type at column {:?} index {}: PostgreSQL type {} (OID {})",
                details.column_name,
                details.column_index,
                details.postgres_type_name,
                details.postgres_type_oid,
            ),
            column_name: details.column_name.unwrap_or_default(),
            column_index: details.column_index,
            postgres_type_name: details.postgres_type_name,
            postgres_type_oid: details.postgres_type_oid,
        },
    }
}

fn raw_db_error(error: database::DbError) -> ServiceError {
    DbError::from(error).into()
}

struct BuilderRecord {
    label: String,
}

impl FromRow for BuilderRecord {
    fn from_row(row: Row) -> Result<Self, DbError> {
        Ok(Self {
            label: row.get("label")?,
        })
    }
}

struct RejectBind;

impl EncodePg for RejectBind {
    const PG_TYPE: PgType = PgType::Int4;

    fn encode_pg(self) -> Result<PgValue, DbError> {
        Err(DbError::Encode {
            parameter: None,
            pg_type: Self::PG_TYPE,
            message: "fixture rejected value".into(),
        })
    }
}

impl proto::DbTestService for Component {
    fn execute(&self, req: proto::ExecuteRequest) -> Result<proto::ExecuteResponse, ServiceError> {
        let params = parse_params(&req.params)?;
        let affected = database::execute(&req.sql, &params).map_err(raw_db_error)?;
        Ok(proto::ExecuteResponse { affected })
    }

    fn query(&self, req: proto::QueryRequest) -> Result<proto::QueryResponse, ServiceError> {
        let params = parse_params(&req.params)?;
        match database::query(&req.sql, &params) {
            Ok(rows) => {
                let proto_rows = rows
                    .iter()
                    .map(|row| {
                        let cols = row
                            .columns
                            .iter()
                            .map(|c| column_to_proto(&c.name, c))
                            .collect();
                        proto::QueryRow { columns: cols }
                    })
                    .collect();
                Ok(proto::QueryResponse { rows: proto_rows })
            }
            Err(error) => Err(raw_db_error(error)),
        }
    }

    fn builder_api(
        &self,
        req: proto::BuilderApiRequest,
    ) -> Result<proto::BuilderApiResponse, ServiceError> {
        let table = if req.table_name.is_empty() {
            "builder_api_test"
        } else {
            &req.table_name
        };

        query(&format!("CREATE TABLE IF NOT EXISTS {table} (id integer)")).execute()?;
        query(&format!("DELETE FROM {table}")).execute()?;

        let raw_value: i32 = query("SELECT $1::integer AS value")
            .bind(7_i32)
            .fetch_exactly_one()?
            .get("value")?;
        let typed_label = query_as::<BuilderRecord>("SELECT $1::text AS label")
            .bind("typed")
            .fetch_exactly_one()?
            .label;
        let scalar_value = query_scalar::<i64>("SELECT ($1::bigint * 10) + $2::bigint")
            .bind(4_i64)
            .bind(2_i64)
            .fetch_exactly_one()?;
        let optional_missing = query_scalar::<i64>("SELECT 1::bigint WHERE false")
            .fetch_optional()?
            .is_none();
        let first_value =
            query_scalar::<i64>("SELECT generate_series(11::bigint, 12::bigint)").fetch_first()?;
        let all_count = query_scalar::<i64>("SELECT generate_series(1::bigint, 3::bigint)")
            .fetch_all()?
            .len() as u32;
        let exactly_one_actual =
            match query_scalar::<i64>("SELECT generate_series(1::bigint, 2::bigint)")
                .fetch_exactly_one()
            {
                Err(DbError::Cardinality {
                    expected: Cardinality::ExactlyOne,
                    actual,
                }) => actual as u32,
                Err(error) => return Err(error.into()),
                Ok(_) => {
                    return Err(ServiceError::internal(
                        "exactly-one builder unexpectedly succeeded",
                    ));
                }
            };
        let execution_failed = query("SELECT * FROM builder_api_missing_table")
            .fetch_all()
            .is_err();

        let bind_error_parameter = match query(&format!("INSERT INTO {table} (id) VALUES ($1)"))
            .bind(RejectBind)
            .execute()
        {
            Err(DbError::Encode {
                parameter: Some(parameter),
                ..
            }) => parameter as u64,
            Err(error) => return Err(error.into()),
            Ok(_) => {
                return Err(ServiceError::internal(
                    "rejected bind unexpectedly executed",
                ));
            }
        };
        let side_effect_count =
            query_scalar::<i64>(&format!("SELECT count(*) FROM {table}")).fetch_exactly_one()?;

        let tx = transaction()?;
        tx.query(&format!("INSERT INTO {table} (id) VALUES ($1)"))
            .bind(2_i32)
            .execute()?;
        tx.rollback()?;
        if query_scalar::<i64>(&format!("SELECT count(*) FROM {table}")).fetch_exactly_one()? != 0 {
            return Err(ServiceError::internal(
                "SDK explicit transaction rollback left a visible write",
            ));
        }

        {
            let tx = transaction()?;
            tx.query(&format!("INSERT INTO {table} (id) VALUES ($1)"))
                .bind(3_i32)
                .execute()?;
        }
        if query_scalar::<i64>(&format!("SELECT count(*) FROM {table}")).fetch_exactly_one()? != 0 {
            return Err(ServiceError::internal(
                "SDK transaction drop left a visible write",
            ));
        }

        let tx = transaction()?;
        match tx
            .query(&format!("INSERT INTO {table} (id) VALUES ($1)"))
            .bind(RejectBind)
            .execute()
        {
            Err(DbError::Encode {
                parameter: Some(1), ..
            }) => {}
            Err(error) => return Err(error.into()),
            Ok(_) => {
                return Err(ServiceError::internal(
                    "transaction rejected bind unexpectedly executed",
                ));
            }
        }
        tx.query(&format!("INSERT INTO {table} (id) VALUES ($1)"))
            .bind(1_i32)
            .execute()?;
        let mut stream = tx
            .query_scalar::<i64>("SELECT generate_series(1::bigint, 5::bigint)")
            .stream(BatchSize::new(2)?)?;
        let streamed_before_drop = u32::from(stream.next().transpose()?.is_some());
        drop(stream);
        tx.commit()?;

        let transaction_count =
            query_scalar::<i64>(&format!("SELECT count(*) FROM {table}")).fetch_exactly_one()?;

        Ok(proto::BuilderApiResponse {
            raw_value,
            typed_label,
            scalar_value,
            optional_missing,
            first_value,
            all_count,
            exactly_one_actual,
            execution_failed,
            bind_error_parameter,
            side_effect_count,
            transaction_count,
            streamed_before_drop,
        })
    }

    fn query_types(
        &self,
        _req: proto::QueryTypesRequest,
    ) -> Result<proto::QueryTypesResponse, ServiceError> {
        // Keep temp-table setup and use on one host-owned transaction
        // connection; checkout hygiene intentionally discards temp objects.
        let tx = database::begin_transaction().map_err(raw_db_error)?;
        tx.execute(
            "CREATE TEMP TABLE type_test (
                b boolean, i2 smallint, i4 integer, i8 bigint,
                f4 real, f8 double precision, t text, ts timestamptz
            )",
            &[],
        )
        .map_err(raw_db_error)?;

        tx.execute(
            "INSERT INTO type_test (b, i2, i4, i8, f4, f8, t, ts) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            &[
                PgValue::Boolean(true),
                PgValue::Int2(42),
                PgValue::Int4(1000),
                PgValue::Int8(9999999),
                PgValue::Float4(std::f32::consts::PI),
                PgValue::Float8(std::f64::consts::E),
                PgValue::Text("hello".to_string()),
                PgValue::Timestamptz(1700000000),
            ],
        )
        .map_err(raw_db_error)?;

        let rows = tx
            .query("SELECT * FROM type_test LIMIT 1", &[])
            .map_err(raw_db_error)?;
        tx.commit().map_err(raw_db_error)?;

        let row = rows.first().map(|row| proto::QueryRow {
            columns: row
                .columns
                .iter()
                .map(|column| column_to_proto(&column.name, column))
                .collect(),
        });

        Ok(proto::QueryTypesResponse { row })
    }

    fn transaction_commit(
        &self,
        req: proto::TransactionCommitRequest,
    ) -> Result<proto::TransactionCommitResponse, ServiceError> {
        let table = if req.table_name.is_empty() {
            "tx_commit_test"
        } else {
            &req.table_name
        };

        database::execute(
            &format!("CREATE TABLE IF NOT EXISTS {table} (id integer)"),
            &[],
        )
        .map_err(raw_db_error)?;

        // Clean up from prior runs
        database::execute(&format!("DELETE FROM {table}"), &[]).map_err(raw_db_error)?;

        let tx = database::begin_transaction().map_err(raw_db_error)?;
        tx.execute(&format!("INSERT INTO {table} (id) VALUES (1)"), &[])
            .map_err(raw_db_error)?;
        tx.commit().map_err(raw_db_error)?;

        let count =
            query_scalar::<i64>(&format!("SELECT count(*) FROM {table}")).fetch_exactly_one()?;

        Ok(proto::TransactionCommitResponse { count })
    }

    fn transaction_rollback(
        &self,
        req: proto::TransactionRollbackRequest,
    ) -> Result<proto::TransactionRollbackResponse, ServiceError> {
        let table = if req.table_name.is_empty() {
            "tx_rollback_test"
        } else {
            &req.table_name
        };

        database::execute(
            &format!("CREATE TABLE IF NOT EXISTS {table} (id integer)"),
            &[],
        )
        .map_err(raw_db_error)?;

        database::execute(&format!("DELETE FROM {table}"), &[]).map_err(raw_db_error)?;

        let tx = database::begin_transaction().map_err(raw_db_error)?;
        tx.execute(&format!("INSERT INTO {table} (id) VALUES (1)"), &[])
            .map_err(raw_db_error)?;
        tx.rollback().map_err(raw_db_error)?;

        let count =
            query_scalar::<i64>(&format!("SELECT count(*) FROM {table}")).fetch_exactly_one()?;

        Ok(proto::TransactionRollbackResponse { count })
    }

    fn transaction_drop(
        &self,
        req: proto::TransactionDropRequest,
    ) -> Result<proto::TransactionDropResponse, ServiceError> {
        let table = if req.table_name.is_empty() {
            "tx_drop_test"
        } else {
            &req.table_name
        };

        database::execute(
            &format!("CREATE TABLE IF NOT EXISTS {table} (id integer)"),
            &[],
        )
        .map_err(raw_db_error)?;

        database::execute(&format!("DELETE FROM {table}"), &[]).map_err(raw_db_error)?;

        {
            let tx = database::begin_transaction().map_err(raw_db_error)?;
            tx.execute(&format!("INSERT INTO {table} (id) VALUES (1)"), &[])
                .map_err(raw_db_error)?;
            // tx is dropped here without commit or rollback
        }

        let count =
            query_scalar::<i64>(&format!("SELECT count(*) FROM {table}")).fetch_exactly_one()?;

        Ok(proto::TransactionDropResponse { count })
    }

    fn transaction_after_complete(
        &self,
        req: proto::TransactionAfterCompleteRequest,
    ) -> Result<proto::TransactionAfterCompleteResponse, ServiceError> {
        let tx = database::begin_transaction().map_err(raw_db_error)?;
        if req.rollback {
            tx.rollback().map_err(raw_db_error)?;
        } else {
            tx.commit().map_err(raw_db_error)?;
        }
        let result = match req.operation.as_str() {
            "query" => tx.query("SELECT 1", &[]).map(|_| ()),
            "execute" => tx.execute("SELECT 1", &[]).map(|_| ()),
            "query-stream" => tx.query_stream("SELECT 1", &[]).map(|_| ()),
            "commit" => tx.commit(),
            "rollback" => tx.rollback(),
            other => {
                return Err(ServiceError::bad_request(format!(
                    "unknown operation: {other}"
                )))
            }
        };
        let error_message = match result {
            Err(error) => db_error_message(error),
            Ok(()) => "unexpectedly succeeded".into(),
        };
        Ok(proto::TransactionAfterCompleteResponse { error_message })
    }

    fn error(&self, req: proto::ErrorRequest) -> Result<proto::ErrorResponse, ServiceError> {
        let params = parse_params(&req.params)?;
        let result = match req.operation.as_str() {
            "" | "query" => database::query(&req.sql, &params).map(|_| ()),
            "transaction" => (|| {
                let transaction = database::begin_transaction()?;
                let result = transaction.query(&req.sql, &params).map(|_| ());
                drop(transaction);
                result
            })(),
            "transaction-commit-after-error" => (|| {
                let transaction = database::begin_transaction()?;
                if transaction.query(&req.sql, &params).is_err() {
                    transaction.commit()?;
                }
                Ok(())
            })(),
            "stream" => (|| {
                let cursor = database::query_stream(&req.sql, &params)?;
                let result = cursor.next_batch(1).map(|_| ());
                drop(cursor);
                result
            })(),
            "transaction-stream" => (|| {
                let transaction = database::begin_transaction()?;
                let cursor = transaction.query_stream(&req.sql, &params)?;
                let result = cursor.next_batch(1).map(|_| ());
                drop(cursor);
                drop(transaction);
                result
            })(),
            other => {
                return Err(ServiceError::bad_request(format!(
                    "unknown error operation: {other}"
                )))
            }
        };

        Ok(match result {
            Ok(()) => proto::ErrorResponse {
                error_kind: "none".into(),
                error_message: "unexpectedly succeeded".into(),
                ..Default::default()
            },
            Err(error) => error_response(error),
        })
    }

    fn query_stream(
        &self,
        req: proto::QueryStreamRequest,
    ) -> Result<proto::QueryStreamResponse, ServiceError> {
        let params = parse_params(&req.params)?;
        let batch_size = if req.batch_size == 0 {
            10
        } else {
            req.batch_size
        };
        let cursor = database::query_stream(&req.sql, &params).map_err(raw_db_error)?;
        if cursor.next_batch(1_025).is_ok() {
            return Err(ServiceError::internal(
                "oversized raw cursor batch unexpectedly succeeded",
            ));
        }
        let mut all_rows = vec![];
        let mut batch_count: u32 = 0;
        loop {
            let batch = cursor.next_batch(batch_size).map_err(raw_db_error)?;
            batch_count += 1;
            if batch.is_empty() {
                break;
            }
            for row in &batch {
                let cols = row
                    .columns
                    .iter()
                    .map(|c| column_to_proto(&c.name, c))
                    .collect();
                all_rows.push(proto::QueryRow { columns: cols });
            }
        }
        Ok(proto::QueryStreamResponse {
            rows: all_rows,
            batch_count,
        })
    }

    fn query_stream_drop(
        &self,
        req: proto::QueryStreamDropRequest,
    ) -> Result<proto::QueryStreamDropResponse, ServiceError> {
        let cursor = database::query_stream(&req.sql, &[]).map_err(raw_db_error)?;
        let mut fetched: u32 = 0;
        while fetched < req.fetch_count {
            let remaining = req.fetch_count - fetched;
            let batch = cursor.next_batch(remaining).map_err(raw_db_error)?;
            if batch.is_empty() {
                break;
            }
            fetched += batch.len() as u32;
        }
        // Drop the cursor without consuming all rows
        drop(cursor);
        Ok(proto::QueryStreamDropResponse { fetched })
    }

    fn alloc_transactions(
        &self,
        req: proto::AllocResourcesRequest,
    ) -> Result<proto::AllocResourcesResponse, ServiceError> {
        let mut held = Vec::new();
        let mut resp = proto::AllocResourcesResponse::default();
        alloc_loop(
            req.initial,
            &mut resp,
            database::begin_transaction,
            &mut held,
        );
        for _ in 0..req.drop_count {
            held.pop(); // Transaction dropped here -> host drop -> live-count decrement
        }
        alloc_loop(
            req.additional,
            &mut resp,
            database::begin_transaction,
            &mut held,
        );
        resp.held = held.len() as u32;
        Ok(resp)
    }

    fn alloc_cursors(
        &self,
        req: proto::AllocResourcesRequest,
    ) -> Result<proto::AllocResourcesResponse, ServiceError> {
        let sql = "SELECT generate_series(1, 100) AS n";
        let mut held = Vec::new();
        let mut resp = proto::AllocResourcesResponse::default();
        alloc_loop(
            req.initial,
            &mut resp,
            || database::query_stream(sql, &[]),
            &mut held,
        );
        for _ in 0..req.drop_count {
            held.pop(); // RowCursor dropped here -> host drop -> live-count decrement
        }
        alloc_loop(
            req.additional,
            &mut resp,
            || database::query_stream(sql, &[]),
            &mut held,
        );
        resp.held = held.len() as u32;
        Ok(resp)
    }
}

fn alloc_loop<T>(
    n: u32,
    resp: &mut proto::AllocResourcesResponse,
    mut make: impl FnMut() -> Result<T, database::DbError>,
    held: &mut Vec<T>,
) {
    for _ in 0..n {
        match make() {
            Ok(v) => held.push(v),
            Err(database::DbError::Connection(m)) => {
                resp.hit_cap = true;
                resp.error_kind = "connection".into();
                resp.error_message = m;
                break;
            }
            Err(database::DbError::Query(m)) => {
                resp.error_kind = "query".into();
                resp.error_message = m;
                break;
            }
            Err(database::DbError::UnsupportedResultType(details)) => {
                resp.error_kind = "unsupported-result-type".into();
                resp.error_message = format!(
                    "PostgreSQL type {} (OID {})",
                    details.postgres_type_name, details.postgres_type_oid
                );
                break;
            }
        }
    }
}
