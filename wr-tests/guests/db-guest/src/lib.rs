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

use wr_sdk::prelude::*;

struct Component;
wr_sdk::export!(Component with_types_in wr_sdk::bindings);

impl wr_sdk::ServiceGuest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        proto::db_test_service_handle(&Component, request, response_out);
    }
}

fn parse_params(params: &[proto::DbParam]) -> Result<Vec<PgValue>, ServiceError> {
    params
        .iter()
        .map(|param| match param.value.as_ref() {
            Some(proto::db_param::Value::NullMarker(true)) => Ok(PgValue::Null),
            Some(proto::db_param::Value::NullMarker(false)) | None => {
                Err(ServiceError::bad_request("DB parameter value is required"))
            }
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
        PgValue::Null => ("null", Value::NullMarker(true)),
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

impl proto::DbTestService for Component {
    fn execute(&self, req: proto::ExecuteRequest) -> Result<proto::ExecuteResponse, ServiceError> {
        let params = parse_params(&req.params)?;
        let affected = database::execute(&req.sql, &params)?;
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
            Err(e) => Err(ServiceError::from(e)),
        }
    }

    fn query_types(
        &self,
        _req: proto::QueryTypesRequest,
    ) -> Result<proto::QueryTypesResponse, ServiceError> {
        // Create a temp table with various types, insert, query back
        database::execute(
            "CREATE TEMP TABLE IF NOT EXISTS type_test (
                b boolean, i2 smallint, i4 integer, i8 bigint,
                f4 real, f8 double precision, t text, ts timestamptz
            )",
            &[],
        )?;

        database::execute(
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
        ?;

        let rows = database::query("SELECT * FROM type_test LIMIT 1", &[])?;

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
        )?;

        // Clean up from prior runs
        database::execute(&format!("DELETE FROM {table}"), &[])?;

        let tx = database::begin_transaction()?;
        tx.execute(&format!("INSERT INTO {table} (id) VALUES (1)"), &[])?;
        tx.commit()?;

        let rows = database::query(&format!("SELECT count(*) as cnt FROM {table}"), &[])?;

        let count = rows
            .first()
            .map(|r| r.get_i64(0))
            .transpose()?
            .unwrap_or(-1);

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
        )?;

        database::execute(&format!("DELETE FROM {table}"), &[])?;

        let tx = database::begin_transaction()?;
        tx.execute(&format!("INSERT INTO {table} (id) VALUES (1)"), &[])?;
        tx.rollback()?;

        let rows = database::query(&format!("SELECT count(*) as cnt FROM {table}"), &[])?;

        let count = rows
            .first()
            .map(|r| r.get_i64(0))
            .transpose()?
            .unwrap_or(-1);

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
        )?;

        database::execute(&format!("DELETE FROM {table}"), &[])?;

        {
            let tx = database::begin_transaction()?;
            tx.execute(&format!("INSERT INTO {table} (id) VALUES (1)"), &[])?;
            // tx is dropped here without commit or rollback
        }

        let rows = database::query(&format!("SELECT count(*) as cnt FROM {table}"), &[])?;

        let count = rows
            .first()
            .map(|r| r.get_i64(0))
            .transpose()?
            .unwrap_or(-1);

        Ok(proto::TransactionDropResponse { count })
    }

    fn transaction_after_complete(
        &self,
        req: proto::TransactionAfterCompleteRequest,
    ) -> Result<proto::TransactionAfterCompleteResponse, ServiceError> {
        let tx = database::begin_transaction()?;
        if req.rollback {
            tx.rollback()?;
        } else {
            tx.commit()?;
        }
        let result = match req.operation.as_str() {
            "query" => tx.query("SELECT 1", &[]).map(|_| ()),
            "execute" => tx.execute("SELECT 1", &[]).map(|_| ()),
            "query-stream" => tx.query_stream("SELECT 1", &[]).map(|_| ()),
            "commit" => tx.commit(),
            "rollback" => tx.rollback(),
            other => return Err(ServiceError::bad_request(format!("unknown operation: {other}"))),
        };
        let error_message = match result {
            Err(database::DbError::Query(message)) | Err(database::DbError::Connection(message)) => message,
            Ok(()) => "unexpectedly succeeded".into(),
        };
        Ok(proto::TransactionAfterCompleteResponse { error_message })
    }

    fn error(&self, req: proto::ErrorRequest) -> Result<proto::ErrorResponse, ServiceError> {
        let params = parse_params(&req.params)?;
        match database::execute(&req.sql, &params) {
            Ok(_) => Ok(proto::ErrorResponse {
                error_kind: "none".into(),
                error_message: "unexpectedly succeeded".into(),
            }),
            Err(e) => {
                let (kind, msg) = match e {
                    database::DbError::Connection(m) => ("connection", m),
                    database::DbError::Query(m) => ("query", m),
                };
                Ok(proto::ErrorResponse {
                    error_kind: kind.into(),
                    error_message: msg,
                })
            }
        }
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
        let cursor = database::query_stream(&req.sql, &params)?;
        let mut all_rows = vec![];
        let mut batch_count: u32 = 0;
        loop {
            let batch = cursor.next_batch(batch_size)?;
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
        let cursor = database::query_stream(&req.sql, &[])?;
        let mut fetched: u32 = 0;
        while fetched < req.fetch_count {
            let remaining = req.fetch_count - fetched;
            let batch = cursor.next_batch(remaining)?;
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
        }
    }
}
