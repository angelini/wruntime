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

/// Parse a JSON params array into PgValue variants.
/// Format: [{"type":"text","value":"hello"},{"type":"int4","value":"42"}]
fn parse_params(json: &str) -> Vec<PgValue> {
    if json.is_empty() {
        return vec![];
    }
    // Minimal JSON parsing without serde — each param is {"type":"<t>","value":"<v>"}
    // For test purposes we support: null, text, int4, int8, boolean, float8, numeric
    let mut params = vec![];
    // Strip outer brackets
    let inner = json.trim().trim_start_matches('[').trim_end_matches(']');
    if inner.trim().is_empty() {
        return params;
    }
    // Split on "},{" to get individual objects
    for obj in split_json_objects(inner) {
        let typ = extract_json_field(&obj, "type");
        let val = extract_json_field(&obj, "value");
        let pg = match typ.as_str() {
            "null" => PgValue::Null,
            "text" => PgValue::Text(val),
            "int4" => PgValue::Int4(val.parse().unwrap_or(0)),
            "int8" => PgValue::Int8(val.parse().unwrap_or(0)),
            "boolean" => PgValue::Boolean(val == "true"),
            "float8" => PgValue::Float8(val.parse().unwrap_or(0.0)),
            "numeric" => PgValue::Numeric(val),
            _ => PgValue::Text(val),
        };
        params.push(pg);
    }
    params
}

fn split_json_objects(s: &str) -> Vec<String> {
    let mut results = vec![];
    let mut depth = 0;
    let mut start = 0;
    for (i, c) in s.char_indices() {
        match c {
            '{' => {
                if depth == 0 {
                    start = i;
                }
                depth += 1;
            }
            '}' => {
                depth -= 1;
                if depth == 0 {
                    results.push(s[start..=i].to_string());
                }
            }
            _ => {}
        }
    }
    results
}

fn extract_json_field(obj: &str, field: &str) -> String {
    let pattern = format!("\"{}\":\"", field);
    if let Some(start) = obj.find(&pattern) {
        let rest = &obj[start + pattern.len()..];
        if let Some(end) = rest.find('"') {
            return rest[..end].to_string();
        }
    }
    String::new()
}

fn column_to_json(name: &str, col: &database::Column) -> String {
    let (typ, val) = match &col.value {
        PgValue::Null => ("null", "null".to_string()),
        PgValue::Boolean(b) => ("boolean", b.to_string()),
        PgValue::Int2(v) => ("int2", v.to_string()),
        PgValue::Int4(v) => ("int4", v.to_string()),
        PgValue::Int8(v) => ("int8", v.to_string()),
        PgValue::Float4(v) => ("float4", v.to_string()),
        PgValue::Float8(v) => ("float8", v.to_string()),
        PgValue::Text(v) => ("text", v.clone()),
        PgValue::Bytea(v) => ("bytea", format!("{:?}", v)),
        PgValue::Timestamptz(v) => ("timestamptz", v.to_string()),
        PgValue::Timestamp(v) => ("timestamp", v.to_string()),
        PgValue::Date(v) => ("date", v.to_string()),
        PgValue::Time(v) => ("time", v.to_string()),
        PgValue::Interval(iv) => (
            "interval",
            format!("{}m{}d{}us", iv.months, iv.days, iv.microseconds),
        ),
        PgValue::Numeric(v) => ("numeric", v.clone()),
        PgValue::Uuid((hi, lo)) => ("uuid", format!("{hi:016x}{lo:016x}")),
        PgValue::Jsonb(v) => ("jsonb", v.clone()),
        PgValue::Oid(v) => ("oid", v.to_string()),
        PgValue::BoolArray(a) => ("bool[]", format!("{:?}", a)),
        PgValue::Int2Array(a) => ("int2[]", format!("{:?}", a)),
        PgValue::Int4Array(a) => ("int4[]", format!("{:?}", a)),
        PgValue::Int8Array(a) => ("int8[]", format!("{:?}", a)),
        PgValue::Float4Array(a) => ("float4[]", format!("{:?}", a)),
        PgValue::Float8Array(a) => ("float8[]", format!("{:?}", a)),
        PgValue::TextArray(a) => ("text[]", format!("{:?}", a)),
        PgValue::TimestamptzArray(a) => ("timestamptz[]", format!("{:?}", a)),
        PgValue::TimestampArray(a) => ("timestamp[]", format!("{:?}", a)),
        PgValue::UuidArray(a) => ("uuid[]", format!("{:?}", a)),
        PgValue::JsonbArray(a) => ("jsonb[]", format!("{:?}", a)),
    };
    format!(
        "{{\"name\":\"{}\",\"type\":\"{}\",\"value\":\"{}\"}}",
        name, typ, val
    )
}

impl proto::DbTestService for Component {
    fn execute(&self, req: proto::ExecuteRequest) -> Result<proto::ExecuteResponse, ServiceError> {
        let params = parse_params(&req.params_json);
        let affected = database::execute(&req.sql, &params)?;
        Ok(proto::ExecuteResponse { affected })
    }

    fn query(&self, req: proto::QueryRequest) -> Result<proto::QueryResponse, ServiceError> {
        let params = parse_params(&req.params_json);
        match database::query(&req.sql, &params) {
            Ok(rows) => {
                let proto_rows = rows
                    .iter()
                    .map(|row| {
                        let cols = row
                            .columns
                            .iter()
                            .map(|c| column_to_json(&c.name, c))
                            .collect();
                        proto::QueryRow { columns_json: cols }
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

        let row_json = if let Some(row) = rows.first() {
            let parts: Vec<String> = row
                .columns
                .iter()
                .map(|c| column_to_json(&c.name, c))
                .collect();
            format!("[{}]", parts.join(","))
        } else {
            "[]".to_string()
        };

        Ok(proto::QueryTypesResponse { row_json })
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

    fn error(&self, req: proto::ErrorRequest) -> Result<proto::ErrorResponse, ServiceError> {
        let params = parse_params(&req.params_json);
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
        let params = parse_params(&req.params_json);
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
                    .map(|c| column_to_json(&c.name, c))
                    .collect();
                all_rows.push(proto::QueryRow { columns_json: cols });
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
