#[allow(dead_code)]
mod proto {
    include!(concat!(env!("OUT_DIR"), "/test.rs"));
}

#[allow(dead_code, unused_imports)]
mod bindings;

use wr_sdk::bindings::wasi::http::types::{IncomingRequest, ResponseOutparam};
use wr_sdk::bindings::wruntime::db::database::{self, PgValue};
use wr_sdk::io::{read_body, send_response};
use wr_sdk::ServiceError;

struct Component;
wr_sdk::export!(Component with_types_in wr_sdk::bindings);

impl wr_sdk::ServiceGuest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        let path = request.path_with_query().unwrap_or_default();
        let body = read_body(request.consume().unwrap());
        let (status, resp) = proto::db_test_service_router(&Component, &path, &body);
        send_response(response_out, status, resp);
    }
}

/// Parse a JSON params array into PgValue variants.
/// Format: [{"type":"text","value":"hello"},{"type":"int4","value":"42"}]
fn parse_params(json: &str) -> Vec<PgValue> {
    if json.is_empty() {
        return vec![];
    }
    // Minimal JSON parsing without serde — each param is {"type":"<t>","value":"<v>"}
    // For test purposes we support: null, text, int4, int8, boolean, float8
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
        PgValue::Date(v) => ("date", v.to_string()),
        PgValue::Time(v) => ("time", v.to_string()),
        PgValue::Numeric(v) => ("numeric", v.clone()),
        PgValue::Uuid((hi, lo)) => ("uuid", format!("{hi:016x}{lo:016x}")),
        PgValue::Jsonb(v) => ("jsonb", v.clone()),
        PgValue::Oid(v) => ("oid", v.to_string()),
    };
    format!("{{\"name\":\"{}\",\"type\":\"{}\",\"value\":\"{}\"}}", name, typ, val)
}

impl proto::DbTestService for Component {
    fn execute(
        &self,
        req: proto::ExecuteRequest,
    ) -> Result<proto::ExecuteResponse, ServiceError> {
        let params = parse_params(&req.params_json);
        match database::execute(&req.sql, &params) {
            Ok(affected) => Ok(proto::ExecuteResponse { affected }),
            Err(e) => Err(ServiceError::internal(format!("{e:?}"))),
        }
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
            Err(e) => Err(ServiceError::internal(format!("{e:?}"))),
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
        )
        .map_err(|e| ServiceError::internal(format!("{e:?}")))?;

        database::execute(
            "INSERT INTO type_test (b, i2, i4, i8, f4, f8, t, ts) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            &[
                PgValue::Boolean(true),
                PgValue::Int2(42),
                PgValue::Int4(1000),
                PgValue::Int8(9999999),
                PgValue::Float4(3.14),
                PgValue::Float8(2.71828),
                PgValue::Text("hello".to_string()),
                PgValue::Timestamptz(1700000000),
            ],
        )
        .map_err(|e| ServiceError::internal(format!("{e:?}")))?;

        let rows = database::query("SELECT * FROM type_test LIMIT 1", &[])
            .map_err(|e| ServiceError::internal(format!("{e:?}")))?;

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
        )
        .map_err(|e| ServiceError::internal(format!("{e:?}")))?;

        // Clean up from prior runs
        database::execute(&format!("DELETE FROM {table}"), &[])
            .map_err(|e| ServiceError::internal(format!("{e:?}")))?;

        let tx = database::begin_transaction()
            .map_err(|e| ServiceError::internal(format!("{e:?}")))?;
        tx.execute(&format!("INSERT INTO {table} (id) VALUES (1)"), &[])
            .map_err(|e| ServiceError::internal(format!("{e:?}")))?;
        tx.commit()
            .map_err(|e| ServiceError::internal(format!("{e:?}")))?;

        let rows = database::query(&format!("SELECT count(*) as cnt FROM {table}"), &[])
            .map_err(|e| ServiceError::internal(format!("{e:?}")))?;

        let count = rows
            .first()
            .and_then(|r| r.columns.first())
            .map(|c| match &c.value {
                PgValue::Int8(v) => *v,
                _ => -1,
            })
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
        )
        .map_err(|e| ServiceError::internal(format!("{e:?}")))?;

        database::execute(&format!("DELETE FROM {table}"), &[])
            .map_err(|e| ServiceError::internal(format!("{e:?}")))?;

        let tx = database::begin_transaction()
            .map_err(|e| ServiceError::internal(format!("{e:?}")))?;
        tx.execute(&format!("INSERT INTO {table} (id) VALUES (1)"), &[])
            .map_err(|e| ServiceError::internal(format!("{e:?}")))?;
        tx.rollback()
            .map_err(|e| ServiceError::internal(format!("{e:?}")))?;

        let rows = database::query(&format!("SELECT count(*) as cnt FROM {table}"), &[])
            .map_err(|e| ServiceError::internal(format!("{e:?}")))?;

        let count = rows
            .first()
            .and_then(|r| r.columns.first())
            .map(|c| match &c.value {
                PgValue::Int8(v) => *v,
                _ => -1,
            })
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
        )
        .map_err(|e| ServiceError::internal(format!("{e:?}")))?;

        database::execute(&format!("DELETE FROM {table}"), &[])
            .map_err(|e| ServiceError::internal(format!("{e:?}")))?;

        {
            let tx = database::begin_transaction()
                .map_err(|e| ServiceError::internal(format!("{e:?}")))?;
            tx.execute(&format!("INSERT INTO {table} (id) VALUES (1)"), &[])
                .map_err(|e| ServiceError::internal(format!("{e:?}")))?;
            // tx is dropped here without commit or rollback
        }

        let rows = database::query(&format!("SELECT count(*) as cnt FROM {table}"), &[])
            .map_err(|e| ServiceError::internal(format!("{e:?}")))?;

        let count = rows
            .first()
            .and_then(|r| r.columns.first())
            .map(|c| match &c.value {
                PgValue::Int8(v) => *v,
                _ => -1,
            })
            .unwrap_or(-1);

        Ok(proto::TransactionDropResponse { count })
    }

    fn error(&self, req: proto::ErrorRequest) -> Result<proto::ErrorResponse, ServiceError> {
        match database::execute(&req.sql, &[]) {
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
}
