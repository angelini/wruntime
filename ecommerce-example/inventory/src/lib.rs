#[allow(warnings)]
mod bindings;

use bindings::exports::wasi::http::incoming_handler::Guest;
use bindings::wasi::http::types::{
    Fields, IncomingBody, IncomingRequest, Method, OutgoingBody, OutgoingResponse, ResponseOutparam,
};
use bindings::wasi::io::streams::StreamError;
use bindings::wruntime::db::database::{self, PgValue};

struct Component;
bindings::export!(Component with_types_in bindings);

impl Guest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        let method = request.method();
        let path = request.path_with_query().unwrap_or_default();

        ensure_schema();

        let (status, body) = match (method, path.as_str()) {
            (Method::Post, "/seed") => handle_seed(),
            (Method::Get, p) if p.starts_with("/stock/") => {
                handle_get_stock(p.trim_start_matches("/stock/"))
            }
            (Method::Post, "/buy") => handle_buy(&read_body(request.consume().unwrap())),
            (Method::Post, "/return") => handle_return(&read_body(request.consume().unwrap())),
            _ => (404, r#"{"error":"not found"}"#.to_string()),
        };

        send_response(response_out, status, body);
    }
}

// ── DB helpers ────────────────────────────────────────────────────────────────

fn ensure_schema() {
    let _ = database::execute(
        "CREATE TABLE IF NOT EXISTS inventory (\
            product_id TEXT PRIMARY KEY, \
            name       TEXT NOT NULL, \
            stock      BIGINT NOT NULL CHECK (stock >= 0)\
        )",
        &[],
    );
}

// ── Route handlers ────────────────────────────────────────────────────────────

fn handle_seed() -> (u16, String) {
    for i in 1u32..=50 {
        let id = format!("prod-{:03}", i);
        let name = format!("Product {}", i);
        let _ = database::execute(
            "INSERT INTO inventory (product_id, name, stock) \
             VALUES ($1, $2, 10000) ON CONFLICT DO NOTHING",
            &[PgValue::Text(id), PgValue::Text(name)],
        );
    }
    (200, r#"{"seeded":50}"#.to_string())
}

fn handle_get_stock(product_id: &str) -> (u16, String) {
    match database::query(
        "SELECT stock FROM inventory WHERE product_id = $1",
        &[PgValue::Text(product_id.to_string())],
    ) {
        Err(e) => (500, format!(r#"{{"error":"{:?}"}}"#, e)),
        Ok(rows) if rows.is_empty() => (
            404,
            format!(r#"{{"error":"product {} not found"}}"#, product_id),
        ),
        Ok(rows) => {
            let stock = match &rows[0].columns[0].value {
                PgValue::Int8(v) => *v,
                _ => return (500, r#"{"error":"unexpected column type"}"#.to_string()),
            };
            (
                200,
                format!(r#"{{"product_id":"{}","stock":{}}}"#, product_id, stock),
            )
        }
    }
}

fn handle_buy(body: &[u8]) -> (u16, String) {
    let (product_id, quantity) = match parse_body(body) {
        Ok(v) => v,
        Err(e) => return (400, format!(r#"{{"error":"{}"}}"#, e)),
    };

    let tx = match database::begin_transaction() {
        Ok(t) => t,
        Err(e) => return (500, format!(r#"{{"error":"{:?}"}}"#, e)),
    };

    let rows = match tx.query(
        "SELECT stock FROM inventory WHERE product_id = $1 FOR UPDATE",
        &[PgValue::Text(product_id.clone())],
    ) {
        Ok(r) => r,
        Err(e) => {
            let _ = tx.rollback();
            return (500, format!(r#"{{"error":"{:?}"}}"#, e));
        }
    };

    if rows.is_empty() {
        let _ = tx.rollback();
        return (
            404,
            format!(r#"{{"error":"product {} not found"}}"#, product_id),
        );
    }

    let stock = match &rows[0].columns[0].value {
        PgValue::Int8(v) => *v,
        _ => {
            let _ = tx.rollback();
            return (500, r#"{"error":"unexpected column type"}"#.to_string());
        }
    };

    if stock < quantity {
        let _ = tx.rollback();
        return (
            409,
            format!(r#"{{"error":"insufficient stock","available":{}}}"#, stock),
        );
    }

    if let Err(e) = tx.execute(
        "UPDATE inventory SET stock = stock - $2 WHERE product_id = $1",
        &[PgValue::Text(product_id.clone()), PgValue::Int8(quantity)],
    ) {
        let _ = tx.rollback();
        return (500, format!(r#"{{"error":"{:?}"}}"#, e));
    }

    if let Err(e) = tx.commit() {
        return (500, format!(r#"{{"error":"{:?}"}}"#, e));
    }

    (
        200,
        format!(
            r#"{{"bought":{},"remaining":{}}}"#,
            quantity,
            stock - quantity
        ),
    )
}

fn handle_return(body: &[u8]) -> (u16, String) {
    let (product_id, quantity) = match parse_body(body) {
        Ok(v) => v,
        Err(e) => return (400, format!(r#"{{"error":"{}"}}"#, e)),
    };

    match database::execute(
        "UPDATE inventory SET stock = stock + $2 WHERE product_id = $1",
        &[PgValue::Text(product_id.clone()), PgValue::Int8(quantity)],
    ) {
        Err(e) => (500, format!(r#"{{"error":"{:?}"}}"#, e)),
        Ok(0) => (
            404,
            format!(r#"{{"error":"product {} not found"}}"#, product_id),
        ),
        Ok(_) => (
            200,
            format!(
                r#"{{"returned":{},"product_id":"{}"}}"#,
                quantity, product_id
            ),
        ),
    }
}

// ── JSON body parsing ─────────────────────────────────────────────────────────

/// Parse `{"product_id":"...","quantity":N}` without an external JSON library.
fn parse_body(body: &[u8]) -> Result<(String, i64), String> {
    let s = std::str::from_utf8(body).map_err(|e| e.to_string())?;

    let product_id =
        json_str(s, "product_id").ok_or_else(|| "missing or invalid product_id".to_string())?;
    let quantity =
        json_num(s, "quantity").ok_or_else(|| "missing or invalid quantity".to_string())?;

    if quantity <= 0 {
        return Err("quantity must be > 0".to_string());
    }
    Ok((product_id, quantity))
}

fn json_str(s: &str, key: &str) -> Option<String> {
    let needle = format!("\"{}\"", key);
    let start = s.find(&needle)?;
    let after_colon = s[start + needle.len()..].trim_start();
    let after_colon = after_colon.strip_prefix(':')?.trim_start();
    let inner = after_colon.strip_prefix('"')?;
    let end = inner.find('"')?;
    Some(inner[..end].to_string())
}

fn json_num(s: &str, key: &str) -> Option<i64> {
    let needle = format!("\"{}\"", key);
    let start = s.find(&needle)?;
    let after_colon = s[start + needle.len()..].trim_start();
    let after_colon = after_colon.strip_prefix(':')?.trim_start();
    let end = after_colon
        .find(|c: char| !c.is_ascii_digit() && c != '-')
        .unwrap_or(after_colon.len());
    after_colon[..end].parse().ok()
}

// ── HTTP body I/O ─────────────────────────────────────────────────────────────

fn read_body(incoming: IncomingBody) -> Vec<u8> {
    let stream = incoming.stream().unwrap();
    let mut bytes = Vec::new();
    loop {
        match stream.blocking_read(8192) {
            Ok(chunk) if chunk.is_empty() => break,
            Ok(chunk) => bytes.extend_from_slice(&chunk),
            Err(StreamError::Closed) => break,
            Err(_) => break,
        }
    }
    drop(stream);
    IncomingBody::finish(incoming);
    bytes
}

fn send_response(response_out: ResponseOutparam, status: u16, body_str: String) {
    let headers = Fields::new();
    let _ = headers.set("content-type", &[b"application/json".to_vec()]);

    let resp = OutgoingResponse::new(headers);
    let _ = resp.set_status_code(status);

    let body = resp.body().unwrap();
    {
        let stream = body.write().unwrap();
        let _ = stream.blocking_write_and_flush(body_str.as_bytes());
    }

    ResponseOutparam::set(response_out, Ok(resp));
    let _ = OutgoingBody::finish(body, None);
}
