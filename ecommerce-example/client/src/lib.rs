#[allow(warnings)]
mod bindings;

use bindings::wasi::http::outgoing_handler;
use bindings::wasi::http::types::{Fields, Method, OutgoingBody, OutgoingRequest, Scheme};
use bindings::wasi::io::streams::StreamError;

struct Component;
bindings::export!(Component with_types_in bindings);

// ── Product catalogue ─────────────────────────────────────────────────────────

const PRODUCTS: &[&str] = &[
    "prod-001", "prod-002", "prod-003", "prod-004", "prod-005",
    "prod-006", "prod-007", "prod-008", "prod-009", "prod-010",
    "prod-011", "prod-012", "prod-013", "prod-014", "prod-015",
    "prod-016", "prod-017", "prod-018", "prod-019", "prod-020",
    "prod-021", "prod-022", "prod-023", "prod-024", "prod-025",
    "prod-026", "prod-027", "prod-028", "prod-029", "prod-030",
    "prod-031", "prod-032", "prod-033", "prod-034", "prod-035",
    "prod-036", "prod-037", "prod-038", "prod-039", "prod-040",
    "prod-041", "prod-042", "prod-043", "prod-044", "prod-045",
    "prod-046", "prod-047", "prod-048", "prod-049", "prod-050",
];

// ── Inventory service authority: "{module}.{namespace}" ───────────────────────
// The proxy routing layer splits on '.' to get module=inventory, ns=ecommerce.
const INVENTORY: &str = "inventory.ecommerce";

impl bindings::Guest for Component {
    fn run() {
        log("client starting");

        // Seed inventory — idempotent (ON CONFLICT DO NOTHING in inventory service).
        match http_post(INVENTORY, "/seed", "{}") {
            Ok((200, _)) => log("inventory seeded"),
            Ok((status, body)) => log(&format!("seed response: {} {}", status, body)),
            Err(e) => log(&format!("seed error: {}", e)),
        }

        // Track purchases so we can return some later.
        let mut purchased: Vec<(&str, i64)> = Vec::new();

        for i in 0u64..100 {
            // Deterministic pseudo-random selection spread across all 50 products.
            let product_id = PRODUCTS[((i.wrapping_mul(7).wrapping_add(13)) % 50) as usize];
            let quantity = (i % 5 + 1) as i64;

            let buy_body =
                format!(r#"{{"product_id":"{}","quantity":{}}}"#, product_id, quantity);

            match http_post(INVENTORY, "/buy", &buy_body) {
                Ok((200, body)) => {
                    log(&format!("bought {} x{} — {}", product_id, quantity, body));
                    purchased.push((product_id, quantity));
                }
                Ok((409, body)) => {
                    log(&format!("out of stock {} x{} — {}", product_id, quantity, body));
                }
                Ok((status, body)) => {
                    log(&format!(
                        "buy error {} x{}: HTTP {} {}",
                        product_id, quantity, status, body
                    ));
                }
                Err(e) => log(&format!("http error buying {}: {}", product_id, e)),
            }

            // Return ~30 % of purchases (every 3rd iteration when we have items to return).
            if i % 10 < 3 && !purchased.is_empty() {
                let (ret_id, ret_qty) = purchased.remove(0);
                let ret_body =
                    format!(r#"{{"product_id":"{}","quantity":{}}}"#, ret_id, ret_qty);
                match http_post(INVENTORY, "/return", &ret_body) {
                    Ok((200, body)) => {
                        log(&format!("returned {} x{} — {}", ret_id, ret_qty, body));
                    }
                    Ok((status, body)) => {
                        log(&format!("return error: HTTP {} {}", status, body));
                    }
                    Err(e) => log(&format!("http error returning {}: {}", ret_id, e)),
                }
            }
        }

        log("client done");
    }
}

// ── HTTP helper ───────────────────────────────────────────────────────────────

fn http_post(authority: &str, path: &str, body: &str) -> Result<(u16, String), String> {
    let headers = Fields::new();
    headers
        .set(
            &"content-type".to_string(),
            &[b"application/json".to_vec()],
        )
        .map_err(|e| format!("set header: {:?}", e))?;

    let req = OutgoingRequest::new(headers);
    req.set_method(&Method::Post)
        .map_err(|_| "set method")?;
    req.set_scheme(Some(&Scheme::Http))
        .map_err(|_| "set scheme")?;
    req.set_authority(Some(authority))
        .map_err(|_| "set authority")?;
    req.set_path_with_query(Some(path))
        .map_err(|_| "set path")?;

    let outgoing_body = req.body().map_err(|_| "get body")?;
    {
        let stream = outgoing_body.write().map_err(|_| "get write stream")?;
        stream
            .blocking_write_and_flush(body.as_bytes())
            .map_err(|e| format!("write: {:?}", e))?;
    }
    OutgoingBody::finish(outgoing_body, None)
        .map_err(|e| format!("finish body: {:?}", e))?;

    let future_resp = outgoing_handler::handle(req, None)
        .map_err(|e| format!("handle: {:?}", e))?;

    // Poll until the response arrives.
    loop {
        match future_resp.get() {
            Some(result) => {
                let response = result
                    .map_err(|()| "response error".to_string())?
                    .map_err(|e| format!("http error: {:?}", e))?;

                let status = response.status();
                let incoming_body = response.consume().map_err(|_| "consume response")?;
                let stream = incoming_body
                    .stream()
                    .map_err(|_| "response body stream")?;

                let mut resp_bytes = Vec::new();
                loop {
                    match stream.blocking_read(8192) {
                        Ok(chunk) if chunk.is_empty() => break,
                        Ok(chunk) => resp_bytes.extend_from_slice(&chunk),
                        Err(StreamError::Closed) => break,
                        Err(StreamError::LastOperationFailed(_)) => break,
                    }
                }

                return Ok((status, String::from_utf8_lossy(&resp_bytes).to_string()));
            }
            None => {
                future_resp.subscribe().block();
            }
        }
    }
}

// ── Stderr logging ────────────────────────────────────────────────────────────

fn log(msg: &str) {
    use bindings::wasi::cli::stderr;
    let err = stderr::get_stderr();
    let _ = err.blocking_write_and_flush(msg.as_bytes());
    let _ = err.blocking_write_and_flush(b"\n");
}
