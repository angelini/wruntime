#[allow(warnings)]
mod bindings;

#[allow(dead_code)]
mod proto {
    include!(concat!(env!("OUT_DIR"), "/ecommerce.rs"));
}

use bindings::wasi::http::outgoing_handler;
use bindings::wasi::http::types::{Fields, Method, OutgoingBody, OutgoingRequest, Scheme};
use bindings::wasi::io::streams::StreamError;
use prost::Message;

struct Component;
bindings::export!(Component with_types_in bindings);

// ── Inventory service authority: "{module}.{namespace}" ───────────────────────
const INVENTORY: &str = "inventory.ecommerce";

impl bindings::Guest for Component {
    fn run() {
        log("client starting");

        // Seed inventory — idempotent (ON CONFLICT DO NOTHING in inventory service).
        let seed_bytes = proto::SeedRequest {}.encode_to_vec();
        match http_rpc(INVENTORY, "/ecommerce.InventoryService/Seed", &seed_bytes) {
            Ok((200, _)) => log("inventory seeded"),
            Ok((status, _)) => log(&format!("seed response: {status}")),
            Err(e) => log(&format!("seed error: {e}")),
        }

        // Track purchases so we can return some later.
        let mut purchased: Vec<(&str, i64)> = Vec::new();

        for i in 0u64..100 {
            // Deterministic pseudo-random selection spread across all 50 products.
            let product_id = PRODUCTS[((i.wrapping_mul(7).wrapping_add(13)) % 50) as usize];
            let quantity = (i % 5 + 1) as i64;

            let buy_bytes = proto::BuyRequest {
                product_id: product_id.to_string(),
                quantity,
            }
            .encode_to_vec();

            match http_rpc(INVENTORY, "/ecommerce.InventoryService/Buy", &buy_bytes) {
                Ok((200, body)) => {
                    let detail = proto::BuyResponse::decode(body.as_slice())
                        .map(|r| format!("bought={} remaining={}", r.bought, r.remaining))
                        .unwrap_or_else(|_| "ok".to_string());
                    log(&format!("bought {} x{} — {}", product_id, quantity, detail));
                    purchased.push((product_id, quantity));
                }
                Ok((409, _)) => {
                    log(&format!("out of stock {} x{}", product_id, quantity));
                }
                Ok((status, _)) => {
                    log(&format!("buy error {} x{}: HTTP {}", product_id, quantity, status));
                }
                Err(e) => log(&format!("http error buying {}: {}", product_id, e)),
            }

            // Return ~30 % of purchases (every 3rd iteration when we have items to return).
            if i % 10 < 3 && !purchased.is_empty() {
                let (ret_id, ret_qty) = purchased.remove(0);

                let ret_bytes = proto::ReturnRequest {
                    product_id: ret_id.to_string(),
                    quantity: ret_qty,
                }
                .encode_to_vec();

                match http_rpc(INVENTORY, "/ecommerce.InventoryService/Return", &ret_bytes) {
                    Ok((200, body)) => {
                        let detail = proto::ReturnResponse::decode(body.as_slice())
                            .map(|r| format!("returned={} product={}", r.returned, r.product_id))
                            .unwrap_or_else(|_| "ok".to_string());
                        log(&format!("returned {} x{} — {}", ret_id, ret_qty, detail));
                    }
                    Ok((status, _)) => {
                        log(&format!("return error: HTTP {}", status));
                    }
                    Err(e) => log(&format!("http error returning {}: {}", ret_id, e)),
                }
            }
        }

        log("client done");
    }
}

// ── Product catalogue ─────────────────────────────────────────────────────────

const PRODUCTS: &[&str] = &[
    "prod-001", "prod-002", "prod-003", "prod-004", "prod-005", "prod-006", "prod-007", "prod-008",
    "prod-009", "prod-010", "prod-011", "prod-012", "prod-013", "prod-014", "prod-015", "prod-016",
    "prod-017", "prod-018", "prod-019", "prod-020", "prod-021", "prod-022", "prod-023", "prod-024",
    "prod-025", "prod-026", "prod-027", "prod-028", "prod-029", "prod-030", "prod-031", "prod-032",
    "prod-033", "prod-034", "prod-035", "prod-036", "prod-037", "prod-038", "prod-039", "prod-040",
    "prod-041", "prod-042", "prod-043", "prod-044", "prod-045", "prod-046", "prod-047", "prod-048",
    "prod-049", "prod-050",
];

// ── HTTP helper ───────────────────────────────────────────────────────────────

fn http_rpc(authority: &str, path: &str, body: &[u8]) -> Result<(u16, Vec<u8>), String> {
    let headers = Fields::new();
    headers
        .set("content-type", &[b"application/x-protobuf".to_vec()])
        .map_err(|e| format!("set header: {:?}", e))?;

    let req = OutgoingRequest::new(headers);
    req.set_method(&Method::Post).map_err(|_| "set method")?;
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
            .blocking_write_and_flush(body)
            .map_err(|e| format!("write: {:?}", e))?;
    }
    OutgoingBody::finish(outgoing_body, None).map_err(|e| format!("finish body: {:?}", e))?;

    let future_resp =
        outgoing_handler::handle(req, None).map_err(|e| format!("handle: {:?}", e))?;

    loop {
        match future_resp.get() {
            Some(result) => {
                let response = result
                    .map_err(|()| "response error".to_string())?
                    .map_err(|e| format!("http error: {:?}", e))?;

                let status = response.status();
                let incoming_body = response.consume().map_err(|_| "consume response")?;
                let stream = incoming_body.stream().map_err(|_| "response body stream")?;

                let mut resp_bytes = Vec::new();
                loop {
                    match stream.blocking_read(8192) {
                        Ok(chunk) if chunk.is_empty() => break,
                        Ok(chunk) => resp_bytes.extend_from_slice(&chunk),
                        Err(StreamError::Closed) => break,
                        Err(StreamError::LastOperationFailed(_)) => break,
                    }
                }

                return Ok((status, resp_bytes));
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
