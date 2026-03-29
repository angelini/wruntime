#[allow(dead_code)]
mod proto {
    include!(concat!(env!("OUT_DIR"), "/ecommerce.rs"));
}

// Include generated bindings so the component-type metadata section
// (which declares `export wasi:http/incoming-handler`) is linked in.
#[allow(dead_code, unused_imports)]
mod bindings;

use wr_sdk::bindings::wasi::http::types::{IncomingRequest, Method, ResponseOutparam};
use wr_sdk::bindings::wruntime::db::database::{self, PgValue};
use wr_sdk::io::{err_body, read_body, send_response};
use prost::Message;

struct Component;
wr_sdk::export!(Component with_types_in wr_sdk::bindings);

impl wr_sdk::ServiceGuest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        let method = request.method();
        let path = request.path_with_query().unwrap_or_default();

        let body_bytes = read_body(request.consume().unwrap());

        let (status, body) = match (method, path.as_str()) {
            (Method::Post, "/ecommerce.inventory/Seed") => handle_seed(),
            (Method::Post, "/ecommerce.inventory/GetStock") => handle_get_stock(&body_bytes),
            (Method::Post, "/ecommerce.inventory/Buy") => handle_buy(&body_bytes),
            (Method::Post, "/ecommerce.inventory/Return") => handle_return(&body_bytes),
            (Method::Post, "/ecommerce.inventory/Transfer") => handle_transfer(&body_bytes),
            (Method::Post, "/ecommerce.inventory/Restock") => handle_restock(&body_bytes),
            _ => err_body(404, &format!("no handler for {path}")),
        };

        send_response(response_out, status, body);
    }
}

// ── Route handlers ────────────────────────────────────────────────────────────

fn handle_seed() -> (u16, Vec<u8>) {
    let _ = database::execute(
        "CREATE TABLE IF NOT EXISTS inventory (\
            product_id TEXT PRIMARY KEY, \
            name       TEXT NOT NULL, \
            stock      BIGINT NOT NULL CHECK (stock >= 0)\
        )",
        &[],
    );

    for i in 1u32..=50 {
        let id = format!("prod-{:03}", i);
        let name = format!("Product {}", i);
        let _ = database::execute(
            "INSERT INTO inventory (product_id, name, stock) \
             VALUES ($1, $2, 10000) ON CONFLICT DO NOTHING",
            &[PgValue::Text(id), PgValue::Text(name)],
        );
    }
    (200, proto::SeedResponse { seeded: 50 }.encode_to_vec())
}

fn handle_get_stock(body: &[u8]) -> (u16, Vec<u8>) {
    let req = match proto::GetStockRequest::decode(body) {
        Ok(r) => r,
        Err(e) => return err_body(400, &format!("invalid request: {e}")),
    };

    match database::query(
        "SELECT stock FROM inventory WHERE product_id = $1",
        &[PgValue::Text(req.product_id.clone())],
    ) {
        Err(e) => err_body(500, &format!("{e:?}")),
        Ok(rows) if rows.is_empty() => {
            err_body(404, &format!("product {} not found", req.product_id))
        }
        Ok(rows) => match &rows[0].columns[0].value {
            PgValue::Int8(v) => (
                200,
                proto::GetStockResponse {
                    product_id: req.product_id,
                    stock: *v,
                }
                .encode_to_vec(),
            ),
            _ => err_body(500, "unexpected column type"),
        },
    }
}

fn handle_buy(body: &[u8]) -> (u16, Vec<u8>) {
    let req = match proto::BuyRequest::decode(body) {
        Ok(r) => r,
        Err(e) => return err_body(400, &format!("invalid request: {e}")),
    };
    if req.quantity <= 0 {
        return err_body(400, "quantity must be > 0");
    }

    let tx = match database::begin_transaction() {
        Ok(t) => t,
        Err(e) => return err_body(500, &format!("{e:?}")),
    };

    let rows = match tx.query(
        "SELECT stock FROM inventory WHERE product_id = $1 FOR UPDATE",
        &[PgValue::Text(req.product_id.clone())],
    ) {
        Ok(r) => r,
        Err(e) => {
            let _ = tx.rollback();
            return err_body(500, &format!("{e:?}"));
        }
    };

    if rows.is_empty() {
        let _ = tx.rollback();
        return err_body(404, &format!("product {} not found", req.product_id));
    }

    let stock = match &rows[0].columns[0].value {
        PgValue::Int8(v) => *v,
        _ => {
            let _ = tx.rollback();
            return err_body(500, "unexpected column type");
        }
    };

    if stock < req.quantity {
        let _ = tx.rollback();
        return err_body(409, &format!("insufficient stock — available: {stock}"));
    }

    if let Err(e) = tx.execute(
        "UPDATE inventory SET stock = stock - $2 WHERE product_id = $1",
        &[PgValue::Text(req.product_id.clone()), PgValue::Int8(req.quantity)],
    ) {
        let _ = tx.rollback();
        return err_body(500, &format!("{e:?}"));
    }

    if let Err(e) = tx.commit() {
        return err_body(500, &format!("{e:?}"));
    }

    (
        200,
        proto::BuyResponse {
            bought: req.quantity,
            remaining: stock - req.quantity,
        }
        .encode_to_vec(),
    )
}

fn handle_transfer(body: &[u8]) -> (u16, Vec<u8>) {
    let req = match proto::TransferRequest::decode(body) {
        Ok(r) => r,
        Err(e) => return err_body(400, &format!("invalid request: {e}")),
    };
    if req.quantity <= 0 {
        return err_body(400, "quantity must be > 0");
    }
    if req.from_product_id == req.to_product_id {
        return err_body(400, "from and to products must differ");
    }

    let tx = match database::begin_transaction() {
        Ok(t) => t,
        Err(e) => return err_body(500, &format!("{e:?}")),
    };

    // Lock both rows in a consistent lexicographic order to avoid deadlocks under
    // concurrent transfers. We acquire the lock with the lower product_id first.
    let lock_first = if req.from_product_id < req.to_product_id {
        req.from_product_id.clone()
    } else {
        req.to_product_id.clone()
    };
    let lock_second = if req.from_product_id < req.to_product_id {
        req.to_product_id.clone()
    } else {
        req.from_product_id.clone()
    };

    for id in [&lock_first, &lock_second] {
        match tx.query(
            "SELECT 1 FROM inventory WHERE product_id = $1 FOR UPDATE",
            &[PgValue::Text(id.clone())],
        ) {
            Ok(rows) if rows.is_empty() => {
                let _ = tx.rollback();
                return err_body(404, &format!("product {id} not found"));
            }
            Err(e) => {
                let _ = tx.rollback();
                return err_body(500, &format!("{e:?}"));
            }
            Ok(_) => {}
        }
    }

    // Read source stock after both locks are held.
    let stock_from = match tx.query(
        "SELECT stock FROM inventory WHERE product_id = $1",
        &[PgValue::Text(req.from_product_id.clone())],
    ) {
        Ok(rows) => match rows.first().and_then(|r| match &r.columns[0].value {
            PgValue::Int8(v) => Some(*v),
            _ => None,
        }) {
            Some(s) => s,
            None => {
                let _ = tx.rollback();
                return err_body(500, "unexpected column type");
            }
        },
        Err(e) => {
            let _ = tx.rollback();
            return err_body(500, &format!("{e:?}"));
        }
    };

    if stock_from < req.quantity {
        let _ = tx.rollback();
        return err_body(409, &format!("insufficient stock — available: {stock_from}"));
    }

    if let Err(e) = tx.execute(
        "UPDATE inventory SET stock = stock - $2 WHERE product_id = $1",
        &[PgValue::Text(req.from_product_id.clone()), PgValue::Int8(req.quantity)],
    ) {
        let _ = tx.rollback();
        return err_body(500, &format!("{e:?}"));
    }

    if let Err(e) = tx.execute(
        "UPDATE inventory SET stock = stock + $2 WHERE product_id = $1",
        &[PgValue::Text(req.to_product_id.clone()), PgValue::Int8(req.quantity)],
    ) {
        let _ = tx.rollback();
        return err_body(500, &format!("{e:?}"));
    }

    if let Err(e) = tx.commit() {
        return err_body(500, &format!("{e:?}"));
    }

    (
        200,
        proto::TransferResponse { transferred: req.quantity }.encode_to_vec(),
    )
}

fn handle_restock(body: &[u8]) -> (u16, Vec<u8>) {
    let req = match proto::RestockRequest::decode(body) {
        Ok(r) => r,
        Err(e) => return err_body(400, &format!("invalid request: {e}")),
    };
    if req.quantity <= 0 {
        return err_body(400, "quantity must be > 0");
    }

    match database::query(
        "UPDATE inventory SET stock = stock + $2 WHERE product_id = $1 RETURNING stock",
        &[PgValue::Text(req.product_id.clone()), PgValue::Int8(req.quantity)],
    ) {
        Err(e) => err_body(500, &format!("{e:?}")),
        Ok(rows) if rows.is_empty() => {
            err_body(404, &format!("product {} not found", req.product_id))
        }
        Ok(rows) => match &rows[0].columns[0].value {
            PgValue::Int8(new_stock) => (
                200,
                proto::RestockResponse {
                    product_id: req.product_id,
                    new_stock: *new_stock,
                }
                .encode_to_vec(),
            ),
            _ => err_body(500, "unexpected column type"),
        },
    }
}

fn handle_return(body: &[u8]) -> (u16, Vec<u8>) {
    let req = match proto::ReturnRequest::decode(body) {
        Ok(r) => r,
        Err(e) => return err_body(400, &format!("invalid request: {e}")),
    };
    if req.quantity <= 0 {
        return err_body(400, "quantity must be > 0");
    }

    match database::execute(
        "UPDATE inventory SET stock = stock + $2 WHERE product_id = $1",
        &[PgValue::Text(req.product_id.clone()), PgValue::Int8(req.quantity)],
    ) {
        Err(e) => err_body(500, &format!("{e:?}")),
        Ok(0) => err_body(404, &format!("product {} not found", req.product_id)),
        Ok(_) => (
            200,
            proto::ReturnResponse {
                returned: req.quantity,
                product_id: req.product_id,
            }
            .encode_to_vec(),
        ),
    }
}
