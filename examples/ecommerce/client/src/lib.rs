#[allow(dead_code)]
mod proto {
    include!(concat!(env!("OUT_DIR"), "/ecommerce.rs"));
}

#[allow(dead_code, unused_imports)]
mod bindings {
    wit_bindgen::generate!({
        path: "wit",
        world: "client",
        generate_all,
    });
}

use proto::InventoryServiceClient;
use wr_sdk::prelude::*;

struct Component;
wr_sdk::export!(Component with_types_in wr_sdk::bindings);

impl wr_sdk::ServiceGuest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        proto::client_service_handle(&Component, request, response_out);
    }
}

impl proto::ClientService for Component {
    fn run(&self, req: proto::RunRequest) -> Result<proto::RunResponse, ServiceError> {
        let count = if req.count == 0 { 100 } else { req.count };
        let count_attr = i64::try_from(count)
            .map_err(|_| ServiceError::bad_request("count exceeds tracing range"))?;

        wr_sdk::log!("client starting — {count} iterations");

        let run_span = wr_sdk::span!("client.run", "client.count" => count_attr);

        let inv = InventoryServiceClient::new("ecommerce.inventory");

        match inv.seed(proto::SeedRequest {}) {
            Ok(_) => wr_sdk::log!("inventory seeded"),
            Err(e) => wr_sdk::log!("seed error: {e}"),
        }

        let mut purchased: Vec<(String, u64)> = Vec::new();
        let mut completed: u64 = 0;
        let mut errors: Vec<String> = Vec::new();

        for i in 0u64..count {
            // Spread load evenly across all 50 products via a cheap hash.
            let idx = ((i.wrapping_mul(7).wrapping_add(13)) % 50) as usize;
            let product_id = PRODUCTS[idx].to_string();
            let quantity = i % 5 + 1;
            let quantity_attr = i64::try_from(quantity).expect("quantity is in 1..=5");

            // Rotate through all 6 actions so every operation type gets exercised.
            match i % 6 {
                // 0, 3 — Buy
                0 | 3 => {
                    let sp = wr_sdk::span!("client.buy", "product.id" => &product_id, "product.quantity" => quantity_attr);
                    match inv.buy(proto::BuyRequest {
                        product_id: product_id.clone(),
                        quantity,
                    }) {
                        Ok(r) => {
                            wr_sdk::set_attrs!(sp, "product.remaining" => r.remaining);
                            wr_sdk::log!(
                                "bought {} x{} — remaining={}",
                                product_id,
                                quantity,
                                r.remaining
                            );
                            purchased.push((product_id, quantity));
                        }
                        Err(ref e) if e.is_status(409) => {
                            tracing::set_error(&sp, "out of stock");
                            wr_sdk::log!("out of stock {} x{}", product_id, quantity);
                        }
                        Err(e) => {
                            tracing::set_error(&sp, &format!("{e}"));
                            wr_sdk::log!("buy error: {e}");
                            errors.push(format!("buy {product_id}: {e}"));
                        }
                    }
                }
                // 1 — Return a previously purchased item
                1 => {
                    if let Some((ret_id, ret_qty)) = purchased.pop() {
                        let sp = wr_sdk::span!("client.return", "product.id" => ret_id.as_str(), "product.quantity" => i64::try_from(ret_qty).expect("returned quantity is in 1..=5"));
                        match inv.r#return(proto::ReturnRequest {
                            product_id: ret_id.clone(),
                            quantity: ret_qty,
                        }) {
                            Ok(r) => {
                                wr_sdk::log!(
                                    "returned {} x{} — product={}",
                                    ret_id,
                                    ret_qty,
                                    r.product_id
                                );
                            }
                            Err(e) => {
                                tracing::set_error(&sp, &format!("{e}"));
                                wr_sdk::log!("return error: {e}");
                                errors.push(format!("return {ret_id}: {e}"));
                            }
                        }
                    }
                }
                // 2 — GetStock (read-only, high-frequency)
                2 => {
                    let sp = wr_sdk::span!("client.get_stock", "product.id" => &product_id);
                    match inv.get_stock(proto::GetStockRequest {
                        product_id: product_id.clone(),
                    }) {
                        Ok(r) => {
                            wr_sdk::set_attrs!(sp, "product.stock" => r.stock);
                            wr_sdk::log!("stock {} = {}", product_id, r.stock);
                        }
                        Err(e) => {
                            tracing::set_error(&sp, &format!("{e}"));
                            wr_sdk::log!("get_stock error: {e}");
                            errors.push(format!("get_stock {product_id}: {e}"));
                        }
                    }
                }
                // 4 — Transfer between two products
                4 => {
                    let to_idx = ((i.wrapping_mul(11).wrapping_add(3)) % 50) as usize;
                    let to_product_id = PRODUCTS[to_idx].to_string();
                    if product_id != to_product_id {
                        let sp = wr_sdk::span!("client.transfer", "product.from" => &product_id, "product.to" => &to_product_id, "product.quantity" => quantity_attr);
                        match inv.transfer(proto::TransferRequest {
                            from_product_id: product_id.clone(),
                            to_product_id: to_product_id.clone(),
                            quantity,
                        }) {
                            Ok(r) => {
                                wr_sdk::set_attrs!(sp, "product.transferred" => r.transferred);
                                wr_sdk::log!(
                                    "transferred {} → {} x{}",
                                    product_id,
                                    to_product_id,
                                    r.transferred
                                );
                            }
                            Err(ref e) if e.is_status(409) => {
                                tracing::set_error(&sp, "insufficient stock");
                                wr_sdk::log!(
                                    "transfer insufficient stock {} → {}",
                                    product_id,
                                    to_product_id
                                );
                            }
                            Err(e) => {
                                tracing::set_error(&sp, &format!("{e}"));
                                wr_sdk::log!("transfer error: {e}");
                                errors
                                    .push(format!("transfer {product_id} → {to_product_id}: {e}"));
                            }
                        }
                    }
                }
                // 5 — Restock
                _ => {
                    let sp = wr_sdk::span!("client.restock", "product.id" => &product_id, "product.quantity" => quantity_attr * 10);
                    match inv.restock(proto::RestockRequest {
                        product_id: product_id.clone(),
                        quantity: quantity * 10,
                    }) {
                        Ok(r) => {
                            wr_sdk::set_attrs!(sp, "product.new_stock" => r.new_stock);
                            wr_sdk::log!("restocked {} — new_stock={}", product_id, r.new_stock);
                        }
                        Err(e) => {
                            tracing::set_error(&sp, &format!("{e}"));
                            wr_sdk::log!("restock error: {e}");
                            errors.push(format!("restock {product_id}: {e}"));
                        }
                    }
                }
            }

            completed += 1;
        }

        wr_sdk::set_attrs!(
            run_span,
            "client.completed" => i64::try_from(completed)
                .expect("completed is bounded by validated count"),
            "client.errors" => i64::try_from(errors.len())
                .expect("error count is bounded by validated count")
        );
        wr_sdk::log!(
            "client done — {completed} operations, {} errors",
            errors.len()
        );

        if errors.is_empty() {
            Ok(proto::RunResponse { completed })
        } else {
            Err(ServiceError::internal(format!(
                "{} operation(s) failed: {}",
                errors.len(),
                errors.join("; ")
            )))
        }
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
