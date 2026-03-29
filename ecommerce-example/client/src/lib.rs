#[allow(dead_code)]
mod proto {
    include!(concat!(env!("OUT_DIR"), "/ecommerce.rs"));
}

#[allow(dead_code, unused_imports)]
mod bindings;

use proto::InventoryServiceClient;

struct Component;
wr_sdk::export_run!(Component);

impl wr_sdk::RunGuest for Component {
    fn run() {
        wr_sdk::log::log("client starting");

        let client = InventoryServiceClient::new("ecommerce.inventory");

        // Seed inventory — idempotent (ON CONFLICT DO NOTHING in inventory service).
        match client.seed(proto::SeedRequest {}) {
            Ok(_) => wr_sdk::log::log("inventory seeded"),
            Err(e) => wr_sdk::log::log(&format!("seed error: {e}")),
        }

        // Track purchases so we can return some later.
        let mut purchased: Vec<(&str, i64)> = Vec::new();

        for i in 0u64..100 {
            // Deterministic pseudo-random selection spread across all 50 products.
            let product_id = PRODUCTS[((i.wrapping_mul(7).wrapping_add(13)) % 50) as usize];
            let quantity = (i % 5 + 1) as i64;

            match client.buy(proto::BuyRequest {
                product_id: product_id.to_string(),
                quantity,
            }) {
                Ok(r) => {
                    wr_sdk::log::log(&format!(
                        "bought {} x{} — bought={} remaining={}",
                        product_id, quantity, r.bought, r.remaining
                    ));
                    purchased.push((product_id, quantity));
                }
                Err(e) if e.contains("HTTP 409") => {
                    wr_sdk::log::log(&format!("out of stock {} x{}", product_id, quantity));
                }
                Err(e) => {
                    wr_sdk::log::log(&format!("buy error {} x{}: {}", product_id, quantity, e));
                }
            }

            // Return ~30 % of purchases (every 3rd iteration when we have items to return).
            if i % 10 < 3 && !purchased.is_empty() {
                let (ret_id, ret_qty) = purchased.remove(0);

                match client.r#return(proto::ReturnRequest {
                    product_id: ret_id.to_string(),
                    quantity: ret_qty,
                }) {
                    Ok(r) => {
                        wr_sdk::log::log(&format!(
                            "returned {} x{} — returned={} product={}",
                            ret_id, ret_qty, r.returned, r.product_id
                        ));
                    }
                    Err(e) => {
                        wr_sdk::log::log(&format!("return error: {e}"));
                    }
                }
            }
        }

        wr_sdk::log::log("client done");
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
