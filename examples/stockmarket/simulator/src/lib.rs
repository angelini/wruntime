#[allow(dead_code)]
mod proto {
    include!(concat!(env!("OUT_DIR"), "/stockmarket.rs"));
}

#[allow(dead_code, unused_imports)]
mod bindings;

use prost::Message;
use proto::{ExchangeServiceClient, LedgerServiceClient};
use wr_sdk::bindings::wasi::http::types::{IncomingRequest, Method, ResponseOutparam};
use wr_sdk::io::{err_body, read_body, send_response};
use wr_sdk::tracing;

struct Component;
wr_sdk::export!(Component with_types_in wr_sdk::bindings);

impl wr_sdk::ServiceGuest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        let method = request.method();
        let path = request.path_with_query().unwrap_or_default();
        let body_bytes = read_body(request.consume().unwrap());

        let (status, body) = match (method, path.as_str()) {
            (Method::Post, "/stockmarket.simulator/Run") => handle_run(&body_bytes),
            _ => err_body(404, &format!("no handler for {path}")),
        };

        send_response(response_out, status, body);
    }
}

/// Generate a deterministic symbol name from an index.
fn symbol_name(idx: u32) -> String {
    // Produce 4-letter tickers: AAAA, BBBB, ..., ZZZZ, then AA01, etc.
    let letters = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    let c = letters[(idx as usize) % letters.len()] as char;
    format!("{c}{c}{c}{c}")
}

fn handle_run(body: &[u8]) -> (u16, Vec<u8>) {
    let req = match proto::SimRunRequest::decode(body) {
        Ok(r) => r,
        Err(e) => return err_body(400, &format!("invalid request: {e}")),
    };

    let num_traders = if req.num_traders > 0 {
        req.num_traders
    } else {
        10
    };
    let orders_per_trader = if req.orders_per_trader > 0 {
        req.orders_per_trader
    } else {
        20
    };
    let num_symbols = if req.num_symbols > 0 {
        req.num_symbols
    } else {
        5
    };

    let total_orders = (num_traders as i64) * (orders_per_trader as i64);

    wr_sdk::log::log(&format!(
        "simulator starting — traders={num_traders}, orders_per_trader={orders_per_trader}, \
         symbols={num_symbols}, total_orders={total_orders}"
    ));

    let run_span = tracing::start(
        "simulator.run",
        &[
            ("sim.num_traders", &num_traders.to_string()),
            ("sim.orders_per_trader", &orders_per_trader.to_string()),
            ("sim.num_symbols", &num_symbols.to_string()),
            ("sim.total_orders", &total_orders.to_string()),
        ],
    );

    let exchange = ExchangeServiceClient::new("stockmarket.exchange");
    let ledger = LedgerServiceClient::new("stockmarket.ledger");

    // Setup: reset ledger (truncate trades + delete old snapshots), then setup exchange.
    match ledger.reset(proto::ResetRequest {}) {
        Ok(r) => wr_sdk::log::log(&format!(
            "ledger reset: {} trades deleted, {} snapshots deleted",
            r.trades_deleted, r.snapshots_deleted
        )),
        Err(e) => {
            wr_sdk::log::log(&format!("ledger reset error: {e}"));
            return err_body(500, &format!("ledger reset failed: {e}"));
        }
    }

    let symbols: Vec<String> = (0..num_symbols as u32).map(symbol_name).collect();
    match exchange.setup(proto::SetupRequest {
        symbols: symbols.clone(),
    }) {
        Ok(r) => wr_sdk::log::log(&format!("exchange setup: {} symbols", r.symbols_created)),
        Err(e) => {
            wr_sdk::log::log(&format!("exchange setup error: {e}"));
            return err_body(500, &format!("exchange setup failed: {e}"));
        }
    }

    let mut total_trades: i64 = 0;
    let mut total_volume: i64 = 0;
    let mut errors: i32 = 0;

    // Place orders for each trader.
    for t in 0..num_traders {
        let trader_id = format!("trader-{:04}", t);
        let trader_span = tracing::start("simulator.trader", &[("trader.id", trader_id.as_str())]);

        for o in 0..orders_per_trader {
            // Deterministic pseudo-random parameters.
            let sym_idx = ((t as u64)
                .wrapping_mul(7)
                .wrapping_add(o as u64)
                .wrapping_mul(13)) as u32
                % (num_symbols as u32);
            let symbol = &symbols[sym_idx as usize];

            // 2/3 buys, 1/3 sells — generates more order book depth.
            let is_buy = (o % 3) != 0;

            // Price: 1000-1499 cents ($10.00-$14.99), varies by trader and order.
            let price = 1000
                + ((t as i64)
                    .wrapping_mul(11)
                    .wrapping_add((o as i64).wrapping_mul(17))
                    % 500)
                    .unsigned_abs() as i64;

            // Quantity: 1-10 shares.
            let quantity = 1 + ((o as i64) % 10);

            let order_span = tracing::start(
                "simulator.place_order",
                &[
                    ("order.symbol", symbol.as_str()),
                    ("order.is_buy", if is_buy { "true" } else { "false" }),
                    ("order.price", &price.to_string()),
                    ("order.quantity", &quantity.to_string()),
                ],
            );

            match exchange.place_order(proto::PlaceOrderRequest {
                trader_id: trader_id.clone(),
                symbol: symbol.clone(),
                is_buy,
                quantity,
                price,
            }) {
                Ok(r) => {
                    total_trades += r.trades_matched as i64;
                    // Approximate volume (actual execution prices may differ).
                    total_volume += r.quantity_filled * price;
                    tracing::set_attribute(
                        &order_span,
                        "order.trades_matched",
                        &r.trades_matched.to_string(),
                    );
                }
                Err(e) => {
                    tracing::set_error(&order_span, &e);
                    wr_sdk::log::log(&format!("place_order error: {e}"));
                    errors += 1;
                }
            }
        }

        tracing::set_attribute(
            &trader_span,
            "trader.orders_placed",
            &orders_per_trader.to_string(),
        );
    }

    wr_sdk::log::log(&format!(
        "orders complete — trades={total_trades}, errors={errors}"
    ));

    // Take a final snapshot.
    let snapshot_span = tracing::start("simulator.snapshot", &[]);
    match ledger.snapshot(proto::SnapshotRequest {
        label: "final".to_string(),
    }) {
        Ok(r) => {
            tracing::set_attribute(&snapshot_span, "snapshot.key", &r.snapshot_key);
            tracing::set_attribute(
                &snapshot_span,
                "snapshot.trade_count",
                &r.trade_count.to_string(),
            );
            wr_sdk::log::log(&format!(
                "snapshot taken: key={}, trades={}, bytes={}",
                r.snapshot_key, r.trade_count, r.snapshot_bytes
            ));
        }
        Err(e) => {
            tracing::set_error(&snapshot_span, &e);
            wr_sdk::log::log(&format!("snapshot error: {e}"));
            errors += 1;
        }
    }

    // Verify the ledger.
    let verify_span = tracing::start("simulator.verify", &[]);
    let (ledger_valid, verification_details) = match ledger.verify(proto::VerifyRequest {}) {
        Ok(r) => {
            tracing::set_attribute(
                &verify_span,
                "verify.valid",
                if r.valid { "true" } else { "false" },
            );
            tracing::set_attribute(
                &verify_span,
                "verify.total_trades",
                &r.total_trades.to_string(),
            );
            wr_sdk::log::log(&format!(
                "verification: valid={}, trades={}, volume={}, details={}",
                r.valid, r.total_trades, r.total_volume, r.details
            ));
            // Use the ledger's actual trade count for the response.
            total_trades = r.total_trades;
            total_volume = r.total_volume;
            (r.valid, r.details)
        }
        Err(e) => {
            tracing::set_error(&verify_span, &e);
            wr_sdk::log::log(&format!("verification error: {e}"));
            errors += 1;
            (false, format!("verification call failed: {e}"))
        }
    };

    tracing::set_attribute(&run_span, "sim.total_trades", &total_trades.to_string());
    tracing::set_attribute(&run_span, "sim.total_volume", &total_volume.to_string());
    tracing::set_attribute(&run_span, "sim.errors", &errors.to_string());
    tracing::set_attribute(
        &run_span,
        "sim.ledger_valid",
        if ledger_valid { "true" } else { "false" },
    );

    wr_sdk::log::log(&format!(
        "simulator done — orders={total_orders}, trades={total_trades}, \
         volume={total_volume}, valid={ledger_valid}, errors={errors}"
    ));

    let resp = proto::SimRunResponse {
        total_orders,
        total_trades,
        total_volume,
        ledger_valid,
        verification_details,
        errors,
    };

    if errors == 0 && ledger_valid {
        (200, resp.encode_to_vec())
    } else if !ledger_valid {
        // Return the response body even on failure so the caller can inspect details.
        (500, resp.encode_to_vec())
    } else {
        (500, resp.encode_to_vec())
    }
}
