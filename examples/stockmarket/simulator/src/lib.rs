#[allow(dead_code)]
mod proto {
    include!(concat!(env!("OUT_DIR"), "/stockmarket.rs"));
}

#[allow(dead_code, unused_imports)]
mod bindings {
    wit_bindgen::generate!({
        path: "wit",
        world: "simulator",
        generate_all,
    });
}

use proto::{ExchangeServiceClient, LedgerServiceClient};
use wr_sdk::prelude::*;

struct Component;
wr_sdk::export!(Component with_types_in wr_sdk::bindings);

impl wr_sdk::ServiceGuest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        proto::simulator_service_handle(&Component, request, response_out);
    }
}

/// Generate a deterministic symbol name from an index.
fn symbol_name(idx: u32) -> String {
    let letters = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    let c = letters[(idx as usize) % letters.len()] as char;
    format!("{c}{c}{c}{c}")
}

impl proto::SimulatorService for Component {
    fn run(&self, req: proto::SimRunRequest) -> Result<proto::SimRunResponse, ServiceError> {
        const MAX_TOTAL_ORDERS: u64 = 1_000_000;
        const MAX_SYMBOLS: u32 = 10_000;

        let num_traders = req.num_traders.max(1);
        let orders_per_trader = req.orders_per_trader.max(1);
        let num_symbols = req.num_symbols.max(1);
        let total_orders_u64 = u64::from(num_traders) * u64::from(orders_per_trader);
        if total_orders_u64 > MAX_TOTAL_ORDERS {
            return Err(ServiceError::bad_request(format!(
                "simulation exceeds {MAX_TOTAL_ORDERS} total orders"
            )));
        }
        if num_symbols > MAX_SYMBOLS {
            return Err(ServiceError::bad_request(format!(
                "simulation exceeds {MAX_SYMBOLS} symbols"
            )));
        }
        let total_orders = i64::try_from(total_orders_u64)
            .map_err(|_| ServiceError::bad_request("total order count exceeds response range"))?;

        let run_span = wr_sdk::span!(
            "simulator.run",
            "sim.num_traders" => num_traders,
            "sim.orders_per_trader" => orders_per_trader,
            "sim.num_symbols" => num_symbols,
            "sim.total_orders" => total_orders
        );

        let exchange = ExchangeServiceClient::new("stockmarket.exchange");
        let ledger = LedgerServiceClient::new("stockmarket.ledger");

        // Setup: reset ledger, then setup exchange symbols.
        ledger
            .reset(proto::ResetRequest {})
            .map_err(|e| ServiceError::internal(format!("ledger reset failed: {e}")))?;

        let symbols: Vec<String> = (0..num_symbols).map(symbol_name).collect();
        exchange
            .setup(proto::SetupRequest {
                symbols: symbols.clone(),
            })
            .map_err(|e| ServiceError::internal(format!("exchange setup failed: {e}")))?;

        let mut errors: u32 = 0;

        // Place orders for each trader.
        for t in 0..num_traders {
            let trader_id = format!("trader-{t:04}");

            for o in 0..orders_per_trader {
                let sym_idx = ((t as u64)
                    .wrapping_mul(7)
                    .wrapping_add(o as u64)
                    .wrapping_mul(13)) as u32
                    % num_symbols;

                let is_buy = (o % 3) != 0;
                let price = 1000
                    + ((t as i64)
                        .wrapping_mul(11)
                        .wrapping_add((o as i64).wrapping_mul(17))
                        % 500)
                        .unsigned_abs() as i64;
                let quantity = 1 + ((o as i64) % 10);

                let _trace = wr_sdk::root_span!(
                    "simulator.place_order",
                    "trader.id" => &trader_id,
                    "order.symbol" => &symbols[sym_idx as usize],
                    "order.side" => if is_buy { "buy" } else { "sell" },
                );
                if let Err(e) = exchange.place_order(proto::PlaceOrderRequest {
                    trader_id: trader_id.clone(),
                    symbol: symbols[sym_idx as usize].clone(),
                    is_buy,
                    quantity,
                    price,
                }) {
                    wr_sdk::log::log(&format!("place_order error: {e}"));
                    errors += 1;
                }
            }
        }

        // Take a final snapshot.
        if let Err(e) = ledger.snapshot(proto::SnapshotRequest {
            label: "final".into(),
        }) {
            wr_sdk::log::log(&format!("snapshot error: {e}"));
            errors += 1;
        }

        // Verify the ledger.
        let (total_trades, total_volume, ledger_valid, verification_details) =
            match ledger.verify(proto::VerifyRequest {}) {
                Ok(r) => (r.total_trades, r.total_volume, r.valid, r.details),
                Err(e) => {
                    errors += 1;
                    (0, 0, false, format!("verification call failed: {e}"))
                }
            };

        tracing::set_attr(&run_span, "sim.total_trades", total_trades);
        tracing::set_attr(&run_span, "sim.total_volume", total_volume);
        tracing::set_attr(&run_span, "sim.errors", errors);
        tracing::set_attr(&run_span, "sim.ledger_valid", ledger_valid);

        Ok(proto::SimRunResponse {
            total_orders,
            total_trades,
            total_volume,
            ledger_valid,
            verification_details,
            errors,
        })
    }
}
