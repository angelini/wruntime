#[allow(dead_code)]
mod proto {
    include!(concat!(env!("OUT_DIR"), "/stockmarket.rs"));
}

#[allow(dead_code, unused_imports)]
mod bindings;

use proto::LedgerServiceClient;
use wr_sdk::bindings::wasi::http::types::{IncomingRequest, ResponseOutparam};
use wr_sdk::bindings::wruntime::db::database::{self, PgValue};
use wr_sdk::io::{read_body, send_response};
use wr_sdk::tracing;
use wr_sdk::ServiceError;

struct Component;
wr_sdk::export!(Component with_types_in wr_sdk::bindings);

impl wr_sdk::ServiceGuest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        let path = request.path_with_query().unwrap_or_default();
        let body = read_body(request.consume().unwrap());
        let (status, resp) = proto::exchange_service_router(&Component, &path, &body);
        send_response(response_out, status, resp);
    }
}

impl proto::ExchangeService for Component {
    fn setup(&self, req: proto::SetupRequest) -> Result<proto::SetupResponse, ServiceError> {
        let sp = tracing::start("exchange.setup", &[]);

        // Tables are created by engine-side migrations; truncate for a clean run.
        let _ = database::execute("TRUNCATE orders, positions", &[]);

        let count = req.symbols.len() as i32;
        tracing::set_attr(&sp, "exchange.symbols", &count.to_string());
        Ok(proto::SetupResponse {
            symbols_created: count,
        })
    }

    fn place_order(
        &self,
        req: proto::PlaceOrderRequest,
    ) -> Result<proto::PlaceOrderResponse, ServiceError> {
        if req.quantity <= 0 {
            return Err(ServiceError::bad_request("quantity must be > 0"));
        }
        if req.price <= 0 {
            return Err(ServiceError::bad_request("price must be > 0"));
        }

        let sp = tracing::start(
            "exchange.place_order",
            &[
                ("order.trader_id", req.trader_id.as_str()),
                ("order.symbol", req.symbol.as_str()),
                ("order.is_buy", if req.is_buy { "true" } else { "false" }),
                ("order.quantity", &req.quantity.to_string()),
                ("order.price", &req.price.to_string()),
            ],
        );

        let tx =
            database::begin_transaction().map_err(|e| ServiceError::internal(format!("{e:?}")))?;

        // Insert the new order.
        let rows = match tx.query(
            "INSERT INTO orders (trader_id, symbol, is_buy, price, quantity, remaining) \
             VALUES ($1, $2, $3, $4, $5, $5) RETURNING order_id",
            &[
                PgValue::Text(req.trader_id.clone()),
                PgValue::Text(req.symbol.clone()),
                PgValue::Boolean(req.is_buy),
                PgValue::Int8(req.price),
                PgValue::Int8(req.quantity),
            ],
        ) {
            Ok(r) => r,
            Err(e) => {
                let _ = tx.rollback();
                return Err(ServiceError::internal(format!("{e:?}")));
            }
        };

        let order_id = match &rows[0].columns[0].value {
            PgValue::Int8(v) => *v,
            _ => {
                let _ = tx.rollback();
                return Err(ServiceError::internal("unexpected order_id type"));
            }
        };

        // Find matching orders on the opposite side.
        // Buy orders match sells at price <= buy price (cheapest first).
        // Sell orders match buys at price >= sell price (most expensive first).
        let (side_filter, order_clause) = if req.is_buy {
            (
                "is_buy = false AND price <= $2",
                "price ASC, created_at ASC",
            )
        } else {
            (
                "is_buy = true AND price >= $2",
                "price DESC, created_at ASC",
            )
        };

        let match_sql = format!(
            "SELECT order_id, trader_id, price, remaining \
             FROM orders \
             WHERE symbol = $1 AND {side_filter} AND remaining > 0 AND order_id != $3 \
             ORDER BY {order_clause} \
             FOR UPDATE"
        );

        let matches = match tx.query(
            &match_sql,
            &[
                PgValue::Text(req.symbol.clone()),
                PgValue::Int8(req.price),
                PgValue::Int8(order_id),
            ],
        ) {
            Ok(r) => r,
            Err(e) => {
                let _ = tx.rollback();
                return Err(ServiceError::internal(format!("{e:?}")));
            }
        };

        let mut total_filled: i64 = 0;
        let mut trades_matched: i32 = 0;
        let mut my_remaining = req.quantity;

        // Collect trade records to send to ledger after commit.
        let mut trade_records: Vec<(String, String, i64, i64, i64)> = Vec::new();

        for row in &matches {
            if my_remaining <= 0 {
                break;
            }

            let match_order_id = match &row.columns[0].value {
                PgValue::Int8(v) => *v,
                _ => continue,
            };
            let match_trader_id = match &row.columns[1].value {
                PgValue::Text(v) => v.clone(),
                _ => continue,
            };
            let match_price = match &row.columns[2].value {
                PgValue::Int8(v) => *v,
                _ => continue,
            };
            let match_remaining = match &row.columns[3].value {
                PgValue::Int8(v) => *v,
                _ => continue,
            };

            let fill_qty = my_remaining.min(match_remaining);
            // Execute at the resting order's price (price-time priority).
            let exec_price = match_price;

            // Update the matched order's remaining quantity.
            if let Err(e) = tx.execute(
                "UPDATE orders SET remaining = remaining - $2 WHERE order_id = $1",
                &[PgValue::Int8(match_order_id), PgValue::Int8(fill_qty)],
            ) {
                let _ = tx.rollback();
                return Err(ServiceError::internal(format!("{e:?}")));
            }

            // Determine buyer and seller.
            let (buyer_id, seller_id) = if req.is_buy {
                (req.trader_id.clone(), match_trader_id.clone())
            } else {
                (match_trader_id.clone(), req.trader_id.clone())
            };

            let cash_amount = fill_qty * exec_price;

            // Update buyer position: +shares, -cash.
            if let Err(e) = tx.execute(
                "INSERT INTO positions (trader_id, symbol, shares, cash_flow) \
                 VALUES ($1, $2, $3, $4) \
                 ON CONFLICT (trader_id, symbol) \
                 DO UPDATE SET shares = positions.shares + $3, cash_flow = positions.cash_flow + $4",
                &[
                    PgValue::Text(buyer_id.clone()),
                    PgValue::Text(req.symbol.clone()),
                    PgValue::Int8(fill_qty),
                    PgValue::Int8(-cash_amount),
                ],
            ) {
                let _ = tx.rollback();
                return Err(ServiceError::internal(format!("{e:?}")));
            }

            // Update seller position: -shares, +cash.
            if let Err(e) = tx.execute(
                "INSERT INTO positions (trader_id, symbol, shares, cash_flow) \
                 VALUES ($1, $2, $3, $4) \
                 ON CONFLICT (trader_id, symbol) \
                 DO UPDATE SET shares = positions.shares + $3, cash_flow = positions.cash_flow + $4",
                &[
                    PgValue::Text(seller_id.clone()),
                    PgValue::Text(req.symbol.clone()),
                    PgValue::Int8(-fill_qty),
                    PgValue::Int8(cash_amount),
                ],
            ) {
                let _ = tx.rollback();
                return Err(ServiceError::internal(format!("{e:?}")));
            }

            trade_records.push((buyer_id, seller_id, fill_qty, exec_price, order_id));
            my_remaining -= fill_qty;
            total_filled += fill_qty;
            trades_matched += 1;
        }

        // Update our order's remaining quantity.
        if let Err(e) = tx.execute(
            "UPDATE orders SET remaining = $2 WHERE order_id = $1",
            &[PgValue::Int8(order_id), PgValue::Int8(my_remaining)],
        ) {
            let _ = tx.rollback();
            return Err(ServiceError::internal(format!("{e:?}")));
        }

        if let Err(e) = tx.commit() {
            return Err(ServiceError::internal(format!("{e:?}")));
        }

        // Record trades on the ledger (after commit, so DB state is consistent).
        let ledger = LedgerServiceClient::new("stockmarket.ledger");
        for (buyer_id, seller_id, qty, price, oid) in &trade_records {
            if let Err(e) = ledger.record_trade(proto::RecordTradeRequest {
                buyer_id: buyer_id.clone(),
                seller_id: seller_id.clone(),
                symbol: req.symbol.clone(),
                quantity: *qty,
                price: *price,
                order_id: *oid,
            }) {
                wr_sdk::log::log(&format!("ledger record_trade error: {e}"));
            }
        }

        tracing::set_attr(&sp, "order.id", &order_id.to_string());
        tracing::set_attr(&sp, "order.trades_matched", &trades_matched.to_string());
        tracing::set_attr(&sp, "order.total_filled", &total_filled.to_string());
        if trades_matched > 0 {
            tracing::record_event(
                &sp,
                "order.matched",
                &[
                    ("trades", &trades_matched.to_string()),
                    ("filled", &total_filled.to_string()),
                ],
            );
        }

        Ok(proto::PlaceOrderResponse {
            order_id,
            trades_matched,
            quantity_filled: total_filled,
            quantity_remaining: my_remaining,
        })
    }

    fn get_order_book(
        &self,
        req: proto::GetOrderBookRequest,
    ) -> Result<proto::GetOrderBookResponse, ServiceError> {
        let bids = database::query(
            "SELECT price, SUM(remaining) as qty \
             FROM orders WHERE symbol = $1 AND is_buy = true AND remaining > 0 \
             GROUP BY price ORDER BY price DESC",
            &[PgValue::Text(req.symbol.clone())],
        )
        .map_err(|e| ServiceError::internal(format!("{e:?}")))?;

        let asks = database::query(
            "SELECT price, SUM(remaining) as qty \
             FROM orders WHERE symbol = $1 AND is_buy = false AND remaining > 0 \
             GROUP BY price ORDER BY price ASC",
            &[PgValue::Text(req.symbol.clone())],
        )
        .map_err(|e| ServiceError::internal(format!("{e:?}")))?;

        fn to_entries(rows: Vec<database::Row>) -> Vec<proto::OrderBookEntry> {
            rows.iter()
                .filter_map(|r| {
                    let price = match &r.columns[0].value {
                        PgValue::Int8(v) => *v,
                        _ => return None,
                    };
                    let quantity = match &r.columns[1].value {
                        PgValue::Int8(v) => *v,
                        _ => return None,
                    };
                    Some(proto::OrderBookEntry { price, quantity })
                })
                .collect()
        }

        Ok(proto::GetOrderBookResponse {
            bids: to_entries(bids),
            asks: to_entries(asks),
        })
    }

    fn get_positions(
        &self,
        _req: proto::GetPositionsRequest,
    ) -> Result<proto::GetPositionsResponse, ServiceError> {
        let rows = database::query(
            "SELECT trader_id, symbol, shares, cash_flow FROM positions ORDER BY trader_id, symbol",
            &[],
        )
        .map_err(|e| ServiceError::internal(format!("{e:?}")))?;

        let positions = rows
            .iter()
            .filter_map(|r| {
                let trader_id = match &r.columns[0].value {
                    PgValue::Text(v) => v.clone(),
                    _ => return None,
                };
                let symbol = match &r.columns[1].value {
                    PgValue::Text(v) => v.clone(),
                    _ => return None,
                };
                let shares = match &r.columns[2].value {
                    PgValue::Int8(v) => *v,
                    _ => return None,
                };
                let cash_flow = match &r.columns[3].value {
                    PgValue::Int8(v) => *v,
                    _ => return None,
                };
                Some(proto::Position {
                    trader_id,
                    symbol,
                    shares,
                    cash_flow,
                })
            })
            .collect();

        Ok(proto::GetPositionsResponse { positions })
    }
}
