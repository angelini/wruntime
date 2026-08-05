#[allow(dead_code)]
mod proto {
    include!(concat!(env!("OUT_DIR"), "/stockmarket.rs"));
}

#[allow(dead_code, unused_imports)]
mod bindings {
    wit_bindgen::generate!({
        path: "wit",
        world: "exchange",
        generate_all,
    });
}

use proto::LedgerServiceClient;
use wr_sdk::prelude::*;

struct Component;
wr_sdk::export!(Component with_types_in wr_sdk::bindings);

#[derive(FromRow)]
struct MatchingOrder {
    order_id: i64,
    trader_id: String,
    price: i64,
    remaining: i64,
}

#[derive(FromRow)]
struct OrderBookRow {
    price: i64,
    quantity: i64,
}

#[derive(FromRow)]
struct PositionRow {
    trader_id: String,
    symbol: String,
    shares: i64,
    cash_flow: i64,
}

fn upsert_position(
    tx: &wr_sdk::db::Transaction,
    trader_id: &str,
    symbol: &str,
    shares: i64,
    cash_flow: i64,
) -> Result<(), ServiceError> {
    tx.query(
        "INSERT INTO positions (trader_id, symbol, shares, cash_flow) \
         VALUES ($1, $2, $3, $4) \
         ON CONFLICT (trader_id, symbol) \
         DO UPDATE SET shares = positions.shares + $3, cash_flow = positions.cash_flow + $4",
    )
    .bind(trader_id.to_owned())
    .bind(symbol.to_owned())
    .bind(shares)
    .bind(cash_flow)
    .execute()?;
    Ok(())
}

impl wr_sdk::ServiceGuest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        proto::exchange_service_handle(&Component, request, response_out);
    }
}

impl proto::ExchangeService for Component {
    fn setup(&self, req: proto::SetupRequest) -> Result<proto::SetupResponse, ServiceError> {
        let sp = wr_sdk::span!("exchange.setup");

        // Tables are created by engine-side migrations; truncate for a clean run.
        query("TRUNCATE orders, positions").execute()?;

        let count = i32::try_from(req.symbols.len())
            .map_err(|_| ServiceError::bad_request("too many symbols"))?;
        wr_sdk::set_attrs!(sp, "exchange.symbols" => count);
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

        let sp = wr_sdk::span!(
            "exchange.place_order",
            "order.trader_id" => req.trader_id.as_str(),
            "order.symbol" => req.symbol.as_str(),
            "order.is_buy" => req.is_buy,
            "order.quantity" => req.quantity,
            "order.price" => req.price
        );

        let tx = wr_sdk::db::transaction()?;

        // Insert the new order.
        let order_id = tx
            .query_scalar::<i64>(
                "INSERT INTO orders (trader_id, symbol, is_buy, price, quantity, remaining) \
                 VALUES ($1, $2, $3, $4, $5, $5) RETURNING order_id",
            )
            .bind(req.trader_id.clone())
            .bind(req.symbol.clone())
            .bind(req.is_buy)
            .bind(req.price)
            .bind(req.quantity)
            .fetch_exactly_one()?;

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

        let matches = tx
            .query_as::<MatchingOrder>(&match_sql)
            .bind(req.symbol.clone())
            .bind(req.price)
            .bind(order_id)
            .fetch_all()?;

        let mut total_filled: i64 = 0;
        let mut trades_matched: i32 = 0;
        let mut my_remaining = req.quantity;

        // Collect trade records to send to ledger after commit.
        let mut trade_records: Vec<(String, String, i64, i64, i64)> = Vec::new();

        for row in &matches {
            if my_remaining <= 0 {
                break;
            }

            let match_order_id = row.order_id;
            let match_trader_id = row.trader_id.clone();
            let match_price = row.price;
            let match_remaining = row.remaining;

            let fill_qty = my_remaining.min(match_remaining);
            // Execute at the resting order's price (price-time priority).
            let exec_price = match_price;

            // Update the matched order's remaining quantity.
            tx.query("UPDATE orders SET remaining = remaining - $2 WHERE order_id = $1")
                .bind(match_order_id)
                .bind(fill_qty)
                .execute()?;

            // Determine buyer and seller.
            let (buyer_id, seller_id) = if req.is_buy {
                (req.trader_id.clone(), match_trader_id.clone())
            } else {
                (match_trader_id.clone(), req.trader_id.clone())
            };

            let cash_amount = fill_qty * exec_price;

            upsert_position(&tx, &buyer_id, &req.symbol, fill_qty, -cash_amount)?;
            upsert_position(&tx, &seller_id, &req.symbol, -fill_qty, cash_amount)?;

            trade_records.push((buyer_id, seller_id, fill_qty, exec_price, order_id));
            my_remaining -= fill_qty;
            total_filled += fill_qty;
            trades_matched += 1;
        }

        // Update our order's remaining quantity.
        tx.query("UPDATE orders SET remaining = $2 WHERE order_id = $1")
            .bind(order_id)
            .bind(my_remaining)
            .execute()?;

        tx.commit()?;

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
                wr_sdk::log!("ledger record_trade error: {e}");
            }
        }

        wr_sdk::set_attrs!(
            sp,
            "order.id" => order_id,
            "order.trades_matched" => trades_matched,
            "order.total_filled" => total_filled
        );
        if trades_matched > 0 {
            wr_sdk::event!(
                sp,
                "order.matched",
                "trades" => trades_matched,
                "filled" => total_filled
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
        let bids = query_as::<OrderBookRow>(
            "SELECT price, SUM(remaining)::BIGINT AS quantity \
             FROM orders WHERE symbol = $1 AND is_buy = true AND remaining > 0 \
             GROUP BY price ORDER BY price DESC",
        )
        .bind(req.symbol.clone())
        .fetch_all()?;

        let asks = query_as::<OrderBookRow>(
            "SELECT price, SUM(remaining)::BIGINT AS quantity \
             FROM orders WHERE symbol = $1 AND is_buy = false AND remaining > 0 \
             GROUP BY price ORDER BY price ASC",
        )
        .bind(req.symbol.clone())
        .fetch_all()?;

        fn to_entries(rows: Vec<OrderBookRow>) -> Vec<proto::OrderBookEntry> {
            rows.into_iter()
                .map(|row| proto::OrderBookEntry {
                    price: row.price,
                    quantity: row.quantity,
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
        let positions = query_as::<PositionRow>(
            "SELECT trader_id, symbol, shares, cash_flow \
             FROM positions ORDER BY trader_id, symbol",
        )
        .fetch_all()?
        .into_iter()
        .map(|row| proto::Position {
            trader_id: row.trader_id,
            symbol: row.symbol,
            shares: row.shares,
            cash_flow: row.cash_flow,
        })
        .collect();

        Ok(proto::GetPositionsResponse { positions })
    }
}
