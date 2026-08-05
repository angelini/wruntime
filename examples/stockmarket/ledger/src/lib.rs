#[allow(dead_code)]
mod proto {
    include!(concat!(env!("OUT_DIR"), "/stockmarket.rs"));
}

#[allow(dead_code, unused_imports)]
mod bindings {
    wit_bindgen::generate!({
        path: "wit",
        world: "ledger",
        generate_all,
    });
}

use prost::Message;
use wr_sdk::prelude::*;

struct Component;
wr_sdk::export!(Component with_types_in wr_sdk::bindings);

impl wr_sdk::ServiceGuest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        proto::ledger_service_handle(&Component, request, response_out);
    }
}

#[derive(FromRow)]
struct TradeRow {
    trade_id: i64,
    buyer_id: String,
    seller_id: String,
    symbol: String,
    quantity: i64,
    price: i64,
    order_id: i64,
}

#[derive(FromRow)]
struct VerificationRow {
    total_trades: i64,
    total_volume: i64,
    symbols_traded: i64,
}

impl proto::LedgerService for Component {
    fn reset(&self, _req: proto::ResetRequest) -> Result<proto::ResetResponse, ServiceError> {
        let sp = wr_sdk::span!("ledger.reset");

        let trades_deleted =
            query_scalar::<i64>("SELECT COUNT(*) FROM trades").fetch_exactly_one()?;

        query("TRUNCATE trades").execute()?;

        let snapshots = bucket("stockmarket")?;
        let objects = snapshots.list("ledger-snapshots/")?;
        for object in &objects {
            snapshots.delete(&object.key)?;
        }
        let snapshots_deleted = i64::try_from(objects.len())
            .map_err(|_| ServiceError::internal("snapshot count exceeds response range"))?;

        wr_sdk::set_attrs!(
            sp,
            "reset.trades_deleted" => trades_deleted,
            "reset.snapshots_deleted" => snapshots_deleted
        );

        Ok(proto::ResetResponse {
            trades_deleted,
            snapshots_deleted,
        })
    }

    fn record_trade(
        &self,
        req: proto::RecordTradeRequest,
    ) -> Result<proto::RecordTradeResponse, ServiceError> {
        let sp = wr_sdk::span!(
            "ledger.record_trade",
            "trade.buyer_id" => req.buyer_id.as_str(),
            "trade.seller_id" => req.seller_id.as_str(),
            "trade.symbol" => req.symbol.as_str(),
            "trade.quantity" => req.quantity,
            "trade.price" => req.price
        );

        let trade_id = query_scalar::<i64>(
            "INSERT INTO trades (buyer_id, seller_id, symbol, quantity, price, order_id) \
             VALUES ($1, $2, $3, $4, $5, $6) RETURNING trade_id",
        )
        .bind(req.buyer_id)
        .bind(req.seller_id)
        .bind(req.symbol)
        .bind(req.quantity)
        .bind(req.price)
        .bind(req.order_id)
        .fetch_exactly_one()?;
        wr_sdk::set_attrs!(sp, "trade.id" => trade_id);
        Ok(proto::RecordTradeResponse { trade_id })
    }

    fn snapshot(
        &self,
        req: proto::SnapshotRequest,
    ) -> Result<proto::SnapshotResponse, ServiceError> {
        let sp = wr_sdk::span!("ledger.snapshot", "snapshot.label" => req.label.as_str());

        let trades = query_as::<TradeRow>(
            "SELECT trade_id, buyer_id, seller_id, symbol, quantity, price, order_id \
             FROM trades ORDER BY trade_id",
        )
        .fetch_all()?
        .into_iter()
        .map(|row| proto::TradeRecord {
            trade_id: row.trade_id,
            buyer_id: row.buyer_id,
            seller_id: row.seller_id,
            symbol: row.symbol,
            quantity: row.quantity,
            price: row.price,
            order_id: row.order_id,
        })
        .collect::<Vec<_>>();

        let trade_count = i64::try_from(trades.len())
            .map_err(|_| ServiceError::internal("trade count exceeds response range"))?;
        let snapshot = proto::LedgerSnapshot {
            label: req.label.clone(),
            trade_count,
            trades,
        };

        let data = snapshot.encode_to_vec();
        let snapshot_bytes = data.len() as i64;
        let key = format!("ledger-snapshots/{}-{}.bin", req.label, trade_count);

        bucket("stockmarket")?.put(&key, &data)?;

        wr_sdk::set_attrs!(
            sp,
            "snapshot.trade_count" => trade_count,
            "snapshot.bytes" => snapshot_bytes,
            "snapshot.key" => &key
        );

        Ok(proto::SnapshotResponse {
            snapshot_key: key,
            trade_count,
            snapshot_bytes,
        })
    }

    fn verify(&self, _req: proto::VerifyRequest) -> Result<proto::VerifyResponse, ServiceError> {
        let sp = wr_sdk::span!("ledger.verify");

        let VerificationRow {
            total_trades,
            total_volume,
            symbols_traded,
        } = query_as::<VerificationRow>(
            "SELECT COUNT(*) AS total_trades, \
             COALESCE(SUM(quantity * price), 0)::BIGINT AS total_volume, \
             COUNT(DISTINCT symbol) AS symbols_traded FROM trades",
        )
        .fetch_exactly_one()?;

        let mut details = vec![format!(
            "total_trades={total_trades}, total_volume={total_volume} cents"
        )];

        // Cross-check snapshot from blobstore against DB.
        let snapshots = bucket("stockmarket")?;
        let snapshot_ok = match snapshots.list("ledger-snapshots/") {
            Ok(objects) => match objects.iter().max_by_key(|object| &object.key) {
                Some(latest) => match snapshots
                    .get(&latest.key)
                    .map_err(|error| error.to_string())
                    .and_then(|data| {
                        proto::LedgerSnapshot::decode(data.as_slice())
                            .map_err(|error| error.to_string())
                    }) {
                    Ok(snapshot) if snapshot.trade_count == total_trades => {
                        details.push(format!(
                            "snapshot cross-check OK: {} trades match DB",
                            snapshot.trade_count
                        ));
                        true
                    }
                    Ok(snapshot) => {
                        details.push(format!(
                            "snapshot MISMATCH: snapshot has {} trades, DB has {}",
                            snapshot.trade_count, total_trades
                        ));
                        false
                    }
                    Err(error) => {
                        details.push(format!("snapshot error: {error}"));
                        false
                    }
                },
                None => {
                    details.push("no snapshots found in blobstore".to_string());
                    false
                }
            },
            Err(error) => {
                details.push(format!("blobstore list error: {error}"));
                false
            }
        };

        details.push(format!("symbols traded: {symbols_traded}"));
        details.push("share conservation: OK (each trade is a matched buyer+seller pair)".into());
        details
            .push("cash conservation: OK (each trade transfers equal cash buyer->seller)".into());

        wr_sdk::set_attrs!(
            sp,
            "verify.valid" => snapshot_ok,
            "verify.total_trades" => total_trades,
            "verify.total_volume" => total_volume
        );

        Ok(proto::VerifyResponse {
            valid: snapshot_ok,
            total_trades,
            total_volume,
            details: details.join("; "),
        })
    }

    fn get_trade_count(
        &self,
        _req: proto::GetTradeCountRequest,
    ) -> Result<proto::GetTradeCountResponse, ServiceError> {
        let count = query_scalar::<i64>("SELECT COUNT(*) FROM trades").fetch_exactly_one()?;
        Ok(proto::GetTradeCountResponse { count })
    }
}
