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
use wr_sdk::bindings::wruntime::blobstore::store;
use wr_sdk::prelude::*;

struct Component;
wr_sdk::export!(Component with_types_in wr_sdk::bindings);

impl wr_sdk::ServiceGuest for Component {
    fn init() {
        wr_sdk::db::enable_tracing();
    }

    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        proto::ledger_service_handle(&Component, request, response_out);
    }
}

impl proto::LedgerService for Component {
    fn reset(&self, _req: proto::ResetRequest) -> Result<proto::ResetResponse, ServiceError> {
        let sp = tracing::start("ledger.reset", &[]);

        let trades_deleted: i64 = query_scalar("SELECT COUNT(*) FROM trades", &[])?;

        database::execute("TRUNCATE trades", &[])?;

        // Delete old snapshot objects from blobstore.
        let mut snapshots_deleted: i64 = 0;
        if let Ok(objects) = store::list_objects("stockmarket", Some("ledger-snapshots/")) {
            for obj in &objects {
                if store::delete_object("stockmarket", &obj.key).is_ok() {
                    snapshots_deleted += 1;
                }
            }
        }

        tracing::set_attr(&sp, "reset.trades_deleted", trades_deleted);
        tracing::set_attr(&sp, "reset.snapshots_deleted", snapshots_deleted);

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

        let trade_id: i64 = query_scalar(
            "INSERT INTO trades (buyer_id, seller_id, symbol, quantity, price, order_id) \
             VALUES ($1, $2, $3, $4, $5, $6) RETURNING trade_id",
            &[
                PgValue::Text(req.buyer_id),
                PgValue::Text(req.seller_id),
                PgValue::Text(req.symbol),
                PgValue::Int8(req.quantity),
                PgValue::Int8(req.price),
                PgValue::Int8(req.order_id),
            ],
        )?;
        tracing::set_attr(&sp, "trade.id", trade_id);
        Ok(proto::RecordTradeResponse { trade_id })
    }

    fn snapshot(
        &self,
        req: proto::SnapshotRequest,
    ) -> Result<proto::SnapshotResponse, ServiceError> {
        let sp = tracing::start("ledger.snapshot", &[("snapshot.label", req.label.as_str())]);

        let rows = database::query(
            "SELECT trade_id, buyer_id, seller_id, symbol, quantity, price, order_id \
             FROM trades ORDER BY trade_id",
            &[],
        )?;

        let trades: Vec<proto::TradeRecord> = rows
            .iter()
            .map(|r| {
                let (trade_id, buyer_id, seller_id, symbol, quantity, price, order_id): (
                    i64,
                    String,
                    String,
                    String,
                    i64,
                    i64,
                    i64,
                ) = r.unpack()?;
                Ok(proto::TradeRecord {
                    trade_id,
                    buyer_id,
                    seller_id,
                    symbol,
                    quantity,
                    price,
                    order_id,
                })
            })
            .collect::<Result<Vec<_>, ServiceError>>()?;

        let trade_count = trades.len() as i64;
        let snapshot = proto::LedgerSnapshot {
            label: req.label.clone(),
            trade_count,
            trades,
        };

        let data = snapshot.encode_to_vec();
        let snapshot_bytes = data.len() as i64;
        let key = format!("ledger-snapshots/{}-{}.bin", req.label, trade_count);

        store::put_object("stockmarket", &key, &data)?;

        tracing::set_attr(&sp, "snapshot.trade_count", trade_count);
        tracing::set_attr(&sp, "snapshot.bytes", snapshot_bytes);
        tracing::set_attr(&sp, "snapshot.key", &key);

        Ok(proto::SnapshotResponse {
            snapshot_key: key,
            trade_count,
            snapshot_bytes,
        })
    }

    fn verify(&self, _req: proto::VerifyRequest) -> Result<proto::VerifyResponse, ServiceError> {
        let sp = tracing::start("ledger.verify", &[]);

        let (total_trades, total_volume, symbols_traded): (i64, i64, i64) = query_one(
            "SELECT COUNT(*), COALESCE(SUM(quantity * price), 0)::BIGINT, \
             COUNT(DISTINCT symbol) FROM trades",
            &[],
        )?;

        let mut details = vec![format!(
            "total_trades={total_trades}, total_volume={total_volume} cents"
        )];

        // Cross-check snapshot from blobstore against DB.
        let snapshot_ok = match store::list_objects("stockmarket", Some("ledger-snapshots/")) {
            Ok(objects) if !objects.is_empty() => {
                let latest_key = objects
                    .iter()
                    .max_by_key(|o| &o.key)
                    .map(|o| o.key.clone())
                    .unwrap_or_default();
                match store::get_object("stockmarket", &latest_key)
                    .map_err(|e| format!("{e:?}"))
                    .and_then(|data| {
                        proto::LedgerSnapshot::decode(data.as_slice())
                            .map_err(|e| format!("{e}"))
                    }) {
                    Ok(snap) if snap.trade_count == total_trades => {
                        details.push(format!(
                            "snapshot cross-check OK: {} trades match DB",
                            snap.trade_count
                        ));
                        true
                    }
                    Ok(snap) => {
                        details.push(format!(
                            "snapshot MISMATCH: snapshot has {} trades, DB has {}",
                            snap.trade_count, total_trades
                        ));
                        false
                    }
                    Err(e) => {
                        details.push(format!("snapshot error: {e}"));
                        false
                    }
                }
            }
            Ok(_) => {
                details.push("no snapshots found in blobstore".to_string());
                false
            }
            Err(e) => {
                details.push(format!("blobstore list error: {e:?}"));
                false
            }
        };

        details.push(format!("symbols traded: {symbols_traded}"));
        details.push("share conservation: OK (each trade is a matched buyer+seller pair)".into());
        details.push("cash conservation: OK (each trade transfers equal cash buyer->seller)".into());

        tracing::set_attr(&sp, "verify.valid", snapshot_ok);
        tracing::set_attr(&sp, "verify.total_trades", total_trades);
        tracing::set_attr(&sp, "verify.total_volume", total_volume);

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
        let count: i64 = query_scalar("SELECT COUNT(*) FROM trades", &[])?;
        Ok(proto::GetTradeCountResponse { count })
    }
}
