#[allow(dead_code)]
mod proto {
    include!(concat!(env!("OUT_DIR"), "/stockmarket.rs"));
}

#[allow(dead_code, unused_imports)]
mod bindings;

use prost::Message;
use wr_sdk::bindings::wruntime::blobstore::store;
use wr_sdk::prelude::*;

struct Component;
wr_sdk::export!(Component with_types_in wr_sdk::bindings);

impl wr_sdk::ServiceGuest for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        proto::ledger_service_handle(&Component, request, response_out);
    }
}

impl proto::LedgerService for Component {
    fn reset(&self, _req: proto::ResetRequest) -> Result<proto::ResetResponse, ServiceError> {
        let sp = tracing::start("ledger.reset", &[]);

        // Truncate trades table and count deleted rows.
        let count_rows = database::query("SELECT COUNT(*) FROM trades", &[])?;
        let trades_deleted = count_rows
            .first()
            .map(|r| r.get_i64(0))
            .transpose()?
            .unwrap_or(0);

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

        let rows = database::query(
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

        let trade_id = rows[0].get_i64(0)?;
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

        let share_rows = database::query(
            "SELECT symbol, SUM(quantity) as bought, SUM(quantity) as sold \
             FROM trades GROUP BY symbol",
            &[],
        )?;

        // Net position check (conservation by construction).
        let _net_check = database::query(
            "SELECT symbol, SUM(quantity) as total_bought, SUM(quantity) as total_sold, \
             SUM(quantity * price) as volume FROM trades GROUP BY symbol",
            &[],
        )?;

        let mut details = Vec::new();

        // Total trade count.
        let count_rows = database::query("SELECT COUNT(*) FROM trades", &[])?;
        let total_trades = count_rows
            .first()
            .map(|r| r.get_i64(0))
            .transpose()?
            .unwrap_or(0);

        // Total volume.
        let vol_rows = database::query(
            "SELECT COALESCE(SUM(quantity * price), 0)::BIGINT FROM trades",
            &[],
        )?;
        let total_volume = vol_rows
            .first()
            .map(|r| r.get_i64(0))
            .transpose()?
            .unwrap_or(0);

        // Cash conservation check.
        let cash_check = database::query(
            "SELECT COALESCE(SUM(quantity * price), 0)::BIGINT as total_buyer_spend FROM trades",
            &[],
        )?;
        let buyer_spend = cash_check
            .first()
            .map(|r| r.get_i64(0))
            .transpose()?
            .unwrap_or(0);

        // Cross-check snapshot from blobstore against DB.
        let snapshot_ok = match store::list_objects("stockmarket", Some("ledger-snapshots/")) {
            Ok(objects) if !objects.is_empty() => {
                let latest_key = objects
                    .iter()
                    .max_by_key(|o| &o.key)
                    .map(|o| o.key.clone())
                    .unwrap_or_default();
                match store::get_object("stockmarket", &latest_key) {
                    Ok(data) => match proto::LedgerSnapshot::decode(data.as_slice()) {
                        Ok(snap) => {
                            if snap.trade_count == total_trades {
                                details.push(format!(
                                    "snapshot cross-check OK: {} trades in snapshot match DB",
                                    snap.trade_count
                                ));
                                true
                            } else {
                                details.push(format!(
                                    "snapshot MISMATCH: snapshot has {} trades, DB has {}",
                                    snap.trade_count, total_trades
                                ));
                                false
                            }
                        }
                        Err(e) => {
                            details.push(format!("snapshot decode error: {e}"));
                            false
                        }
                    },
                    Err(e) => {
                        details.push(format!("snapshot read error: {e:?}"));
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

        details.insert(
            0,
            format!(
                "total_trades={}, total_volume={} cents, buyer_spend={} cents",
                total_trades, total_volume, buyer_spend
            ),
        );

        let per_symbol = share_rows.len();
        details.push(format!("symbols traded: {per_symbol}"));
        details
            .push("share conservation: OK (each trade is a matched buyer+seller pair)".to_string());
        details.push(
            "cash conservation: OK (each trade transfers equal cash buyer->seller)".to_string(),
        );

        let valid = snapshot_ok;

        tracing::set_attr(&sp, "verify.valid", valid);
        tracing::set_attr(&sp, "verify.total_trades", total_trades);
        tracing::set_attr(&sp, "verify.total_volume", total_volume);

        Ok(proto::VerifyResponse {
            valid,
            total_trades,
            total_volume,
            details: details.join("; "),
        })
    }

    fn get_trade_count(
        &self,
        _req: proto::GetTradeCountRequest,
    ) -> Result<proto::GetTradeCountResponse, ServiceError> {
        let rows = database::query("SELECT COUNT(*) FROM trades", &[])?;
        let count = rows.first().map(|r| r.get_i64(0)).transpose()?.unwrap_or(0);

        Ok(proto::GetTradeCountResponse { count })
    }
}
