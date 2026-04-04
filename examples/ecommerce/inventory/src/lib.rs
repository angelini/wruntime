#[allow(dead_code)]
mod proto {
    include!(concat!(env!("OUT_DIR"), "/ecommerce.rs"));
}

// Include generated bindings so the component-type metadata section
// (which declares `export wasi:http/incoming-handler`) is linked in.
#[allow(dead_code, unused_imports)]
mod bindings;

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
        let (status, resp) = proto::inventory_service_router(&Component, &path, &body);
        send_response(response_out, status, resp);
    }
}

impl proto::InventoryService for Component {
    fn seed(&self, _req: proto::SeedRequest) -> Result<proto::SeedResponse, ServiceError> {
        let sp = tracing::start("inventory.seed", &[]);

        // Table is created by engine-side migrations; seed data only.
        for i in 1u32..=50 {
            let id = format!("prod-{:03}", i);
            let name = format!("Product {}", i);
            let _ = database::execute(
                "INSERT INTO inventory (product_id, name, stock) \
                 VALUES ($1, $2, 10000) ON CONFLICT DO NOTHING",
                &[PgValue::Text(id), PgValue::Text(name)],
            );
        }
        tracing::set_attribute(&sp, "inventory.seeded", "50");
        Ok(proto::SeedResponse { seeded: 50 })
    }

    fn get_stock(
        &self,
        req: proto::GetStockRequest,
    ) -> Result<proto::GetStockResponse, ServiceError> {
        let sp = tracing::start(
            "inventory.get_stock",
            &[("product.id", req.product_id.as_str())],
        );

        let rows = database::query(
            "SELECT stock FROM inventory WHERE product_id = $1",
            &[PgValue::Text(req.product_id.clone())],
        )
        .map_err(|e| ServiceError::internal(format!("{e:?}")))?;

        if rows.is_empty() {
            return Err(ServiceError::not_found(format!(
                "product {} not found",
                req.product_id
            )));
        }

        match &rows[0].columns[0].value {
            PgValue::Int8(v) => {
                tracing::set_attribute(&sp, "product.stock", &v.to_string());
                Ok(proto::GetStockResponse {
                    product_id: req.product_id,
                    stock: *v,
                })
            }
            _ => Err(ServiceError::internal("unexpected column type")),
        }
    }

    fn buy(&self, req: proto::BuyRequest) -> Result<proto::BuyResponse, ServiceError> {
        if req.quantity <= 0 {
            return Err(ServiceError::bad_request("quantity must be > 0"));
        }

        let sp = tracing::start(
            "inventory.buy",
            &[
                ("product.id", req.product_id.as_str()),
                ("product.quantity", &req.quantity.to_string()),
            ],
        );

        let tx =
            database::begin_transaction().map_err(|e| ServiceError::internal(format!("{e:?}")))?;

        let rows = match tx.query(
            "SELECT stock FROM inventory WHERE product_id = $1 FOR UPDATE",
            &[PgValue::Text(req.product_id.clone())],
        ) {
            Ok(r) => r,
            Err(e) => {
                let _ = tx.rollback();
                return Err(ServiceError::internal(format!("{e:?}")));
            }
        };

        if rows.is_empty() {
            let _ = tx.rollback();
            return Err(ServiceError::not_found(format!(
                "product {} not found",
                req.product_id
            )));
        }

        let stock = match &rows[0].columns[0].value {
            PgValue::Int8(v) => *v,
            _ => {
                let _ = tx.rollback();
                return Err(ServiceError::internal("unexpected column type"));
            }
        };

        if stock < req.quantity {
            let _ = tx.rollback();
            tracing::set_error(&sp, &format!("insufficient stock — available: {stock}"));
            return Err(ServiceError::conflict(format!(
                "insufficient stock — available: {stock}"
            )));
        }

        if let Err(e) = tx.execute(
            "UPDATE inventory SET stock = stock - $2 WHERE product_id = $1",
            &[
                PgValue::Text(req.product_id.clone()),
                PgValue::Int8(req.quantity),
            ],
        ) {
            let _ = tx.rollback();
            return Err(ServiceError::internal(format!("{e:?}")));
        }

        if let Err(e) = tx.commit() {
            return Err(ServiceError::internal(format!("{e:?}")));
        }

        let remaining = stock - req.quantity;
        tracing::set_attribute(&sp, "product.remaining", &remaining.to_string());
        tracing::record_event(
            &sp,
            "buy.committed",
            &[
                ("product_id", req.product_id.as_str()),
                ("quantity", &req.quantity.to_string()),
            ],
        );
        Ok(proto::BuyResponse {
            bought: req.quantity,
            remaining,
        })
    }

    fn r#return(&self, req: proto::ReturnRequest) -> Result<proto::ReturnResponse, ServiceError> {
        if req.quantity <= 0 {
            return Err(ServiceError::bad_request("quantity must be > 0"));
        }

        tracing::start(
            "inventory.return",
            &[
                ("product.id", req.product_id.as_str()),
                ("product.quantity", &req.quantity.to_string()),
            ],
        );

        let affected = database::execute(
            "UPDATE inventory SET stock = stock + $2 WHERE product_id = $1",
            &[
                PgValue::Text(req.product_id.clone()),
                PgValue::Int8(req.quantity),
            ],
        )
        .map_err(|e| ServiceError::internal(format!("{e:?}")))?;

        if affected == 0 {
            return Err(ServiceError::not_found(format!(
                "product {} not found",
                req.product_id
            )));
        }

        Ok(proto::ReturnResponse {
            returned: req.quantity,
            product_id: req.product_id,
        })
    }

    fn transfer(
        &self,
        req: proto::TransferRequest,
    ) -> Result<proto::TransferResponse, ServiceError> {
        if req.quantity <= 0 {
            return Err(ServiceError::bad_request("quantity must be > 0"));
        }
        if req.from_product_id == req.to_product_id {
            return Err(ServiceError::bad_request(
                "from and to products must differ",
            ));
        }

        let sp = tracing::start(
            "inventory.transfer",
            &[
                ("product.from", req.from_product_id.as_str()),
                ("product.to", req.to_product_id.as_str()),
                ("product.quantity", &req.quantity.to_string()),
            ],
        );

        let tx =
            database::begin_transaction().map_err(|e| ServiceError::internal(format!("{e:?}")))?;

        // Lock both rows in consistent lexicographic order to avoid deadlocks.
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
                    return Err(ServiceError::not_found(format!("product {id} not found")));
                }
                Err(e) => {
                    let _ = tx.rollback();
                    return Err(ServiceError::internal(format!("{e:?}")));
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
                    return Err(ServiceError::internal("unexpected column type"));
                }
            },
            Err(e) => {
                let _ = tx.rollback();
                return Err(ServiceError::internal(format!("{e:?}")));
            }
        };

        if stock_from < req.quantity {
            let _ = tx.rollback();
            tracing::set_error(
                &sp,
                &format!("insufficient stock — available: {stock_from}"),
            );
            return Err(ServiceError::conflict(format!(
                "insufficient stock — available: {stock_from}"
            )));
        }

        if let Err(e) = tx.execute(
            "UPDATE inventory SET stock = stock - $2 WHERE product_id = $1",
            &[
                PgValue::Text(req.from_product_id.clone()),
                PgValue::Int8(req.quantity),
            ],
        ) {
            let _ = tx.rollback();
            return Err(ServiceError::internal(format!("{e:?}")));
        }

        if let Err(e) = tx.execute(
            "UPDATE inventory SET stock = stock + $2 WHERE product_id = $1",
            &[
                PgValue::Text(req.to_product_id.clone()),
                PgValue::Int8(req.quantity),
            ],
        ) {
            let _ = tx.rollback();
            return Err(ServiceError::internal(format!("{e:?}")));
        }

        if let Err(e) = tx.commit() {
            return Err(ServiceError::internal(format!("{e:?}")));
        }

        tracing::record_event(
            &sp,
            "transfer.committed",
            &[
                ("from", req.from_product_id.as_str()),
                ("to", req.to_product_id.as_str()),
                ("quantity", &req.quantity.to_string()),
            ],
        );
        Ok(proto::TransferResponse {
            transferred: req.quantity,
        })
    }

    fn restock(&self, req: proto::RestockRequest) -> Result<proto::RestockResponse, ServiceError> {
        if req.quantity <= 0 {
            return Err(ServiceError::bad_request("quantity must be > 0"));
        }

        let sp = tracing::start(
            "inventory.restock",
            &[
                ("product.id", req.product_id.as_str()),
                ("product.quantity", &req.quantity.to_string()),
            ],
        );

        let rows = database::query(
            "UPDATE inventory SET stock = stock + $2 WHERE product_id = $1 RETURNING stock",
            &[
                PgValue::Text(req.product_id.clone()),
                PgValue::Int8(req.quantity),
            ],
        )
        .map_err(|e| ServiceError::internal(format!("{e:?}")))?;

        if rows.is_empty() {
            return Err(ServiceError::not_found(format!(
                "product {} not found",
                req.product_id
            )));
        }

        match &rows[0].columns[0].value {
            PgValue::Int8(new_stock) => {
                tracing::set_attribute(&sp, "product.new_stock", &new_stock.to_string());
                Ok(proto::RestockResponse {
                    product_id: req.product_id,
                    new_stock: *new_stock,
                })
            }
            _ => Err(ServiceError::internal("unexpected column type")),
        }
    }
}
