#[allow(dead_code)]
mod proto {
    include!(concat!(env!("OUT_DIR"), "/ecommerce.rs"));
}

#[allow(dead_code, unused_imports)]
mod bindings {
    wit_bindgen::generate!({
        path: "wit",
        world: "inventory",
        generate_all,
    });
}

use wr_sdk::prelude::*;

struct Component;
wr_sdk::export!(Component with_types_in wr_sdk::bindings);

impl wr_sdk::ServiceGuest for Component {
    fn init() {
        wr_sdk::db::enable_tracing();
    }

    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        proto::inventory_service_handle(&Component, request, response_out);
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
        tracing::set_attr(&sp, "inventory.seeded", "50");
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
        )?;

        if rows.is_empty() {
            return Err(ServiceError::not_found(format!(
                "product {} not found",
                req.product_id
            )));
        }

        let stock = rows[0].get_i64(0)?;
        tracing::set_attr(&sp, "product.stock", stock);
        Ok(proto::GetStockResponse {
            product_id: req.product_id,
            stock,
        })
    }

    fn buy(&self, req: proto::BuyRequest) -> Result<proto::BuyResponse, ServiceError> {
        if req.quantity <= 0 {
            return Err(ServiceError::bad_request("quantity must be > 0"));
        }

        let sp = wr_sdk::span!("inventory.buy",
            "product.id" => req.product_id.as_str(),
            "product.quantity" => req.quantity,
        );

        let tx = wr_sdk::db::transaction()?;

        let rows = tx.query(
            "SELECT stock FROM inventory WHERE product_id = $1 FOR UPDATE",
            &[PgValue::Text(req.product_id.clone())],
        )?;

        if rows.is_empty() {
            return Err(ServiceError::not_found(format!(
                "product {} not found",
                req.product_id
            )));
        }

        let stock = rows[0].get_i64(0)?;

        if stock < req.quantity {
            tracing::set_error(&sp, &format!("insufficient stock — available: {stock}"));
            return Err(ServiceError::conflict(format!(
                "insufficient stock — available: {stock}"
            )));
        }

        tx.execute(
            "UPDATE inventory SET stock = stock - $2 WHERE product_id = $1",
            &[
                PgValue::Text(req.product_id.clone()),
                PgValue::Int8(req.quantity),
            ],
        )?;

        tx.commit()?;

        let remaining = stock - req.quantity;
        tracing::set_attr(&sp, "product.remaining", remaining);
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

        wr_sdk::span!("inventory.return",
            "product.id" => req.product_id.as_str(),
            "product.quantity" => req.quantity,
        );

        let affected = database::execute(
            "UPDATE inventory SET stock = stock + $2 WHERE product_id = $1",
            &[
                PgValue::Text(req.product_id.clone()),
                PgValue::Int8(req.quantity),
            ],
        )?;

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

        let sp = wr_sdk::span!("inventory.transfer",
            "product.from" => req.from_product_id.as_str(),
            "product.to" => req.to_product_id.as_str(),
            "product.quantity" => req.quantity,
        );

        let tx = wr_sdk::db::transaction()?;

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
            let rows = tx.query(
                "SELECT 1 FROM inventory WHERE product_id = $1 FOR UPDATE",
                &[PgValue::Text(id.clone())],
            )?;
            if rows.is_empty() {
                return Err(ServiceError::not_found(format!("product {id} not found")));
            }
        }

        // Read source stock after both locks are held.
        let rows = tx.query(
            "SELECT stock FROM inventory WHERE product_id = $1",
            &[PgValue::Text(req.from_product_id.clone())],
        )?;
        let stock_from = rows
            .first()
            .ok_or_else(|| ServiceError::internal("missing row"))?
            .get_i64(0)?;

        if stock_from < req.quantity {
            tracing::set_error(
                &sp,
                &format!("insufficient stock — available: {stock_from}"),
            );
            return Err(ServiceError::conflict(format!(
                "insufficient stock — available: {stock_from}"
            )));
        }

        tx.execute(
            "UPDATE inventory SET stock = stock - $2 WHERE product_id = $1",
            &[
                PgValue::Text(req.from_product_id.clone()),
                PgValue::Int8(req.quantity),
            ],
        )?;

        tx.execute(
            "UPDATE inventory SET stock = stock + $2 WHERE product_id = $1",
            &[
                PgValue::Text(req.to_product_id.clone()),
                PgValue::Int8(req.quantity),
            ],
        )?;

        tx.commit()?;

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

        let sp = wr_sdk::span!("inventory.restock",
            "product.id" => req.product_id.as_str(),
            "product.quantity" => req.quantity,
        );

        let rows = database::query(
            "UPDATE inventory SET stock = stock + $2 WHERE product_id = $1 RETURNING stock",
            &[
                PgValue::Text(req.product_id.clone()),
                PgValue::Int8(req.quantity),
            ],
        )?;

        if rows.is_empty() {
            return Err(ServiceError::not_found(format!(
                "product {} not found",
                req.product_id
            )));
        }

        let new_stock = rows[0].get_i64(0)?;
        tracing::set_attr(&sp, "product.new_stock", new_stock);
        Ok(proto::RestockResponse {
            product_id: req.product_id,
            new_stock,
        })
    }
}
