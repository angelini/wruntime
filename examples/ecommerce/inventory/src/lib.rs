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
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        proto::inventory_service_handle(&Component, request, response_out);
    }
}

fn positive_quantity(quantity: u64) -> Result<i64, ServiceError> {
    if quantity == 0 {
        return Err(ServiceError::bad_request("quantity must be > 0"));
    }
    i64::try_from(quantity)
        .map_err(|_| ServiceError::bad_request("quantity exceeds database range"))
}

impl proto::InventoryService for Component {
    fn seed(&self, _req: proto::SeedRequest) -> Result<proto::SeedResponse, ServiceError> {
        let sp = wr_sdk::span!("inventory.seed");

        // Table is created by engine-side migrations; seed data only.
        for i in 1u32..=50 {
            let id = format!("prod-{:03}", i);
            let name = format!("Product {}", i);
            query(
                "INSERT INTO inventory (product_id, name, stock) \
                 VALUES ($1, $2, 10000) ON CONFLICT DO NOTHING",
            )
            .bind(id)
            .bind(name)
            .execute()?;
        }
        wr_sdk::set_attrs!(sp, "inventory.seeded" => 50_i64);
        Ok(proto::SeedResponse { seeded: 50 })
    }

    fn get_stock(
        &self,
        req: proto::GetStockRequest,
    ) -> Result<proto::GetStockResponse, ServiceError> {
        let sp = wr_sdk::span!(
            "inventory.get_stock",
            "product.id" => req.product_id.as_str()
        );

        let stock = query_scalar::<i64>("SELECT stock FROM inventory WHERE product_id = $1")
            .bind(req.product_id.clone())
            .fetch_optional()?
            .ok_or_else(|| {
                ServiceError::not_found(format!("product {} not found", req.product_id))
            })?;
        wr_sdk::set_attrs!(sp, "product.stock" => stock);
        Ok(proto::GetStockResponse {
            product_id: req.product_id,
            stock,
        })
    }

    fn buy(&self, req: proto::BuyRequest) -> Result<proto::BuyResponse, ServiceError> {
        let quantity = positive_quantity(req.quantity)?;

        let sp = wr_sdk::span!("inventory.buy",
            "product.id" => req.product_id.as_str(),
            "product.quantity" => quantity,
        );

        let tx = wr_sdk::db::transaction()?;

        let stock = tx
            .query_scalar::<i64>("SELECT stock FROM inventory WHERE product_id = $1 FOR UPDATE")
            .bind(req.product_id.clone())
            .fetch_optional()?
            .ok_or_else(|| {
                ServiceError::not_found(format!("product {} not found", req.product_id))
            })?;

        if stock < quantity {
            tracing::set_error(&sp, &format!("insufficient stock — available: {stock}"));
            return Err(ServiceError::conflict(format!(
                "insufficient stock — available: {stock}"
            )));
        }

        tx.query("UPDATE inventory SET stock = stock - $2 WHERE product_id = $1")
            .bind(req.product_id.clone())
            .bind(quantity)
            .execute()?;

        tx.commit()?;

        let remaining = stock - quantity;
        wr_sdk::set_attrs!(sp, "product.remaining" => remaining);
        wr_sdk::event!(
            sp,
            "buy.committed",
            "product_id" => req.product_id.as_str(),
            "quantity" => quantity
        );
        Ok(proto::BuyResponse {
            bought: quantity,
            remaining,
        })
    }

    fn r#return(&self, req: proto::ReturnRequest) -> Result<proto::ReturnResponse, ServiceError> {
        let quantity = positive_quantity(req.quantity)?;

        wr_sdk::span!("inventory.return",
            "product.id" => req.product_id.as_str(),
            "product.quantity" => quantity,
        );

        let affected = query("UPDATE inventory SET stock = stock + $2 WHERE product_id = $1")
            .bind(req.product_id.clone())
            .bind(quantity)
            .execute()?;

        if affected == 0 {
            return Err(ServiceError::not_found(format!(
                "product {} not found",
                req.product_id
            )));
        }

        Ok(proto::ReturnResponse {
            returned: quantity,
            product_id: req.product_id,
        })
    }

    fn transfer(
        &self,
        req: proto::TransferRequest,
    ) -> Result<proto::TransferResponse, ServiceError> {
        let quantity = positive_quantity(req.quantity)?;
        if req.from_product_id == req.to_product_id {
            return Err(ServiceError::bad_request(
                "from and to products must differ",
            ));
        }

        let sp = wr_sdk::span!("inventory.transfer",
            "product.from" => req.from_product_id.as_str(),
            "product.to" => req.to_product_id.as_str(),
            "product.quantity" => quantity,
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
            let exists = tx
                .query_scalar::<i32>("SELECT 1 FROM inventory WHERE product_id = $1 FOR UPDATE")
                .bind(id.clone())
                .fetch_optional()?
                .is_some();
            if !exists {
                return Err(ServiceError::not_found(format!("product {id} not found")));
            }
        }

        // Read source stock after both locks are held.
        let stock_from = tx
            .query_scalar::<i64>("SELECT stock FROM inventory WHERE product_id = $1")
            .bind(req.from_product_id.clone())
            .fetch_exactly_one()?;

        if stock_from < quantity {
            tracing::set_error(
                &sp,
                &format!("insufficient stock — available: {stock_from}"),
            );
            return Err(ServiceError::conflict(format!(
                "insufficient stock — available: {stock_from}"
            )));
        }

        tx.query("UPDATE inventory SET stock = stock - $2 WHERE product_id = $1")
            .bind(req.from_product_id.clone())
            .bind(quantity)
            .execute()?;

        tx.query("UPDATE inventory SET stock = stock + $2 WHERE product_id = $1")
            .bind(req.to_product_id.clone())
            .bind(quantity)
            .execute()?;

        tx.commit()?;

        wr_sdk::event!(
            sp,
            "transfer.committed",
            "from" => req.from_product_id.as_str(),
            "to" => req.to_product_id.as_str(),
            "quantity" => quantity
        );
        Ok(proto::TransferResponse {
            transferred: quantity,
        })
    }

    fn restock(&self, req: proto::RestockRequest) -> Result<proto::RestockResponse, ServiceError> {
        let quantity = positive_quantity(req.quantity)?;

        let sp = wr_sdk::span!("inventory.restock",
            "product.id" => req.product_id.as_str(),
            "product.quantity" => quantity,
        );

        let new_stock = query_scalar::<i64>(
            "UPDATE inventory SET stock = stock + $2 WHERE product_id = $1 RETURNING stock",
        )
        .bind(req.product_id.clone())
        .bind(quantity)
        .fetch_optional()?
        .ok_or_else(|| ServiceError::not_found(format!("product {} not found", req.product_id)))?;
        wr_sdk::set_attrs!(sp, "product.new_stock" => new_stock);
        Ok(proto::RestockResponse {
            product_id: req.product_id,
            new_stock,
        })
    }
}
