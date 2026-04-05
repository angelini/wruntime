# Proto-to-Code Generation

Exact mapping from protobuf definitions to generated Rust code.

## WrServiceGenerator output

Given this proto:

```protobuf
syntax = "proto3";
package ecommerce;

service InventoryService {
  rpc Seed     (SeedRequest)     returns (SeedResponse);
  rpc Buy      (BuyRequest)      returns (BuyResponse);
  rpc GetStock (GetStockRequest) returns (GetStockResponse);
  rpc Return   (ReturnRequest)   returns (ReturnResponse);
}
```

Generated code (in `$OUT_DIR/ecommerce.rs`):

```rust
// --- prost generates message structs ---
pub struct SeedRequest { }
pub struct SeedResponse { pub seeded: i32 }
pub struct BuyRequest { pub product_id: String, pub quantity: i64 }
pub struct BuyResponse { pub bought: i64, pub remaining: i64 }
// ... etc

// --- WrServiceGenerator generates ---
pub trait InventoryService {
    fn seed(&self, req: SeedRequest) -> Result<SeedResponse, wr_sdk::ServiceError>;
    fn buy(&self, req: BuyRequest) -> Result<BuyResponse, wr_sdk::ServiceError>;
    fn get_stock(&self, req: GetStockRequest) -> Result<GetStockResponse, wr_sdk::ServiceError>;
    fn r#return(&self, req: ReturnRequest) -> Result<ReturnResponse, wr_sdk::ServiceError>;
    //  ^^ Rust keyword → escaped with r#
}

pub fn inventory_service_router<T: InventoryService>(
    svc: &T,
    path: &str,
    body: &[u8],
) -> (u16, Vec<u8>) {
    match path {
        "/ecommerce.inventory/Seed"     => { /* decode SeedRequest → svc.seed() → encode */ }
        "/ecommerce.inventory/Buy"      => { /* decode BuyRequest → svc.buy() → encode */ }
        "/ecommerce.inventory/GetStock" => { /* ... */ }
        "/ecommerce.inventory/Return"   => { /* ... */ }
        _ => (404, r#"{"error":"no handler for {path}"}"#)
    }
}
```

### Naming rules

| Proto | Generated Rust |
|-------|---------------|
| Service `InventoryService` | Trait: `InventoryService` |
| Service `InventoryService` | Router: `inventory_service_router` |
| Method `GetStock` | Trait method: `get_stock` |
| Method `Return` (keyword) | Trait method: `r#return` |
| Route path | `/{package}.{service_snake}/{ProtoMethodName}` |

**service_snake** = snake_case of service name, with `_service` suffix stripped:
- `InventoryService` → `inventory_service` → strip `_service` → `inventory`
- `OrderService` → `order_service` → strip `_service` → `order`
- `Gateway` → `gateway` (no suffix to strip)

## WrClientGenerator output

Given the same proto, generated code:

```rust
pub struct InventoryServiceClient {
    authority: String,
}

impl InventoryServiceClient {
    pub fn new(authority: impl Into<String>) -> Self {
        Self { authority: authority.into() }
    }

    pub fn seed(&self, req: SeedRequest) -> Result<SeedResponse, wr_sdk::http::HttpError> {
        let body = prost::Message::encode_to_vec(&req);
        let path = format!("/{}/Seed", self.authority);
        wr_sdk::http::http_request(&wr_sdk::http::HttpRequest {
            authority: &self.authority,
            path: &path,
            method: wr_sdk::http::Method::Post,
            headers: &[("content-type", b"application/x-protobuf" as &[u8])],
            body: &body,
        })?
        .error_for_status()?
        .decode()
    }

    pub fn buy(&self, req: BuyRequest) -> Result<BuyResponse, wr_sdk::http::HttpError> { /* same pattern */ }
    pub fn get_stock(&self, req: GetStockRequest) -> Result<GetStockResponse, wr_sdk::http::HttpError> { /* ... */ }
    pub fn r#return(&self, req: ReturnRequest) -> Result<ReturnResponse, wr_sdk::http::HttpError> { /* ... */ }
}
```

### Client naming rules

| Proto | Generated Rust |
|-------|---------------|
| Service `InventoryService` | Struct: `InventoryServiceClient` |
| Method `GetStock` | Method: `get_stock` |
| Constructor authority | `"namespace.module"` (e.g. `"ecommerce.inventory"`) |
| RPC path | `/{authority}/{ProtoMethodName}` (e.g. `/ecommerce.inventory/GetStock`) |

### Error handling difference

| Generator | Return type | Error type |
|-----------|------------|------------|
| `WrServiceGenerator` (trait) | `Result<Response, wr_sdk::ServiceError>` | `ServiceError { status, message }` |
| `WrClientGenerator` (client) | `Result<Response, wr_sdk::http::HttpError>` | `HttpError` enum |

Use `e.is_status(409)` to check for specific HTTP status codes. `HttpError` implements `Display` and `From<HttpError> for ServiceError` enables `?` propagation in handlers.

## WrCombinedGenerator output

Produces both the trait + router AND the client struct for every service in the proto file. Use when a module needs to both serve and consume RPCs.
