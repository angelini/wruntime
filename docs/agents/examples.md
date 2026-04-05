# Worked Examples Index

Real code in the repository demonstrating each pattern. Read these files for concrete usage.

## Handler modules

| Scenario | File | What it demonstrates |
|----------|------|---------------------|
| Full handler with DB, transactions, tracing | `examples/ecommerce/inventory/src/lib.rs` | ServiceGuest, router wiring, database::query/execute, begin_transaction, tx.query/commit/rollback, tracing spans, ServiceError |
| Handler Cargo.toml with all metadata | `examples/ecommerce/inventory/Cargo.toml` | cdylib, component metadata, WIT deps |
| Handler build.rs | `examples/ecommerce/inventory/build.rs` | WrServiceGenerator usage |
| Handler world.wit with DB import | `examples/ecommerce/inventory/wit/world.wit` | Full world definition with wruntime:db |

## Client/runner modules

| Scenario | File | What it demonstrates |
|----------|------|---------------------|
| Client calling another service | `examples/ecommerce/client/src/lib.rs` | ServiceGuest as HTTP-triggered runner, InventoryServiceClient, error handling, tracing |
| Client build.rs | `examples/ecommerce/client/build.rs` | WrClientGenerator, compiling multiple proto files |
| Client world.wit (no DB) | `examples/ecommerce/client/wit/world.wit` | World without database import |

## Host binding test guests

| Capability | File | What it demonstrates |
|-----------|------|---------------------|
| Database CRUD + transactions | `wr-tests/guests/db-guest/src/lib.rs` | query, execute, begin_transaction, streaming cursors |
| Blobstore operations | `wr-tests/guests/blobstore-guest/src/lib.rs` | put_object, get_object, delete_object, list_objects, head_object |
| Outbound HTTP | `wr-tests/guests/http-guest/src/lib.rs` | http_request to other modules |
| Tracing spans | `wr-tests/guests/tracing-guest/src/lib.rs` | start, set_attribute, record_event, set_error |

## Proto schemas

| File | What it demonstrates |
|------|---------------------|
| `examples/ecommerce/schemas/inventory.proto` | Service with multiple RPCs, various message types |
| `examples/ecommerce/schemas/client.proto` | Simple trigger schema (RunRequest/RunResponse) |

## Database migrations

| File | What it demonstrates |
|------|---------------------|
| `examples/ecommerce/inventory/migrations/V1__create_tables.sql` | Single table with CHECK constraint |
| `examples/stockmarket/exchange/migrations/V1__create_tables.sql` | Multiple tables + partial index |
| `examples/stockmarket/ledger/migrations/V1__create_tables.sql` | Simple table with BIGSERIAL PK |

## Engine configs

| File | What it demonstrates |
|------|---------------------|
| `examples/config/engine.toml` | Base engine config with module declaration |
| `examples/ecommerce/engine-inventory-1.toml` | Handler with database = true and migrations_path |
| `examples/ecommerce/engine-client.toml` | Client module without database |
