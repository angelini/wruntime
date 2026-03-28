# Ecommerce Example Plan

Two WASM services (`inventory`, `client`) wired together via wr-proxy, with a shell script to spin everything up.

---

## Directory layout

```
ecommerce-example/
├── inventory/                  # WASM HTTP handler service
│   ├── Cargo.toml              # cargo-component crate
│   ├── wit/
│   │   └── world.wit           # imports wruntime:db/database, exports wasi:http/incoming-handler
│   └── src/
│       └── lib.rs
├── client/                     # Long-running WASM task
│   ├── Cargo.toml              # cargo-component crate
│   ├── wit/
│   │   └── world.wit           # imports wasi:http/outgoing-handler, exports run
│   └── src/
│       └── lib.rs
├── engine-inventory.toml       # Engine config for inventory instances
├── engine-client.toml          # Engine config for client instances
└── run.sh                      # Orchestration script
```

---

## `inventory` service

**WIT world** — imports `wruntime:db@0.2.0/database`, exports the WASI HTTP proxy handler:

```wit
package ecommerce:inventory@0.1.0;

world inventory {
    import wruntime:db/database@0.2.0;
    include wasi:http/proxy@0.2.0;
}
```

**HTTP API** (no proto schema — plain JSON bodies, schema validation skipped):

| Method | Path | Body / Response |
|--------|------|-----------------|
| `POST` | `/seed` | Seeds N products with initial stock |
| `GET`  | `/stock/{id}` | `{"product_id": "...", "stock": 42}` |
| `POST` | `/buy` | `{"product_id": "...", "quantity": N}` → 200 or 409 |
| `POST` | `/return` | `{"product_id": "...", "quantity": N}` → 200 |

**Database schema** (created on first request if absent):

```sql
CREATE TABLE IF NOT EXISTS inventory (
    product_id TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    stock      BIGINT NOT NULL CHECK (stock >= 0)
);
```

**Stock enforcement** — `/buy` uses a serialisable transaction to ensure stock never goes negative:

```rust
// begin-transaction → SELECT stock ... FOR UPDATE → check → UPDATE → commit
// If stock < quantity: rollback, return 409 Conflict
```

The `CHECK (stock >= 0)` constraint acts as a final safety net; the transaction-level check provides a clean 409 before the DB ever sees a violation.

**Seeding** — `POST /seed` inserts 50 products (`prod-001` … `prod-050`) each with 10 000 units. Uses `INSERT … ON CONFLICT DO NOTHING` so re-running is idempotent.

---

## `client` service

**WIT world** — standard WASI HTTP outgoing handler, exports `run`:

```wit
package ecommerce:client@0.1.0;

world client {
    include wasi:http/proxy@0.2.0;
    export run: func();
}
```

**Behaviour** (`run` export — the engine calls this once, it runs until done):

1. Pick 5 random products from a hard-coded list (`prod-001` … `prod-050`).
2. Loop 100 iterations:
   - Choose a random product and a quantity (1–5).
   - `POST http://inventory/buy` with `{"product_id": "...", "quantity": N}`.
   - 30 % of the time, immediately `POST http://inventory/return` for the same quantity (simulates a return).
3. Log each outcome (200 = success, 409 = out of stock, surfaced via `eprintln!` → WASI stdio).

The host rewrites `http://inventory/buy` to `http://wr-proxy:9001/buy` and injects `x-wr-destination: http://inventory/buy`, which the proxy uses to resolve the engine.

---

## Engine configs

### `engine-inventory.toml`

```toml
listen_address  = "0.0.0.0:9100"
manager_address = "http://127.0.0.1:9000"
proxy_address   = "http://127.0.0.1:9001"

[database]
url             = "postgres://user:pass@localhost:5432/ecommerce"
max_connections = 8

[[module]]
name      = "inventory"
namespace = "ecommerce"
version   = "1.0.0"
wasm_path = "inventory/target/wasm32-wasip2/release/inventory.wasm"
database  = true
```

A second inventory instance runs on port 9101 with the same config (different `listen_address`). The manager load-balances between them.

### `engine-client.toml`

```toml
listen_address  = "0.0.0.0:9200"
manager_address = "http://127.0.0.1:9000"
proxy_address   = "http://127.0.0.1:9001"

[[module]]
name      = "client"
namespace = "ecommerce"
version   = "1.0.0"
wasm_path = "client/target/wasm32-wasip2/release/client.wasm"
```

Multiple client instances (e.g. `client-a`, `client-b`, `client-c`) declared as separate `[[module]]` blocks — each is an independent `run` task.

---

## `run.sh` orchestration script

```bash
#!/usr/bin/env bash
set -euo pipefail

# 1. Build both WASM components
(cd inventory && cargo component build --release)
(cd client    && cargo component build --release)

# 2. Start manager and proxy (from repo root)
cargo run -p wr-manager -- --config manager.toml &  MANAGER_PID=$!
cargo run -p wr-proxy   -- --config proxy.toml   &  PROXY_PID=$!
sleep 1   # wait for gRPC + HTTP listeners

# 3. Seed inventory via a direct HTTP call once engine-inventory is up
cargo run -p wr-engine -- --config ecommerce-example/engine-inventory.toml &  ENG_INV_PID=$!
sleep 2
curl -sf -X POST http://127.0.0.1:9100/inventory/seed

# 4. Start a second inventory engine instance
cargo run -p wr-engine -- \
  --config ecommerce-example/engine-inventory.toml \
  --listen 0.0.0.0:9101 &  ENG_INV2_PID=$!

# 5. Start client engines (each declares 3 client module instances)
cargo run -p wr-engine -- --config ecommerce-example/engine-client.toml &  ENG_CLI_PID=$!

trap "kill $MANAGER_PID $PROXY_PID $ENG_INV_PID $ENG_INV2_PID $ENG_CLI_PID 2>/dev/null" EXIT
wait
```

> The script assumes `manager.toml` / `proxy.toml` already exist in the repo root. It does not require any extra tooling beyond `cargo`, `cargo-component`, and a running Postgres instance.

---

## Build prerequisites

- `cargo-component` (`cargo install cargo-component`)
- `wasm-tools` (`cargo install wasm-tools`)
- `wasm32-wasip2` target (`rustup target add wasm32-wasip2`)
- Postgres running locally with an `ecommerce` database

---

## Key design decisions

| Decision | Rationale |
|----------|-----------|
| Transaction + `FOR UPDATE` in `/buy` | Prevents race conditions when multiple client instances hit the same product concurrently |
| `CHECK (stock >= 0)` DB constraint | Database-level guard; should never fire if transaction logic is correct |
| No proto schema on either module | Simplifies the example — bodies are plain JSON, schema validation skipped by proxy |
| `client` uses `run` export | Makes it a fire-and-forget long-running task; no HTTP listener needed |
| Multiple `[[module]]` blocks in client engine | Cheap way to run N concurrent client workloads without separate engine processes |
| Seed via `POST /seed` on engine startup | Idempotent (`ON CONFLICT DO NOTHING`) — safe to call multiple times |
