#!/usr/bin/env bash
# Run from the repo root: bash ecommerce-example/run.sh
# Prerequisites: cargo, cargo-component, rustup target add wasm32-wasip2,
#                Postgres running with an 'ecommerce' database. `just db-start-example`
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

DB_URL="${DB_URL:-${WRUNTIME_EXAMPLE_DB_URL:-postgres://localhost:5432/wruntime_example}}"
echo "DB_URL: ${DB_URL}"

# Substitute the DB URL into the engine configs if DB_URL is set.
update_db_url() {
    local file="$1"
    sed -i.bak "s|postgres://user:pass@localhost:5432/ecommerce|${DB_URL}|g" "$file"
    rm -f "${file}.bak"
}

# ── Build WASM components ──────────────────────────────────────────────────────
echo "==> Building inventory..."
(cd ecommerce-example/inventory && cargo component build --release --target wasm32-wasip2)

echo "==> Building client..."
(cd ecommerce-example/client && cargo component build --release --target wasm32-wasip2)

# ── Build host binaries ────────────────────────────────────────────────────────
echo "==> Building host services..."
cargo build --release -p wr-manager -p wr-proxy -p wr-engine

# ── Apply DB URL to engine configs ────────────────────────────────────────────
cp ecommerce-example/engine-inventory-1.toml /tmp/inv1.toml
cp ecommerce-example/engine-inventory-2.toml /tmp/inv2.toml
update_db_url /tmp/inv1.toml
update_db_url /tmp/inv2.toml

# ── Start manager ──────────────────────────────────────────────────────────────
echo "==> Starting manager on :9000"
./target/release/wr-manager manager.toml &
MANAGER_PID=$!

sleep 1

# ── Start proxy ────────────────────────────────────────────────────────────────
echo "==> Starting proxy on :9001"
./target/release/wr-proxy proxy.toml &
PROXY_PID=$!

sleep 1

# ── Start inventory engines ────────────────────────────────────────────────────
echo "==> Starting inventory engine 1 on :9100"
./target/release/wr-engine /tmp/inv1.toml &
INV1_PID=$!

echo "==> Starting inventory engine 2 on :9101"
./target/release/wr-engine /tmp/inv2.toml &
INV2_PID=$!

# Wait for inventory engines to register with the manager.
echo "==> Waiting for inventory engines to register..."
sleep 3

# ── Seed inventory via the proxy ───────────────────────────────────────────────
# The proxy resolves "inventory.ecommerce" → engine running the inventory module.
echo "==> Seeding inventory..."
curl -sf -X POST http://127.0.0.1:9001/seed \
    -H "x-wr-destination: http://inventory.ecommerce/seed" \
    -H "x-wr-source: bootstrap" \
    -H "x-wr-source-ns: ecommerce" \
    -H "Content-Type: application/json" \
    -d '{}' && echo " OK" || echo " (seed may already exist)"

# ── Start client engines ───────────────────────────────────────────────────────
echo "==> Starting client engine on :9200 (3 concurrent clients)"
./target/release/wr-engine ecommerce-example/engine-client.toml &
CLIENT_PID=$!

echo ""
echo "All services running. Press Ctrl-C to stop."
echo "  Manager  : http://127.0.0.1:9000 (gRPC)"
echo "  Proxy    : http://127.0.0.1:9001"
echo "  Inventory: http://127.0.0.1:9100 + :9101 (2 engines, shared Postgres)"
echo "  Clients  : http://127.0.0.1:9200 (client-a, client-b, client-c)"
echo ""
echo "Check stock: curl http://127.0.0.1:9001/stock/prod-001"
echo "             -H 'x-wr-destination: http://inventory.ecommerce/stock/prod-001'"
echo "             -H 'x-wr-source: cli' -H 'x-wr-source-ns: ecommerce'"

cleanup() {
    echo "==> Shutting down..."
    kill "$CLIENT_PID" "$INV1_PID" "$INV2_PID" "$PROXY_PID" "$MANAGER_PID" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

wait "$CLIENT_PID"
echo "==> All clients finished."
