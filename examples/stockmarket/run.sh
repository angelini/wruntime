#!/usr/bin/env bash
# Run from the repo root: bash examples/stockmarket/run.sh
# Prerequisites: cargo, cargo-component, rustup target add wasm32-wasip2,
#                Postgres + RustFS S3 running. `just dev-up`
source "$(dirname "$0")/../helpers.sh" "$@"

# ── Kill stale processes from a previous run ─────────────────────────────
kill_stale_ports 9000 9001 9002 9010 9100 9101 9200

echo "DB_URL: ${DB_URL}"
echo "S3_ENDPOINT: ${S3_ENDPOINT}"

# ── Substitute DB and S3 URLs into engine configs ────────────────────────
update_config() {
    local file="$1"
    sed_replace "$file" "postgres://user:pass@localhost:5432/stockmarket" "${DB_URL}"
    sed_replace "$file" "http://127.0.0.1:8900" "${S3_ENDPOINT}"
    sed_replace "$file" "access_key_id     = \"rustfsadmin\"" "access_key_id     = \"${S3_ACCESS_KEY}\""
    sed_replace "$file" "secret_access_key = \"rustfsadmin\"" "secret_access_key = \"${S3_SECRET_KEY}\""
}

cp examples/stockmarket/engine-exchange.toml /tmp/sm-exchange.toml
cp examples/stockmarket/engine-ledger.toml /tmp/sm-ledger.toml
update_config /tmp/sm-exchange.toml
update_config /tmp/sm-ledger.toml

# ── Prepare manager + proxy configs ──────────────────────────────────────
MANAGER_CFG=$(prepare_manager_config)
PROXY_CFG=$(prepare_proxy_config)

# ── Create S3 bucket ─────────────────────────────────────────────────────
create_s3_bucket stockmarket

# ── Clean stale manager state ────────────────────────────────────────────
clean_manager_state

# ── Start manager + proxy ────────────────────────────────────────────────
start_manager_proxy "$MANAGER_CFG" "$PROXY_CFG"

# ── Deploy engines ───────────────────────────────────────────────────────
deploy_engine /tmp/sm-exchange.toml "exchange engine" 9100
deploy_engine /tmp/sm-ledger.toml "ledger engine" 9101
list_services

deploy_engine examples/stockmarket/engine-simulator.toml "simulator engine" 9200
list_services

setup_cleanup_trap

if [ "$INLINE" = true ]; then
    echo "==> Running simulator inline (10 traders, 20 orders each, 5 symbols)..."
    cargo run -p wr-cli -- invoke \
        --proxy http://127.0.0.1:9001 \
        --destination http://stockmarket.simulator/Run \
        --source loadtest --source-ns stockmarket \
        --body '{"num_traders": 10, "orders_per_trader": 20, "num_symbols": 5}'
    exit $?
fi

cat <<'USAGE'

All services running. Press Ctrl-C to stop.
  Manager   : http://127.0.0.1:9000 (gRPC)
  Proxy     : http://127.0.0.1:9001
  Exchange  : http://127.0.0.1:9100 (DB-backed order book)
  Ledger    : http://127.0.0.1:9101 (DB + S3 blobstore)
  Simulator : http://127.0.0.1:9200

Run a simulation (default: 10 traders, 20 orders each, 5 symbols):
  cargo run -p wr-cli -- invoke \
    --proxy http://127.0.0.1:9001 \
    --destination http://stockmarket.simulator/Run \
    --source loadtest --source-ns stockmarket \
    --body ''

Stress test (100 traders, 100 orders each, 10 symbols = 10,000 orders):
  cargo run -p wr-cli -- invoke \
    --proxy http://127.0.0.1:9001 \
    --destination http://stockmarket.simulator/Run \
    --source loadtest --source-ns stockmarket \
    --body '{"num_traders": 100, "orders_per_trader": 100, "num_symbols": 10}'

Inspect metrics:
  cargo run -p wr-cli -- metrics summary
USAGE

wait_forever
