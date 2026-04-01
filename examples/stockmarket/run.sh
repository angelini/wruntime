#!/usr/bin/env bash
# Run from the repo root: bash examples/stockmarket/run.sh
# Prerequisites: cargo, cargo-component, rustup target add wasm32-wasip2,
#                Postgres + RustFS S3 running. `just dev-up`
set -euo pipefail

INLINE=false
for arg in "$@"; do
    case "$arg" in
        --inline) INLINE=true ;;
    esac
done

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"

DB_URL="${DB_URL:-${WRT_EXAMPLE_DB_URL:-postgres://postgres@localhost:5433/wruntime_example}}"
S3_ENDPOINT="${S3_ENDPOINT:-http://localhost:8900}"
S3_ACCESS_KEY="${S3_ACCESS_KEY:-rustfsadmin}"
S3_SECRET_KEY="${S3_SECRET_KEY:-rustfsadmin}"
echo "DB_URL: ${DB_URL}"
echo "S3_ENDPOINT: ${S3_ENDPOINT}"

# ── Tracing (OpenTelemetry → Grafana LGTM) ────────────────────────────────────
export RUST_LOG="${RUST_LOG:-info}"

# ── Substitute DB and S3 URLs into engine configs ─────────────────────────────
update_config() {
    local file="$1"
    sed -i.bak "s|postgres://user:pass@localhost:5432/stockmarket|${DB_URL}|g" "$file"
    sed -i.bak "s|http://127.0.0.1:8900|${S3_ENDPOINT}|g" "$file"
    sed -i.bak "s|access_key_id     = \"rustfsadmin\"|access_key_id     = \"${S3_ACCESS_KEY}\"|g" "$file"
    sed -i.bak "s|secret_access_key = \"rustfsadmin\"|secret_access_key = \"${S3_SECRET_KEY}\"|g" "$file"
    rm -f "${file}.bak"
}

cp examples/stockmarket/engine-exchange.toml /tmp/sm-exchange.toml
cp examples/stockmarket/engine-ledger.toml /tmp/sm-ledger.toml
update_config /tmp/sm-exchange.toml
update_config /tmp/sm-ledger.toml

cp examples/config/manager.toml /tmp/wr-manager.toml
sed -i.bak "s|postgres://postgres@localhost:5433/wruntime_example|${DB_URL}|g" /tmp/wr-manager.toml
rm -f /tmp/wr-manager.toml.bak

# ── Create S3 bucket ──────────────────────────────────────────────────────────
echo "==> Creating S3 bucket 'stockmarket'"
AWS_ACCESS_KEY_ID="${S3_ACCESS_KEY}" AWS_SECRET_ACCESS_KEY="${S3_SECRET_KEY}" \
    aws --endpoint-url "${S3_ENDPOINT}" s3 mb s3://stockmarket 2>/dev/null || true

# ── Clean stale manager state ─────────────────────────────────────────────────
echo "==> Cleaning manager state..."
psql "${DB_URL}" -c "TRUNCATE wr_engines, wr_routing_rules, wr_schemas CASCADE" 2>/dev/null \
    || echo "   (tables may not exist yet — first run)"

# ── Start manager ──────────────────────────────────────────────────────────────
echo "==> Starting manager on :9000"
./target/debug/wr-manager /tmp/wr-manager.toml &
MANAGER_PID=$!
sleep 1

# ── Start proxy ────────────────────────────────────────────────────────────────
echo "==> Starting proxy on :9001"
./target/debug/wr-proxy examples/config/proxy.toml &
PROXY_PID=$!
sleep 1

# ── Start exchange engine ─────────────────────────────────────────────────────
echo "==> Starting exchange engine on :9100"
./target/debug/wr-engine /tmp/sm-exchange.toml &
EXCHANGE_PID=$!

# ── Start ledger engine ───────────────────────────────────────────────────────
echo "==> Starting ledger engine on :9101"
./target/debug/wr-engine /tmp/sm-ledger.toml &
LEDGER_PID=$!

echo "==> Waiting for engines to register..."
sleep 3

cargo run -p wr-cli -- engines list
cargo run -p wr-cli -- services list

# ── Start simulator engine ────────────────────────────────────────────────────
echo "==> Starting simulator engine on :9200"
./target/debug/wr-engine examples/stockmarket/engine-simulator.toml &
SIMULATOR_PID=$!
sleep 2

cargo run -p wr-cli -- engines list
cargo run -p wr-cli -- services list

cleanup() {
    echo "==> Shutting down..."
    kill -INT "$SIMULATOR_PID" "$EXCHANGE_PID" "$LEDGER_PID" 2>/dev/null || true
    wait "$SIMULATOR_PID" "$EXCHANGE_PID" "$LEDGER_PID" 2>/dev/null || true
    kill -INT "$PROXY_PID" "$MANAGER_PID" 2>/dev/null || true
    sleep 5
}
trap cleanup EXIT
trap 'exit 0' INT TERM

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

wait
