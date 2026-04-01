#!/usr/bin/env bash
# Run from the repo root: bash examples/ecommerce/run.sh
# Prerequisites: cargo, cargo-component, rustup target add wasm32-wasip2,
#                Postgres running with an 'ecommerce' database. `just dev-up`
set -euo pipefail

INLINE=false
for arg in "$@"; do
    case "$arg" in
        --inline) INLINE=true ;;
    esac
done

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"

DB_URL="${DB_URL:-${WRT_EXAMPLE_DB_URL:-postgres://localhost:5432/wruntime_example}}"
echo "DB_URL: ${DB_URL}"

# ── Tracing (OpenTelemetry → Grafana LGTM) ────────────────────────────────────
# OTLP gRPC collector exposed by `just dev-up` (grafana/otel-lgtm) on :4317.
# The services hard-code localhost:4317 as their OTLP endpoint, so no endpoint
# override is needed — just set the log level.
export RUST_LOG="${RUST_LOG:-info}"

# Substitute the DB URL into the engine configs.
update_db_url() {
    local file="$1"
    sed -i.bak "s|postgres://user:pass@localhost:5432/ecommerce|${DB_URL}|g" "$file"
    rm -f "${file}.bak"
}

# ── Apply DB URL to engine and manager configs ────────────────────────────────
cp examples/ecommerce/engine-inventory-1.toml /tmp/inv1.toml
cp examples/ecommerce/engine-inventory-2.toml /tmp/inv2.toml
update_db_url /tmp/inv1.toml
update_db_url /tmp/inv2.toml

cp examples/config/manager.toml /tmp/wr-manager.toml
sed -i.bak "s|postgres://postgres@localhost:5433/wruntime_example|${DB_URL}|g" /tmp/wr-manager.toml
rm -f /tmp/wr-manager.toml.bak

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

# ── Start inventory engines ────────────────────────────────────────────────────
echo "==> Starting inventory engine 1 on :9100"
./target/debug/wr-engine /tmp/inv1.toml &
INV1_PID=$!

echo "==> Starting inventory engine 2 on :9101"
./target/debug/wr-engine /tmp/inv2.toml &
INV2_PID=$!

# Wait for inventory engines to register with the manager.
echo "==> Waiting for inventory engines to register..."
sleep 3

cargo run -p wr-cli -- engines list
cargo run -p wr-cli -- services list

# ── Seed inventory via the proxy ───────────────────────────────────────────────
echo "==> Seeding inventory..."
cargo run -p wr-cli -- invoke \
    --proxy http://127.0.0.1:9001 \
    --destination http://ecommerce.inventory/Seed \
    --source bootstrap \
    --source-ns ecommerce \
    --body '' || echo " (seed may already exist)"

# ── Start client engine ────────────────────────────────────────────────────────
echo "==> Starting client engine on :9200 (3 load-balanced client instances)"
./target/debug/wr-engine examples/ecommerce/engine-client.toml &
CLIENT_PID=$!

# Wait for the client engine to register.
sleep 2

cargo run -p wr-cli -- engines list
cargo run -p wr-cli -- services list

cleanup() {
    echo "==> Shutting down..."
    # Stop engines first so they can deregister with the manager before it exits.
    kill -INT "$CLIENT_PID" "$INV1_PID" "$INV2_PID" 2>/dev/null || true
    wait "$CLIENT_PID" "$INV1_PID" "$INV2_PID" 2>/dev/null || true
    # Now stop proxy and manager.
    kill -INT "$PROXY_PID" "$MANAGER_PID" 2>/dev/null || true
    # Give services time to flush the OTLP batch exporter before exiting.
    sleep 5
}
trap cleanup EXIT
trap 'exit 0' INT TERM

if [ "$INLINE" = true ]; then
    echo "==> Running client inline with {\"count\": 1}..."
    cargo run -p wr-cli -- invoke \
        --proxy http://127.0.0.1:9001 \
        --destination http://ecommerce.client/Run \
        --source loadtest --source-ns ecommerce \
        --body '{"count": 1}'
    # cleanup runs via EXIT trap; exit with invoke's exit code
    exit $?
fi

cat <<'USAGE'

All services running. Press Ctrl-C to stop.
  Manager  : http://127.0.0.1:9000 (gRPC)
  Proxy    : http://127.0.0.1:9001
  Inventory: http://127.0.0.1:9100 + :9101 (2 engines, shared Postgres)
  Client   : http://127.0.0.1:9200 (3 instances, ServiceGuest)

Trigger a load run (default 100 iterations):
  cargo run -p wr-cli -- invoke \
    --proxy http://127.0.0.1:9001 \
    --destination http://ecommerce.client/Run \
    --source loadtest --source-ns ecommerce \
    --body ''

Trigger with a custom request count (e.g. 1000):
  cargo run -p wr-cli -- invoke \
    --proxy http://127.0.0.1:9001 \
    --destination http://ecommerce.client/Run \
    --source loadtest --source-ns ecommerce \
    --body '{"count": 1000}'

Inspect metrics:
  cargo run -p wr-cli -- metrics summary
USAGE

wait
