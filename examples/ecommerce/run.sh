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

# ── Kill stale processes from a previous run ─────────────────────────────
./target/debug/wr-cli dev down 2>/dev/null || true
for port in 9000 9001 9100 9101 9200; do
    pid=$(lsof -ti ":$port" 2>/dev/null || true)
    if [ -n "$pid" ]; then
        echo "   killing stale process on :$port (pid $pid)"
        kill -INT $pid 2>/dev/null || true
    fi
done
sleep 1

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

# ── Start manager + proxy ─────────────────────────────────────────────────────
echo "==> Starting manager + proxy..."
./target/debug/wr-cli dev up \
    --manager-config /tmp/wr-manager.toml \
    --proxy-config examples/config/proxy.toml

# ── Deploy inventory engines ──────────────────────────────────────────────────
echo "==> Deploying inventory engine 1 on :9100"
./target/debug/wr-cli dev deploy /tmp/inv1.toml --skip-build

echo "==> Deploying inventory engine 2 on :9101"
./target/debug/wr-cli dev deploy /tmp/inv2.toml --skip-build

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

# ── Deploy client engine ──────────────────────────────────────────────────────
echo "==> Deploying client engine on :9200 (3 load-balanced client instances)"
./target/debug/wr-cli dev deploy examples/ecommerce/engine-client.toml --skip-build

cargo run -p wr-cli -- engines list
cargo run -p wr-cli -- services list

cleanup() {
    echo "==> Shutting down..."
    ./target/debug/wr-cli dev down
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

# Block until Ctrl-C (no child PIDs to wait on — processes are managed by wr dev)
while true; do sleep 60; done
