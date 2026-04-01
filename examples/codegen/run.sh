#!/usr/bin/env bash
# Run from the repo root: bash examples/codegen/run.sh
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

# ── Substitute DB and S3 URLs into engine config ─────────────────────────────
update_config() {
    local file="$1"
    sed -i.bak "s|postgres://user:pass@localhost:5432/codegen|${DB_URL}|g" "$file"
    sed -i.bak "s|http://127.0.0.1:8900|${S3_ENDPOINT}|g" "$file"
    sed -i.bak "s|access_key_id     = \"rustfsadmin\"|access_key_id     = \"${S3_ACCESS_KEY}\"|g" "$file"
    sed -i.bak "s|secret_access_key = \"rustfsadmin\"|secret_access_key = \"${S3_SECRET_KEY}\"|g" "$file"
    rm -f "${file}.bak"
}

cp examples/codegen/engine.toml /tmp/cg-engine.toml
update_config /tmp/cg-engine.toml

# ── Manager config ─────────────────────────────────────────────────────────────
cp examples/config/manager.toml /tmp/wr-manager.toml
sed -i.bak "s|postgres://postgres@localhost:5433/wruntime_example|${DB_URL}|g" /tmp/wr-manager.toml
rm -f /tmp/wr-manager.toml.bak

# ── Proxy config (with egress + external ingress) ─────────────────────────────
cp examples/config/proxy.toml /tmp/cg-proxy.toml
cat >> /tmp/cg-proxy.toml << 'PROXY'

[egress]
allowed_domains = ["api.github.com", "codeload.github.com", "*.docs.rs", "crates.io", "static.crates.io"]

[external]
listen_address = "0.0.0.0:8080"

[[external.route]]
path      = "/tasks"
methods   = ["POST"]
module    = "coordinator"
namespace = "codegen"

[[external.route]]
path      = "/tasks/{id}"
methods   = ["GET"]
module    = "coordinator"
namespace = "codegen"
PROXY

# ── Create S3 bucket ──────────────────────────────────────────────────────────
echo "==> Creating S3 bucket 'codegen'"
AWS_ACCESS_KEY_ID="${S3_ACCESS_KEY}" AWS_SECRET_ACCESS_KEY="${S3_SECRET_KEY}" \
    aws --endpoint-url "${S3_ENDPOINT}" s3 mb s3://codegen 2>/dev/null || true

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
echo "==> Starting proxy on :9001 (external on :8080)"
./target/debug/wr-proxy /tmp/cg-proxy.toml &
PROXY_PID=$!
sleep 1

# ── Start engine (all 3 modules) ──────────────────────────────────────────────
echo "==> Starting engine on :9100 (collector + agent + coordinator)"
./target/debug/wr-engine /tmp/cg-engine.toml &
ENGINE_PID=$!

echo "==> Waiting for engine to register..."
sleep 3

cargo run -p wr-cli -- engines list
cargo run -p wr-cli -- services list

cleanup() {
    echo "==> Shutting down..."
    kill -INT "$ENGINE_PID" 2>/dev/null || true
    wait "$ENGINE_PID" 2>/dev/null || true
    kill -INT "$PROXY_PID" "$MANAGER_PID" 2>/dev/null || true
    sleep 5
}
trap cleanup EXIT
trap 'exit 0' INT TERM

if [ "$INLINE" = true ]; then
    echo "==> Running sample codegen task..."
    cargo run -p wr-cli -- invoke \
        --proxy http://127.0.0.1:9001 \
        --destination http://codegen.coordinator/CreateTask \
        --source test --source-ns codegen \
        --body ''
    exit $?
fi

cat <<'USAGE'

All services running. Press Ctrl-C to stop.
  Manager     : http://127.0.0.1:9000 (gRPC)
  Proxy       : http://127.0.0.1:9001
  External API: http://127.0.0.1:8080
  Engine      : http://127.0.0.1:9100 (collector + agent + coordinator)

Create a task via external API:
  curl -X POST http://localhost:8080/tasks \
    -H 'Content-Type: application/json' \
    -d '{"repo_url":"https://github.com/dtolnay/anyhow","ref":"main","doc_sources":[{"source_type":"docs_rs","owner":"anyhow","ref_or_ver":"1.0"}],"task_description":"Add a context_with method"}'

Check task status:
  curl http://localhost:8080/tasks/{task_id}

Create a task via internal RPC:
  cargo run -p wr-cli -- invoke \
    --proxy http://127.0.0.1:9001 \
    --destination http://codegen.coordinator/CreateTask \
    --source test --source-ns codegen \
    --body ''
USAGE

wait
