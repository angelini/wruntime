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

# ── Kill stale processes from a previous run ─────────────────────────────
./target/debug/wr-cli dev down 2>/dev/null || true
for port in 9000 9001 9100 8080; do
    pid=$(lsof -ti ":$port" 2>/dev/null || true)
    if [ -n "$pid" ]; then
        echo "   killing stale process on :$port (pid $pid)"
        kill -INT $pid 2>/dev/null || true
    fi
done
sleep 1

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
allowed_domains = ["api.github.com", "codeload.github.com", "docs.rs", "*.docs.rs", "crates.io", "static.crates.io"]

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

# ── Start manager + proxy ─────────────────────────────────────────────────────
echo "==> Starting manager + proxy (external on :8080)..."
./target/debug/wr-cli dev up \
    --manager-config /tmp/wr-manager.toml \
    --proxy-config /tmp/cg-proxy.toml

# ── Deploy engine (all 3 modules) ────────────────────────────────────────────
echo "==> Deploying engine on :9100 (collector + agent + coordinator)"
./target/debug/wr-cli dev deploy /tmp/cg-engine.toml --skip-build

cargo run -p wr-cli -- engines list
cargo run -p wr-cli -- services list

cleanup() {
    echo "==> Shutting down..."
    ./target/debug/wr-cli dev down
}
trap cleanup EXIT
trap 'exit 0' INT TERM

if [ "$INLINE" = true ]; then
    echo "==> Creating codegen task (worker will process it automatically)..."
    CREATE_OUTPUT=$(cargo run -p wr-cli -- invoke \
        --proxy http://127.0.0.1:9001 \
        --destination http://codegen.coordinator/CreateTask \
        --source test --source-ns codegen \
        --body '{"repo_url":"https://github.com/dtolnay/anyhow","doc_sources":[{"source_type":"docs_rs","owner":"anyhow","ref_or_ver":"1.0"}],"task_description":"Add a context_with method"}')
    echo "$CREATE_OUTPUT"

    TASK_ID=$(echo "$CREATE_OUTPUT" | grep -o '"taskId": *"[^"]*"' | head -1 | sed 's/"taskId": *"//;s/"//')
    if [ -z "$TASK_ID" ]; then
        echo "ERROR: failed to extract task_id from create response"
        exit 1
    fi
    echo "==> Polling task ${TASK_ID}..."

    while true; do
        TASK_OUTPUT=$(cargo run -p wr-cli -- invoke \
            --proxy http://127.0.0.1:9001 \
            --destination http://codegen.coordinator/GetTask \
            --source test --source-ns codegen \
            --body "{\"task_id\":\"${TASK_ID}\"}" 2>/dev/null)
        STATUS=$(echo "$TASK_OUTPUT" | grep -o '"status": *"[^"]*"' | head -1 | sed 's/"status": *"//;s/"//')
        case "$STATUS" in
            complete)
                echo "$TASK_OUTPUT"
                exit 0
                ;;
            error)
                echo "$TASK_OUTPUT"
                exit 1
                ;;
            *)
                echo "   status: ${STATUS:-unknown}"
                sleep 5
                ;;
        esac
    done
fi

cat <<'USAGE'

All services running. Press Ctrl-C to stop.
  Manager     : http://127.0.0.1:9000 (gRPC)
  Proxy       : http://127.0.0.1:9001
  External API: http://127.0.0.1:8080
  Engine      : http://127.0.0.1:9100 (collector + agent + coordinator)

Create a task (returns immediately, processing starts in background):
  curl -X POST http://localhost:8080/tasks \
    -H 'Content-Type: application/json' \
    -d '{"repo_url":"https://github.com/dtolnay/anyhow","doc_sources":[{"source_type":"docs_rs","owner":"anyhow","ref_or_ver":"1.0"}],"task_description":"Add a context_with method"}'

Poll task status until complete:
  curl http://localhost:8080/tasks/{task_id}
USAGE

# Block until Ctrl-C (no child PIDs to wait on — processes are managed by wr dev)
while true; do sleep 60; done
