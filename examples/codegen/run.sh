#!/usr/bin/env bash
# Run from the repo root: bash examples/codegen/run.sh
# Prerequisites: cargo, rustup target add wasm32-wasip2, wasm-tools,
#                Postgres + RustFS S3 running. `just dev-up`
source "$(dirname "$0")/../helpers.sh" "$@"

# ── Kill stale processes from a previous run ─────────────────────────────
kill_stale_ports 9000 9001 9002 9010 9100 8080

echo "DB_URL: ${DB_URL}"
echo "S3_ENDPOINT: ${S3_ENDPOINT}"

# ── Substitute DB and S3 URLs into engine config ─────────────────────────
update_config() {
    local file="$1"
    sed_replace "$file" "postgres://user:pass@localhost:5432/codegen" "${DB_URL}"
    sed_replace "$file" "postgres://wr_guest:pass@localhost:5432/codegen" "${GUEST_DB_URL}"
    sed_replace "$file" "http://127.0.0.1:8900" "${S3_ENDPOINT}"
    sed_replace "$file" "access_key_id     = \"rustfsadmin\"" "access_key_id     = \"${S3_ACCESS_KEY}\""
    sed_replace "$file" "secret_access_key = \"rustfsadmin\"" "secret_access_key = \"${S3_SECRET_KEY}\""
}

cp examples/codegen/engine.toml /tmp/cg-engine.toml
update_config /tmp/cg-engine.toml

# ── Prepare manager + proxy configs ──────────────────────────────────────
MANAGER_CFG=$(prepare_manager_config)
PROXY_CFG=$(prepare_proxy_config /tmp/cg-proxy.toml)
cat >> "$PROXY_CFG" << 'PROXY'

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

# ── Create S3 bucket ─────────────────────────────────────────────────────
create_s3_bucket codegen

# ── Clean stale manager state ────────────────────────────────────────────
clean_manager_state

# ── Start manager + proxy ────────────────────────────────────────────────
echo "==> Starting manager + proxy (external on :8080)..."
./target/debug/wr-cli dev up \
    --manager-config "$MANAGER_CFG" \
    --proxy-config "$PROXY_CFG"

# ── Deploy engine (all 3 modules) ────────────────────────────────────────
deploy_engine /tmp/cg-engine.toml "engine (collector + agent + coordinator)" 9100
list_services

setup_cleanup_trap

if [ "$INLINE" = true ]; then
    echo "==> Creating codegen task (worker will process it automatically)..."
    CREATE_OUTPUT=$(just cli invoke \
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
        TASK_OUTPUT=$(just cli invoke \
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

wait_forever
