#!/usr/bin/env bash
# Common helpers for example run.sh scripts.
# Source this file: source "$(dirname "$0")/../helpers.sh"

set -euo pipefail

# ── Parse --inline flag ──────────────────────────────────────────────────────
INLINE=false
for arg in "$@"; do
    case "$arg" in
        --inline) INLINE=true ;;
    esac
done

# ── Repo root ────────────────────────────────────────────────────────────────
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# ── Environment defaults ─────────────────────────────────────────────────────
DB_URL="${DB_URL:-${WRT_EXAMPLE_DB_URL:-postgres://postgres@localhost:5433/wruntime_example}}"
GUEST_DB_URL="${GUEST_DB_URL:-postgres://wr_guest@localhost:5433/wruntime_example}"
S3_ENDPOINT="${S3_ENDPOINT:-http://localhost:8900}"
S3_ACCESS_KEY="${S3_ACCESS_KEY:-rustfsadmin}"
S3_SECRET_KEY="${S3_SECRET_KEY:-rustfsadmin}"
export RUST_LOG="${RUST_LOG:-info}"

# ── Kill stale processes ─────────────────────────────────────────────────────
# Usage: kill_stale_ports 9000 9001 9100 9101
kill_stale_ports() {
    ./target/debug/wr-cli dev down 2>/dev/null || true
    for port in "$@"; do
        pid=$(lsof -ti ":$port" 2>/dev/null || true)
        if [ -n "$pid" ]; then
            echo "   killing stale process on :$port (pid $pid)"
            kill -INT $pid 2>/dev/null || true
        fi
    done
    sleep 1
}

# ── Sed-based config substitution ────────────────────────────────────────────
# Usage: sed_replace <file> <old> <new>
sed_replace() {
    local file="$1" old="$2" new="$3"
    sed -i.bak "s|${old}|${new}|g" "$file"
    rm -f "${file}.bak"
}

# ── Prepare manager config ───────────────────────────────────────────────────
# Copies manager.toml to /tmp and substitutes DB_URL.
# Returns the path via stdout.
prepare_manager_config() {
    local dest="/tmp/wr-manager.toml"
    cp examples/config/manager.toml "$dest"
    sed_replace "$dest" "postgres://postgres@localhost:5433/wruntime_example" "${DB_URL}"
    echo "$dest"
}

# ── Prepare proxy config ─────────────────────────────────────────────────────
# Copies proxy.toml to /tmp and substitutes DB_URL.
# Returns the path via stdout. Caller can append extra config after.
prepare_proxy_config() {
    local dest="${1:-/tmp/wr-proxy.toml}"
    cp examples/config/proxy.toml "$dest"
    sed_replace "$dest" "postgres://postgres@localhost:5433/wruntime_example" "${DB_URL}"
    echo "$dest"
}

# ── Clean stale manager state ────────────────────────────────────────────────
clean_manager_state() {
    echo "==> Cleaning manager state..."
    psql "${DB_URL}" -c "TRUNCATE wr_engines, wr_routing_rules, wr_schemas, wr_managers CASCADE" 2>/dev/null \
        || echo "   (tables may not exist yet — first run)"
}

# ── Start manager + proxy ────────────────────────────────────────────────────
# Usage: start_manager_proxy <manager_config> <proxy_config>
start_manager_proxy() {
    local manager_cfg="$1" proxy_cfg="$2"
    echo "==> Starting manager + proxy..."
    ./target/debug/wr-cli dev up \
        --manager-config "$manager_cfg" \
        --proxy-config "$proxy_cfg"
}

# ── Deploy an engine ─────────────────────────────────────────────────────────
# Usage: deploy_engine <config_path> <label> <port>
deploy_engine() {
    local config="$1" label="$2" port="$3"
    echo "==> Deploying ${label} on :${port}"
    ./target/debug/wr-cli dev deploy "$config" --skip-build
}

# ── Print engine/service lists ───────────────────────────────────────────────
list_services() {
    cargo run -p wr-cli -- engines list
    cargo run -p wr-cli -- services list
}

# ── Create S3 bucket ─────────────────────────────────────────────────────────
# Usage: create_s3_bucket <bucket_name>
create_s3_bucket() {
    local bucket="$1"
    echo "==> Creating S3 bucket '${bucket}'"
    AWS_ACCESS_KEY_ID="${S3_ACCESS_KEY}" AWS_SECRET_ACCESS_KEY="${S3_SECRET_KEY}" \
        aws --endpoint-url "${S3_ENDPOINT}" s3 mb "s3://${bucket}" 2>/dev/null || true
}

# ── Setup cleanup trap ───────────────────────────────────────────────────────
setup_cleanup_trap() {
    cleanup() {
        echo "==> Shutting down..."
        ./target/debug/wr-cli dev down
    }
    trap cleanup EXIT
    trap 'exit 0' INT TERM
}

# ── Wait forever (block until Ctrl-C) ────────────────────────────────────────
wait_forever() {
    while true; do sleep 60; done
}
