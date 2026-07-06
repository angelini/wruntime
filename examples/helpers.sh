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
export WR_MANAGER="${WR_MANAGER:-https://127.0.0.1:9000}"

RUN_DIR="${WR_EXAMPLE_RUN_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/wr-example.XXXXXX")}"
CONFIG_DIR="${RUN_DIR}/config"
WR_DEV_STATE_DIR="${RUN_DIR}/dev-state"
mkdir -p "${CONFIG_DIR}" "${WR_DEV_STATE_DIR}"
DEV_STATE_ARGS=(--state-dir "${WR_DEV_STATE_DIR}")

cleanup_example_run() {
	local status=$?
	trap - EXIT INT TERM
	echo "==> Shutting down..."
	./target/debug/wr-cli dev "${DEV_STATE_ARGS[@]}" down 2>/dev/null || true
	rm -rf "${RUN_DIR}"
	exit "$status"
}
trap cleanup_example_run EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# ── Generate TLS certificates if missing ─────────────────────────────────────
if [ ! -f certs/ca.crt ]; then
	echo "==> Generating TLS certificates..."
	./target/debug/wr-cli cert init-ca --output certs/
	./target/debug/wr-cli cert generate 127.0.0.1 --ca-dir certs/
	./target/debug/wr-cli cert generate manager --ca-dir certs/
fi

# ── Config rendering ─────────────────────────────────────────────────────────
render_config() {
	local src="$1" dest="$2"
	shift 2
	python3 - "$src" "$dest" "$@" <<'PY'
import pathlib
import sys

src = pathlib.Path(sys.argv[1])
dest = pathlib.Path(sys.argv[2])
pairs = sys.argv[3:]
if len(pairs) % 2:
    raise SystemExit("render_config requires OLD NEW replacement pairs")
text = src.read_text()
for old, new in zip(pairs[0::2], pairs[1::2]):
    if old not in text:
        raise SystemExit(f"{src}: expected template value not found: {old!r}")
    text = text.replace(old, new)
dest.parent.mkdir(parents=True, exist_ok=True)
dest.write_text(text)
PY
}

copy_config() {
	render_config "$1" "$2"
}

# ── Prepare manager config ───────────────────────────────────────────────────
# Copies manager.toml to the run config dir and substitutes DB_URL.
# Returns the path via stdout.
prepare_manager_config() {
	local dest="${CONFIG_DIR}/manager.toml"
	render_config examples/config/manager.toml "$dest" "postgres://postgres@localhost:5433/wruntime_example" "${DB_URL}"
	echo "$dest"
}

# ── Prepare proxy config ─────────────────────────────────────────────────────
# Copies proxy.toml to the run config dir and substitutes DB_URL.
# Returns the path via stdout. Caller can append extra config after.
prepare_proxy_config() {
	local dest="${1:-${CONFIG_DIR}/proxy.toml}"
	render_config examples/config/proxy.toml "$dest" "postgres://postgres@localhost:5433/wruntime_example" "${DB_URL}"
	echo "$dest"
}

# ── Clean stale manager state ────────────────────────────────────────────────
clean_manager_state() {
	echo "==> Cleaning manager state..."
	psql "${DB_URL}" -c "TRUNCATE wr_system.wr_engines, wr_system.wr_routing_rules, wr_system.wr_schemas, wr_system.wr_managers CASCADE" 2>/dev/null ||
		echo "   (tables may not exist yet — first run)"
}

# ── Start manager + proxy ────────────────────────────────────────────────────
# Usage: start_manager_proxy <manager_config> <proxy_config>
start_manager_proxy() {
	local manager_cfg="$1" proxy_cfg="$2"
	echo "==> Starting manager + proxy..."
	./target/debug/wr-cli dev "${DEV_STATE_ARGS[@]}" up \
		--manager-config "$manager_cfg" \
		--proxy-config "$proxy_cfg"
}

# ── Deploy an engine ─────────────────────────────────────────────────────────
# Usage: deploy_engine <config_path> <label> <port>
deploy_engine() {
	local config="$1" label="$2" port="$3"
	echo "==> Deploying ${label} on :${port}"
	./target/debug/wr-cli dev "${DEV_STATE_ARGS[@]}" deploy "$config" --skip-build
}

dev_status() {
	./target/debug/wr-cli dev "${DEV_STATE_ARGS[@]}" status
}

# ── Print engine/service lists ───────────────────────────────────────────────
list_services() {
	just cli engines list
	just cli services list
}

# ── Create S3 bucket ─────────────────────────────────────────────────────────
# Usage: create_s3_bucket <bucket_name>
create_s3_bucket() {
	local bucket="$1"
	echo "==> Creating S3 bucket '${bucket}'"
	AWS_ACCESS_KEY_ID="${S3_ACCESS_KEY}" AWS_SECRET_ACCESS_KEY="${S3_SECRET_KEY}" \
		aws --endpoint-url "${S3_ENDPOINT}" s3 mb "s3://${bucket}" 2>/dev/null || true
}

# ── Wait forever (block until Ctrl-C) ────────────────────────────────────────
wait_forever() {
	while true; do sleep 60; done
}
