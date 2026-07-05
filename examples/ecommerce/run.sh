#!/usr/bin/env bash
# Run from the repo root: bash examples/ecommerce/run.sh
# Prerequisites: cargo, rustup target add wasm32-wasip2, wasm-tools,
#                Postgres running via `just dev-up` (uses wruntime_example by default).
source "$(dirname "$0")/../helpers.sh" "$@"

# ── Kill stale processes from a previous run ─────────────────────────────
kill_stale_ports 9000 9001 9002 9010 9100 9101 9200

echo "DB_URL: ${DB_URL}"

# ── Substitute the DB URL into the engine configs ────────────────────────
update_db_url() {
	local file="$1"
	sed_replace "$file" "postgres://user:pass@localhost:5432/ecommerce" "${DB_URL}"
	sed_replace "$file" "postgres://wr_guest@localhost:5433/wruntime_example" "${GUEST_DB_URL}"
}

cp examples/ecommerce/engine-inventory-1.toml /tmp/inv1.toml
cp examples/ecommerce/engine-inventory-2.toml /tmp/inv2.toml
update_db_url /tmp/inv1.toml
update_db_url /tmp/inv2.toml

# ── Prepare manager + proxy configs ──────────────────────────────────────
MANAGER_CFG=$(prepare_manager_config)
PROXY_CFG=$(prepare_proxy_config)

# ── Clean stale manager state ────────────────────────────────────────────
clean_manager_state

# ── Start manager + proxy ────────────────────────────────────────────────
start_manager_proxy "$MANAGER_CFG" "$PROXY_CFG"

# ── Deploy inventory engines ─────────────────────────────────────────────
deploy_engine /tmp/inv1.toml "inventory engine 1" 9100
deploy_engine /tmp/inv2.toml "inventory engine 2" 9101
list_services

# ── Seed inventory via the proxy ─────────────────────────────────────────
echo "==> Seeding inventory..."
just cli invoke \
	--proxy http://127.0.0.1:9001 \
	--destination http://ecommerce.inventory/Seed \
	--source bootstrap \
	--source-ns ecommerce \
	--body '' || echo " (seed may already exist)"

# ── Deploy client engine ─────────────────────────────────────────────────
deploy_engine examples/ecommerce/engine-client.toml "client engine" 9200
list_services

setup_cleanup_trap

if [ "$INLINE" = true ]; then
	echo "==> Running client inline with {\"count\": 1}..."
	just cli invoke \
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
  just cli invoke \
    --manager https://127.0.0.1:9000 \
    --proxy http://127.0.0.1:9001 \
    --destination http://ecommerce.client/Run \
    --source loadtest --source-ns ecommerce \
    --body ''

Trigger with a custom request count (e.g. 1000):
  just cli invoke \
    --manager https://127.0.0.1:9000 \
    --proxy http://127.0.0.1:9001 \
    --destination http://ecommerce.client/Run \
    --source loadtest --source-ns ecommerce \
    --body '{"count": 1000}'

Inspect metrics:
  just cli --manager https://127.0.0.1:9000 metrics summary
USAGE

wait_forever
