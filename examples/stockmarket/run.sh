#!/usr/bin/env bash
# Run from the repo root: bash examples/stockmarket/run.sh
# Prerequisites: cargo, rustup target add wasm32-wasip2, wasm-tools,
#                Postgres + RustFS S3 running. `just dev-up`
#
# Options:
#   --inline          Run a default simulation and exit
#   --exchanges N     Number of exchange engines to deploy (default: 1)
# shellcheck disable=SC1091
source "$(dirname "$0")/../helpers.sh" "$@"

# ── Parse --exchanges N flag ────────────────────────────────────────────
NUM_EXCHANGES=1
args=("$@")
for ((i = 0; i < ${#args[@]}; i++)); do
	if [ "${args[$i]}" = "--exchanges" ] && [ $((i + 1)) -lt ${#args[@]} ]; then
		NUM_EXCHANGES="${args[$((i + 1))]}"
	fi
done

if ! [[ "$NUM_EXCHANGES" =~ ^[0-9]+$ ]] || [ "$NUM_EXCHANGES" -lt 1 ]; then
	echo "Error: --exchanges must be a positive integer (got: $NUM_EXCHANGES)"
	exit 1
fi

# ── Port layout ─────────────────────────────────────────────────────────
# Exchanges: 9100 .. 9100+(N-1)
# Ledger:    9100+N
# Simulator: 9200
EXCHANGE_BASE_PORT=9100
LEDGER_PORT=$((EXCHANGE_BASE_PORT + NUM_EXCHANGES))
SIMULATOR_PORT=9200

echo "DB_URL: ${DB_URL}"
echo "S3_ENDPOINT: ${S3_ENDPOINT}"
echo "Exchange engines: ${NUM_EXCHANGES} (ports ${EXCHANGE_BASE_PORT}..$((EXCHANGE_BASE_PORT + NUM_EXCHANGES - 1)))"
echo "Ledger engine: port ${LEDGER_PORT}"

# ── Substitute fields present in each engine config ─────────────────────
render_exchange_config() {
	local src="$1" dest="$2"
	render_config "$src" "$dest" \
		"postgres://user:pass@localhost:5432/stockmarket" "${DB_URL}"
}

render_ledger_config() {
	local src="$1" dest="$2"
	render_config "$src" "$dest" \
		"postgres://user:pass@localhost:5432/stockmarket" "${DB_URL}" \
		"http://127.0.0.1:8900" "${S3_ENDPOINT}" \
		"access_key_id     = \"rustfsadmin\"" "access_key_id     = \"${S3_ACCESS_KEY}\"" \
		"secret_access_key = \"rustfsadmin\"" "secret_access_key = \"${S3_SECRET_KEY}\""
}

# ── Generate exchange engine configs ─────────────────────────────────────
EXCHANGE_CONFIGS=()
for ((i = 0; i < NUM_EXCHANGES; i++)); do
	port=$((EXCHANGE_BASE_PORT + i))
	cfg="${CONFIG_DIR}/stockmarket-exchange-${i}.toml"
	render_exchange_config examples/stockmarket/engine-exchange.toml "$cfg"
	render_config "$cfg" "$cfg" "127.0.0.1:9100" "127.0.0.1:${port}"
	EXCHANGE_CONFIGS+=("$cfg")
done

# ── Generate ledger and simulator engine configs ─────────────────────────
LEDGER_CFG="${CONFIG_DIR}/stockmarket-ledger.toml"
SIMULATOR_CFG="${CONFIG_DIR}/stockmarket-simulator.toml"
render_ledger_config examples/stockmarket/engine-ledger.toml "$LEDGER_CFG"
render_config "$LEDGER_CFG" "$LEDGER_CFG" "127.0.0.1:9101" "127.0.0.1:${LEDGER_PORT}"
copy_config examples/stockmarket/engine-simulator.toml "$SIMULATOR_CFG"

# ── Prepare manager + proxy configs ──────────────────────────────────────
MANAGER_CFG=$(prepare_manager_config)
PROXY_CFG=$(prepare_proxy_config "${CONFIG_DIR}/proxy.toml")

# ── Create S3 bucket ─────────────────────────────────────────────────────
create_s3_bucket stockmarket

# ── Clean stale manager state ────────────────────────────────────────────
clean_manager_state

configure_dev_run "$MANAGER_CFG"
add_dev_proxy primary "$PROXY_CFG"
for config in "${EXCHANGE_CONFIGS[@]}"; do
	add_dev_engine "$config"
done
add_dev_engine "$LEDGER_CFG"
add_dev_engine "$SIMULATOR_CFG"
run_example_scenario examples/stockmarket/scenario.sh --exchanges "$NUM_EXCHANGES"
