#!/usr/bin/env bash
# Run from the repo root: bash examples/stockmarket/run.sh
# Prerequisites: cargo, rustup target add wasm32-wasip2, wasm-tools,
#                Postgres + RustFS S3 running. `just dev-up`
#
# Options:
#   --inline          Run a default simulation and exit
#   --exchanges N     Number of exchange engines to deploy (default: 1)
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
PROXY_CFG=$(prepare_proxy_config)

# ── Create S3 bucket ─────────────────────────────────────────────────────
create_s3_bucket stockmarket

# ── Clean stale manager state ────────────────────────────────────────────
clean_manager_state

# ── Start manager + proxy ────────────────────────────────────────────────
start_manager_proxy "$MANAGER_CFG" "$PROXY_CFG"

# ── Deploy exchange engines ──────────────────────────────────────────────
for ((i = 0; i < NUM_EXCHANGES; i++)); do
	port=$((EXCHANGE_BASE_PORT + i))
	deploy_engine "${EXCHANGE_CONFIGS[$i]}" "exchange engine $((i + 1))/${NUM_EXCHANGES}" "$port"
done

# ── Deploy ledger engine ─────────────────────────────────────────────────
deploy_engine "$LEDGER_CFG" "ledger engine" "$LEDGER_PORT"
list_services
dev_status

# ── Deploy simulator engine ──────────────────────────────────────────────
deploy_engine "$SIMULATOR_CFG" "simulator engine" "$SIMULATOR_PORT"
list_services
dev_status

if [ "$INLINE" = true ]; then
	echo "==> Running simulator inline (10 traders, 20 orders each, 5 symbols, ${NUM_EXCHANGES} exchange(s))..."
	just cli invoke \
		--proxy http://127.0.0.1:9001 \
		--destination http://stockmarket.simulator/stockmarket.SimulatorService/Run \
		--source loadtest --source-ns stockmarket \
		--body '{"num_traders": 10, "orders_per_trader": 20, "num_symbols": 5}'
	exit $?
fi

# ── Build exchange port list for usage text ──────────────────────────────
EXCHANGE_PORTS=""
for ((i = 0; i < NUM_EXCHANGES; i++)); do
	port=$((EXCHANGE_BASE_PORT + i))
	if [ $i -eq 0 ]; then
		EXCHANGE_PORTS=":${port}"
	else
		EXCHANGE_PORTS="${EXCHANGE_PORTS} + :${port}"
	fi
done

cat <<USAGE

All services running. Press Ctrl-C to stop.
  Manager   : http://127.0.0.1:9000 (gRPC)
  Proxy     : http://127.0.0.1:9001
  Exchange  : ${EXCHANGE_PORTS} (${NUM_EXCHANGES} engine(s), DB-backed order book)
  Ledger    : http://127.0.0.1:${LEDGER_PORT} (DB + S3 blobstore)
  Simulator : http://127.0.0.1:${SIMULATOR_PORT}

Run a simulation (default: 10 traders, 20 orders each, 5 symbols):
  just cli invoke \\
    --manager https://127.0.0.1:9000 \\
    --proxy http://127.0.0.1:9001 \\
    --destination http://stockmarket.simulator/stockmarket.SimulatorService/Run \\
    --source loadtest --source-ns stockmarket \\
    --body ''

Stress test (100 traders, 100 orders each, 10 symbols = 10,000 orders):
  just cli invoke \\
    --manager https://127.0.0.1:9000 \\
    --proxy http://127.0.0.1:9001 \\
    --destination http://stockmarket.simulator/stockmarket.SimulatorService/Run \\
    --source loadtest --source-ns stockmarket \\
    --body '{"num_traders": 100, "orders_per_trader": 100, "num_symbols": 10}'

Inspect metrics:
  just cli --manager https://127.0.0.1:9000 metrics summary
USAGE

wait_forever
