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

# Build list of all ports to kill on startup
KILL_PORTS=(9000 9001 9002 9010)
for ((i = 0; i < NUM_EXCHANGES; i++)); do
	KILL_PORTS+=($((EXCHANGE_BASE_PORT + i)))
done
KILL_PORTS+=($LEDGER_PORT $SIMULATOR_PORT)

kill_stale_ports "${KILL_PORTS[@]}"

echo "DB_URL: ${DB_URL}"
echo "S3_ENDPOINT: ${S3_ENDPOINT}"
echo "Exchange engines: ${NUM_EXCHANGES} (ports ${EXCHANGE_BASE_PORT}..$((EXCHANGE_BASE_PORT + NUM_EXCHANGES - 1)))"
echo "Ledger engine: port ${LEDGER_PORT}"

# ── Substitute DB and S3 URLs into engine configs ────────────────────────
update_config() {
	local file="$1"
	sed_replace "$file" "postgres://user:pass@localhost:5432/stockmarket" "${DB_URL}"
	sed_replace "$file" "http://127.0.0.1:8900" "${S3_ENDPOINT}"
	sed_replace "$file" "access_key_id     = \"rustfsadmin\"" "access_key_id     = \"${S3_ACCESS_KEY}\""
	sed_replace "$file" "secret_access_key = \"rustfsadmin\"" "secret_access_key = \"${S3_SECRET_KEY}\""
}

# ── Generate exchange engine configs ─────────────────────────────────────
for ((i = 0; i < NUM_EXCHANGES; i++)); do
	port=$((EXCHANGE_BASE_PORT + i))
	cfg="/tmp/sm-exchange-${i}.toml"
	cp examples/stockmarket/engine-exchange.toml "$cfg"
	sed_replace "$cfg" "127.0.0.1:9100" "127.0.0.1:${port}"
	update_config "$cfg"
done

# ── Generate ledger engine config ────────────────────────────────────────
cp examples/stockmarket/engine-ledger.toml /tmp/sm-ledger.toml
sed_replace /tmp/sm-ledger.toml "127.0.0.1:9101" "127.0.0.1:${LEDGER_PORT}"
update_config /tmp/sm-ledger.toml

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
	deploy_engine "/tmp/sm-exchange-${i}.toml" "exchange engine $((i + 1))/${NUM_EXCHANGES}" "$port"
done

# ── Deploy ledger engine ─────────────────────────────────────────────────
deploy_engine /tmp/sm-ledger.toml "ledger engine" "$LEDGER_PORT"
list_services

# ── Deploy simulator engine ──────────────────────────────────────────────
deploy_engine examples/stockmarket/engine-simulator.toml "simulator engine" "$SIMULATOR_PORT"
list_services

setup_cleanup_trap

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
