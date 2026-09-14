#!/usr/bin/env bash
set -euo pipefail

INLINE=false
NUM_EXCHANGES=1
args=("$@")
for ((i = 0; i < ${#args[@]}; i++)); do
	case "${args[$i]}" in
	--inline) INLINE=true ;;
	--exchanges)
		if [ $((i + 1)) -ge ${#args[@]} ]; then
			echo "Error: --exchanges requires a value" >&2
			exit 2
		fi
		NUM_EXCHANGES="${args[$((i + 1))]}"
		;;
	esac
done
if ! [[ "$NUM_EXCHANGES" =~ ^[0-9]+$ ]] || [ "$NUM_EXCHANGES" -lt 1 ] || [ "$NUM_EXCHANGES" -gt 63 ]; then
	echo "Error: --exchanges must be between 1 and 63 (got: $NUM_EXCHANGES)" >&2
	exit 2
fi

: "${WRT_MANAGER_PORT:=9000}"
: "${WRT_PROXY_PORT:=9001}"
: "${WRT_ENGINE_BASE_PORT:=9100}"
: "${WRT_SIMULATOR_PORT:=9200}"
EXCHANGE_BASE_PORT=$WRT_ENGINE_BASE_PORT
LEDGER_PORT=$((EXCHANGE_BASE_PORT + NUM_EXCHANGES))
SIMULATOR_PORT=$WRT_SIMULATOR_PORT

if [ "$INLINE" = true ]; then
	echo "==> Running simulator inline (10 traders, 20 orders each, 5 symbols, ${NUM_EXCHANGES} exchange(s))..."
	just cli invoke \
		--proxy "http://127.0.0.1:${WRT_PROXY_PORT}" \
		--destination http://stockmarket.simulator/stockmarket.SimulatorService/Run \
		--source loadtest --source-ns stockmarket \
		--body '{"num_traders": 10, "orders_per_trader": 20, "num_symbols": 5}'
	exit $?
fi

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
  Manager   : https://127.0.0.1:${WRT_MANAGER_PORT} (mTLS gRPC)
  Proxy     : http://127.0.0.1:${WRT_PROXY_PORT}
  Exchange  : ${EXCHANGE_PORTS} (${NUM_EXCHANGES} engine(s), DB-backed order book)
  Ledger    : http://127.0.0.1:${LEDGER_PORT} (DB + S3 blobstore)
  Simulator : http://127.0.0.1:${SIMULATOR_PORT}

Run a simulation (default: 10 traders, 20 orders each, 5 symbols):
  just cli invoke \
    --manager https://127.0.0.1:${WRT_MANAGER_PORT} \
    --proxy http://127.0.0.1:${WRT_PROXY_PORT} \
    --destination http://stockmarket.simulator/stockmarket.SimulatorService/Run \
    --source loadtest --source-ns stockmarket \
    --body ''

Stress test (100 traders, 100 orders each, 10 symbols = 10,000 orders):
  just cli invoke \
    --manager https://127.0.0.1:${WRT_MANAGER_PORT} \
    --proxy http://127.0.0.1:${WRT_PROXY_PORT} \
    --destination http://stockmarket.simulator/stockmarket.SimulatorService/Run \
    --source loadtest --source-ns stockmarket \
    --body '{"num_traders": 100, "orders_per_trader": 100, "num_symbols": 10}'

Inspect metrics:
  just cli --manager https://127.0.0.1:${WRT_MANAGER_PORT} metrics summary
USAGE

trap 'exit 0' INT TERM
while true; do
	sleep 3600 &
	wait $! || exit 0
done
