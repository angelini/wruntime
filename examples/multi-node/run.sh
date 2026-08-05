#!/usr/bin/env bash
# Run from the repo root: bash examples/multi-node/run.sh
# Prerequisites: cargo and Postgres running via `just dev-up`.
source "$(dirname "$0")/../helpers.sh" "$@"

NODE_A_PROXY_CFG="${CONFIG_DIR}/multi-node-a-proxy.toml"
NODE_B_PROXY_CFG="${CONFIG_DIR}/multi-node-b-proxy.toml"
NODE_A_ENGINE_1_CFG="${CONFIG_DIR}/multi-node-a-engine-1.toml"
NODE_A_ENGINE_2_CFG="${CONFIG_DIR}/multi-node-a-engine-2.toml"
NODE_B_ENGINE_1_CFG="${CONFIG_DIR}/multi-node-b-engine-1.toml"
NODE_B_PROXY_LOG="${RUN_DIR}/node-b-proxy.log"
ECHO_ERROR_LOG="${RUN_DIR}/echo-invoke.log"

MANAGER_CFG=$(prepare_manager_config)
render_config examples/multi-node/node-a/proxy.toml "$NODE_A_PROXY_CFG" \
	"postgres://postgres@localhost:5433/wruntime_example" "${DB_URL}"
render_config examples/multi-node/node-b/proxy.toml "$NODE_B_PROXY_CFG" \
	"postgres://postgres@localhost:5433/wruntime_example" "${DB_URL}"
copy_config examples/multi-node/node-a/engine-1.toml "$NODE_A_ENGINE_1_CFG"
copy_config examples/multi-node/node-a/engine-2.toml "$NODE_A_ENGINE_2_CFG"
copy_config examples/multi-node/node-b/engine-1.toml "$NODE_B_ENGINE_1_CFG"

wait_for_ports() {
	local pid="$1" label="$2"
	shift 2
	python3 - "$pid" "$label" "$@" <<'PY'
import os
import socket
import sys
import time

pid = int(sys.argv[1])
label = sys.argv[2]
pending = {int(port) for port in sys.argv[3:]}
deadline = time.monotonic() + 30

while pending and time.monotonic() < deadline:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        raise SystemExit(f"{label} exited before becoming ready")

    for port in list(pending):
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                pending.remove(port)
        except OSError:
            pass
    if pending:
        time.sleep(0.1)

if pending:
    ports = ", ".join(str(port) for port in sorted(pending))
    raise SystemExit(f"timed out waiting for {label} ports: {ports}")
PY
}

check_tracked_processes() {
	local role pid config
	while read -r role pid config; do
		if ! kill -0 "$pid" 2>/dev/null; then
			echo "ERROR: ${role} process ${pid} stopped unexpectedly${config:+ (${config})}" >&2
			return 1
		fi
	done <"${WR_DEV_STATE_DIR}/.wr-dev.pid"
}

monitor_topology() {
	while true; do
		if ! kill -0 "$NODE_B_PROXY_PID" 2>/dev/null; then
			echo "ERROR: node B proxy stopped unexpectedly" >&2
			cat "$NODE_B_PROXY_LOG" >&2
			return 1
		fi
		check_tracked_processes
		sleep 2
	done
}

echo "DB_URL: ${DB_URL}"
clean_manager_state

# wr-cli owns the manager, node A proxy, and all three engine processes.
start_manager_proxy "$MANAGER_CFG" "$NODE_A_PROXY_CFG"

echo "==> Starting node B proxy..."
./target/debug/wr-proxy "$NODE_B_PROXY_CFG" >"$NODE_B_PROXY_LOG" 2>&1 &
NODE_B_PROXY_PID=$!
register_example_child "$NODE_B_PROXY_PID"
if ! wait_for_ports "$NODE_B_PROXY_PID" "node B proxy" 9003 9004; then
	cat "$NODE_B_PROXY_LOG" >&2
	exit 1
fi

deploy_engine "$NODE_A_ENGINE_1_CFG" "node A engine 1" 9100
deploy_engine "$NODE_A_ENGINE_2_CFG" "node A engine 2" 9101
deploy_engine "$NODE_B_ENGINE_1_CFG" "node B engine 1 with echo" 9200
list_services
dev_status

echo "==> Verifying Node A -> Node B echo routing..."
ECHO_MESSAGE="hello across nodes"
ECHO_READY=false
for _ in {1..20}; do
	if ECHO_OUTPUT=$(just cli invoke --json \
		--proxy http://127.0.0.1:9001 \
		--destination http://multinode.echo/multinode.EchoService/Echo \
		--source smoke --source-ns multinode \
		--body "{\"message\":\"${ECHO_MESSAGE}\"}" 2>"$ECHO_ERROR_LOG"); then
		ECHO_READY=true
		break
	fi
	sleep 0.5
done
if [ "$ECHO_READY" != true ]; then
	echo "ERROR: cross-node echo request did not become ready" >&2
	cat "$ECHO_ERROR_LOG" >&2
	exit 1
fi
printf '%s\n' "$ECHO_OUTPUT" | python3 -c 'import json, sys
expected = sys.argv[1]
response = json.load(sys.stdin)
if response.get("message") != expected:
    raise SystemExit(f"unexpected echo response: {response!r}")' "$ECHO_MESSAGE"
echo "    echo response: ${ECHO_MESSAGE}"

if [ "$INLINE" = true ]; then
	exit 0
fi

cat <<'USAGE'

Local multi-node topology is running. Press Ctrl-C to stop.
  Manager       : https://127.0.0.1:9000
  Node A proxy  : http://127.0.0.1:9001 (control :9002, peer TLS :9443)
  Node A engines: http://127.0.0.1:9100 and :9101
  Node B proxy  : http://127.0.0.1:9003 (control :9004, peer TLS :9444)
  Node B engine : http://127.0.0.1:9200

The echo module runs only on Node B. Requests sent through Node A therefore
exercise the mTLS peer-proxy hop before reaching the module.

Repeat the cross-node request:
  just cli invoke --json \
    --proxy http://127.0.0.1:9001 \
    --destination http://multinode.echo/multinode.EchoService/Echo \
    --source smoke --source-ns multinode \
    --body '{"message":"hello across nodes"}'
USAGE

monitor_topology
