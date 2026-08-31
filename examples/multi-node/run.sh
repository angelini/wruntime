#!/usr/bin/env bash
# Run from the repo root: bash examples/multi-node/run.sh
# Prerequisites: cargo and Postgres running via `just dev-up`.
source "$(dirname "$0")/../helpers.sh" "$@"

NODE_A_PROXY_CFG="${CONFIG_DIR}/multi-node-a-proxy.toml"
NODE_B_PROXY_CFG="${CONFIG_DIR}/multi-node-b-proxy.toml"
NODE_A_ENGINE_1_CFG="${CONFIG_DIR}/multi-node-a-engine-1.toml"
NODE_A_ENGINE_2_CFG="${CONFIG_DIR}/multi-node-a-engine-2.toml"
NODE_B_ENGINE_1_CFG="${CONFIG_DIR}/multi-node-b-engine-1.toml"

MANAGER_CFG=$(prepare_manager_config)
render_config examples/multi-node/node-a/proxy.toml "$NODE_A_PROXY_CFG" \
  "postgres://postgres@localhost:5433/wruntime_example" "${DB_URL}"
render_config examples/multi-node/node-b/proxy.toml "$NODE_B_PROXY_CFG" \
  "postgres://postgres@localhost:5433/wruntime_example" "${DB_URL}"
copy_config examples/multi-node/node-a/engine-1.toml "$NODE_A_ENGINE_1_CFG"
copy_config examples/multi-node/node-a/engine-2.toml "$NODE_A_ENGINE_2_CFG"
copy_config examples/multi-node/node-b/engine-1.toml "$NODE_B_ENGINE_1_CFG"

echo "DB_URL: ${DB_URL}"
clean_manager_state

# One persistent wr-cli supervisor owns the complete local topology.
start_manager_proxy "$MANAGER_CFG" "$NODE_A_PROXY_CFG"

echo "==> Starting node B proxy..."
./target/debug/wr-cli dev "${DEV_STATE_ARGS[@]}" start-proxy \
  --name node-b "$NODE_B_PROXY_CFG"

# Start the remote echo engine first. The final node A engine readiness
# acknowledgement then proves node A's proxy has converged through that route.
deploy_engine "$NODE_B_ENGINE_1_CFG" "node B engine 1 with echo" 9200
deploy_engine "$NODE_A_ENGINE_1_CFG" "node A engine 1" 9100
deploy_engine "$NODE_A_ENGINE_2_CFG" "node A engine 2" 9101
list_services
dev_status

echo "==> Verifying Node A -> Node B echo routing..."
ECHO_MESSAGE="hello across nodes"
ECHO_OUTPUT=$(just cli invoke --json \
  --proxy http://127.0.0.1:9001 \
  --destination http://multinode.echo/multinode.EchoService/Echo \
  --source smoke --source-ns multinode \
  --body "{\"message\":\"${ECHO_MESSAGE}\"}")
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

wait_for_supervisor
