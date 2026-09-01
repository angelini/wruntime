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

configure_dev_run "$MANAGER_CFG"
add_dev_proxy primary "$NODE_A_PROXY_CFG"
add_dev_proxy node-b "$NODE_B_PROXY_CFG"
add_dev_engine "$NODE_B_ENGINE_1_CFG"
add_dev_engine "$NODE_A_ENGINE_1_CFG"
add_dev_engine "$NODE_A_ENGINE_2_CFG"
run_example_scenario examples/multi-node/scenario.sh
