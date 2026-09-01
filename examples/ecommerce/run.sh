#!/usr/bin/env bash
# Run from the repo root: bash examples/ecommerce/run.sh
# Prerequisites: cargo, rustup target add wasm32-wasip2, wasm-tools,
#                Postgres running via `just dev-up` (uses wruntime_example by default).
# shellcheck disable=SC1091
source "$(dirname "$0")/../helpers.sh" "$@"

echo "DB_URL: ${DB_URL}"

INV1_CFG="${CONFIG_DIR}/ecommerce-inventory-1.toml"
INV2_CFG="${CONFIG_DIR}/ecommerce-inventory-2.toml"
CLIENT_CFG="${CONFIG_DIR}/ecommerce-client.toml"

render_config examples/ecommerce/engine-inventory-1.toml "$INV1_CFG" \
	"postgres://postgres@localhost:5433/wruntime_example" "${DB_URL}"
render_config examples/ecommerce/engine-inventory-2.toml "$INV2_CFG" \
	"postgres://postgres@localhost:5433/wruntime_example" "${DB_URL}"
copy_config examples/ecommerce/engine-client.toml "$CLIENT_CFG"

# ── Prepare manager + proxy configs ──────────────────────────────────────
MANAGER_CFG=$(prepare_manager_config)
PROXY_CFG=$(prepare_proxy_config "${CONFIG_DIR}/proxy.toml")

# ── Clean stale manager state ────────────────────────────────────────────
clean_manager_state

configure_dev_run "$MANAGER_CFG"
add_dev_proxy primary "$PROXY_CFG"
add_dev_engine "$INV1_CFG"
add_dev_engine "$INV2_CFG"
add_dev_engine "$CLIENT_CFG"
run_example_scenario examples/ecommerce/scenario.sh
