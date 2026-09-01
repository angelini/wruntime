#!/usr/bin/env bash
# Run from the repo root: bash examples/codegen/run.sh
# Prerequisites: cargo, rustup target add wasm32-wasip2, wasm-tools,
#                Postgres + RustFS S3 running. `just dev-up`
source "$(dirname "$0")/../helpers.sh" "$@"

echo "DB_URL: ${DB_URL}"
echo "S3_ENDPOINT: ${S3_ENDPOINT}"

CG_ENGINE_CFG="${CONFIG_DIR}/codegen-engine.toml"
render_config examples/codegen/engine.toml "$CG_ENGINE_CFG" \
	"postgres://user:pass@localhost:5432/codegen" "${DB_URL}" \
	"http://127.0.0.1:8900" "${S3_ENDPOINT}" \
	"access_key_id     = \"rustfsadmin\"" "access_key_id     = \"${S3_ACCESS_KEY}\"" \
	"secret_access_key = \"rustfsadmin\"" "secret_access_key = \"${S3_SECRET_KEY}\""
# ── Prepare manager + proxy configs ──────────────────────────────────────
MANAGER_CFG=$(prepare_manager_config)
PROXY_CFG=$(prepare_proxy_config "${CONFIG_DIR}/codegen-proxy.toml")
cat >>"$PROXY_CFG" <<'PROXY'

[egress]
allowed_domains = ["api.github.com", "codeload.github.com", "docs.rs", "*.docs.rs", "crates.io", "static.crates.io"]

[external]
listen_address = "0.0.0.0:8080"

[[external.route]]
path      = "/tasks"
methods   = ["POST"]
module    = "coordinator"
namespace = "codegen"

[[external.route]]
path      = "/tasks/{id}"
methods   = ["GET"]
module    = "coordinator"
namespace = "codegen"
PROXY

# ── Create S3 bucket ─────────────────────────────────────────────────────
create_s3_bucket codegen

# ── Clean stale manager state ────────────────────────────────────────────
clean_manager_state

configure_dev_run "$MANAGER_CFG"
add_dev_proxy primary "$PROXY_CFG"
add_dev_engine "$CG_ENGINE_CFG"
run_example_scenario examples/codegen/scenario.sh
