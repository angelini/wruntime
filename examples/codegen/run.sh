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

# ── Start manager + proxy ────────────────────────────────────────────────
echo "==> Starting manager + proxy (external on :8080)..."
start_manager_proxy "$MANAGER_CFG" "$PROXY_CFG"

# ── Deploy engine (all 4 modules) ────────────────────────────────────────
deploy_engine "$CG_ENGINE_CFG" "engine (collector + agent + coordinator + worker)" 9100
list_services
dev_status

json_field() {
	local field="$1"
	python3 -c 'import json, sys
field = sys.argv[1]
try:
    data = json.load(sys.stdin)
except json.JSONDecodeError as exc:
    raise SystemExit(f"ERROR: failed to parse wr-cli --json output: {exc}")
value = data.get(field)
if not isinstance(value, str) or not value:
    raise SystemExit(f"ERROR: response JSON missing non-empty string field {field!r}")
print(value)' "$field"
}

if [ "$INLINE" = true ]; then
	echo "==> Creating codegen task (worker will process it automatically)..."
	CREATE_OUTPUT=$(just cli invoke --json \
		--proxy http://127.0.0.1:9001 \
		--destination http://codegen.coordinator/codegen.CoordinatorService/CreateTask \
		--source test --source-ns codegen \
		--body '{"repo_url":"https://github.com/dtolnay/anyhow","doc_sources":[{"source_type":"DOC_SOURCE_TYPE_DOCS_RS","owner":"anyhow","ref_or_ver":"1.0"}],"task_description":"Add a context_with method"}')
	echo "$CREATE_OUTPUT"

	TASK_ID=$(printf '%s\n' "$CREATE_OUTPUT" | json_field taskId)
	echo "==> Polling task ${TASK_ID}..."

	while true; do
		if ! TASK_OUTPUT=$(just cli invoke --json \
			--proxy http://127.0.0.1:9001 \
			--destination http://codegen.coordinator/codegen.CoordinatorService/GetTask \
			--source test --source-ns codegen \
			--body "{\"task_id\":\"${TASK_ID}\"}" 2>&1); then
			echo "ERROR: failed to poll task ${TASK_ID}" >&2
			echo "$TASK_OUTPUT" >&2
			exit 1
		fi
		STATUS=$(printf '%s\n' "$TASK_OUTPUT" | json_field status)
		case "$STATUS" in
		complete | TASK_STATUS_COMPLETE)
			echo "$TASK_OUTPUT"
			exit 0
			;;
		error | TASK_STATUS_ERROR)
			echo "$TASK_OUTPUT"
			exit 1
			;;
		*)
			echo "   status: ${STATUS:-unknown}"
			sleep 5
			;;
		esac
	done
fi

cat <<'USAGE'

All services running. Press Ctrl-C to stop.
  Manager     : http://127.0.0.1:9000 (gRPC)
  Proxy       : http://127.0.0.1:9001
  External API: http://127.0.0.1:8080
  Engine      : http://127.0.0.1:9100 (collector + agent + coordinator)

Create a task (returns immediately, processing starts in background):
  curl -X POST http://localhost:8080/tasks \
    -H 'Content-Type: application/json' \
    -d '{"repo_url":"https://github.com/dtolnay/anyhow","doc_sources":[{"source_type":"docs_rs","owner":"anyhow","ref_or_ver":"1.0"}],"task_description":"Add a context_with method"}'

Poll task status until complete:
  curl http://localhost:8080/tasks/{task_id}
USAGE

wait_forever
