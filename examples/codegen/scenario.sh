#!/usr/bin/env bash
set -euo pipefail

INLINE=false
for arg in "$@"; do
	case "$arg" in
	--inline) INLINE=true ;;
	esac
done

: "${WRT_MANAGER_PORT:=9000}"
: "${WRT_PROXY_PORT:=9001}"
: "${WRT_ENGINE_BASE_PORT:=9100}"
: "${WRT_EXTERNAL_PORT:=8080}"

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
	CREATE_OUTPUT=$(curl --fail-with-body --silent --show-error \
		-X POST "http://127.0.0.1:${WRT_EXTERNAL_PORT}/tasks" \
		-H 'Content-Type: application/json' \
		-d '{"repo_url":"https://github.com/dtolnay/anyhow","doc_sources":[{"source_type":"DOC_SOURCE_TYPE_DOCS_RS","owner":"anyhow","ref_or_ver":"1.0"}],"task_description":"Add a context_with method"}')
	echo "$CREATE_OUTPUT"

	TASK_ID=$(printf '%s\n' "$CREATE_OUTPUT" | json_field task_id)
	echo "==> Polling task ${TASK_ID}..."

	while true; do
		if ! TASK_OUTPUT=$(curl --fail-with-body --silent --show-error \
			-X POST "http://127.0.0.1:${WRT_EXTERNAL_PORT}/tasks/status" \
			-H 'Content-Type: application/json' \
			-d "{\"task_id\":\"${TASK_ID}\"}" 2>&1); then
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

cat <<USAGE

All services running. Press Ctrl-C to stop.
  Manager     : https://127.0.0.1:${WRT_MANAGER_PORT} (mTLS gRPC)
  Proxy       : http://127.0.0.1:${WRT_PROXY_PORT}
  External API: http://127.0.0.1:${WRT_EXTERNAL_PORT}
  Engine      : http://127.0.0.1:${WRT_ENGINE_BASE_PORT} (collector + agent + coordinator)

Create a task (returns immediately, processing starts in background):
  curl -X POST http://127.0.0.1:${WRT_EXTERNAL_PORT}/tasks \
    -H 'Content-Type: application/json' \
    -d '{"repo_url":"https://github.com/dtolnay/anyhow","doc_sources":[{"source_type":"DOC_SOURCE_TYPE_DOCS_RS","owner":"anyhow","ref_or_ver":"1.0"}],"task_description":"Add a context_with method"}'

Poll task status until complete:
  curl -X POST http://127.0.0.1:${WRT_EXTERNAL_PORT}/tasks/status \
    -H 'Content-Type: application/json' \
    -d '{"task_id":"{task_id}"}'
USAGE

trap 'exit 0' INT TERM
while true; do
	sleep 3600 &
	wait $! || exit 0
done
