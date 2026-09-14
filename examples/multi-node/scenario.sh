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
: "${WRT_PROXY_CONTROL_PORT:=9002}"
: "${WRT_PROXY_PEER_PORT:=9443}"
: "${WRT_SECOND_PROXY_PORT:=9003}"
: "${WRT_SECOND_PROXY_CONTROL_PORT:=9004}"
: "${WRT_SECOND_PROXY_PEER_PORT:=9444}"
: "${WRT_ENGINE_BASE_PORT:=9100}"
: "${WRT_SIMULATOR_PORT:=9200}"

if [ "$INLINE" = true ]; then
	echo "==> Verifying Node A -> Node B echo routing..."
	ECHO_MESSAGE="hello across nodes"
	ECHO_OUTPUT=$(just cli invoke --json \
		--proxy "http://127.0.0.1:${WRT_PROXY_PORT}" \
		--destination http://multinode.echo/multinode.EchoService/Echo \
		--source smoke --source-ns multinode \
		--body "{\"message\":\"${ECHO_MESSAGE}\"}")
	printf '%s\n' "$ECHO_OUTPUT" | python3 -c 'import json, sys
expected = sys.argv[1]
response = json.load(sys.stdin)
if response.get("message") != expected:
    raise SystemExit(f"unexpected echo response: {response!r}")' "$ECHO_MESSAGE"
	echo "    echo response: ${ECHO_MESSAGE}"
	exit 0
fi

cat <<USAGE

Local multi-node topology is running. Press Ctrl-C to stop.
  Manager       : https://127.0.0.1:${WRT_MANAGER_PORT}
  Node A proxy  : http://127.0.0.1:${WRT_PROXY_PORT} (control :${WRT_PROXY_CONTROL_PORT}, peer TLS :${WRT_PROXY_PEER_PORT})
  Node A engines: http://127.0.0.1:${WRT_ENGINE_BASE_PORT} and :$((WRT_ENGINE_BASE_PORT + 1))
  Node B proxy  : http://127.0.0.1:${WRT_SECOND_PROXY_PORT} (control :${WRT_SECOND_PROXY_CONTROL_PORT}, peer TLS :${WRT_SECOND_PROXY_PEER_PORT})
  Node B engine : http://127.0.0.1:${WRT_SIMULATOR_PORT}

The echo module runs only on Node B. Requests sent through Node A therefore
exercise the mTLS peer-proxy hop before reaching the module.

Repeat the cross-node request:
  just cli invoke --json \
    --proxy http://127.0.0.1:${WRT_PROXY_PORT} \
    --destination http://multinode.echo/multinode.EchoService/Echo \
    --source smoke --source-ns multinode \
    --body '{"message":"hello across nodes"}'
USAGE

trap 'exit 0' INT TERM
while true; do
	sleep 3600 &
	wait $! || exit 0
done
