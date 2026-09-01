#!/usr/bin/env bash
set -euo pipefail

INLINE=false
for arg in "$@"; do
	case "$arg" in
	--inline) INLINE=true ;;
	esac
done

if [ "$INLINE" = true ]; then
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

trap 'exit 0' INT TERM
while true; do
	sleep 3600 &
	wait $! || exit 0
done
