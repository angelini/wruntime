#!/usr/bin/env bash
# Shared production tunnel and traffic-probe orchestration.

stop_probe() {
	local status=0 pid="${PROBE_PID:-}"
	[ -n "$pid" ] || return 0
	PROBE_PID=""
	[ -n "${PROBE_STOP_FILE:-}" ] && : >"$PROBE_STOP_FILE"
	if wait "$pid"; then :; else status=$?; fi
	lifecycle_trace probe stop "pid=$pid,status=$status"
	return "$status"
}

stop_tunnel() {
	local status=0 pid="${TUNNEL_PID:-}"
	[ -n "$pid" ] || return 0
	TUNNEL_PID=""
	TUNNEL_PORT=""
	if kill -0 "$pid" 2>/dev/null; then
		if kill "$pid"; then :; else status=$?; fi
		if wait "$pid"; then :; else
			status=$?
			case "$status" in 130 | 143) status=0 ;; esac
		fi
	elif wait "$pid"; then
		status=0
	else
		status=$?
	fi
	lifecycle_trace tunnel stop "pid=$pid,status=$status"
	return "$status"
}

start_tunnel() {
	local tunnel_log="$1" status ready=false
	[ -z "${TUNNEL_PID:-}" ] || return 0
	TUNNEL_PORT="$("${PYTHON[@]}" - <<'PY'
import socket
with socket.socket() as listener:
    listener.bind(("127.0.0.1", 0))
    print(listener.getsockname()[1])
PY
	)"
	ssh -i "$WRT_DEPLOY_E2E_SSH_KEY" -o ConnectTimeout=5 \
		-o ExitOnForwardFailure=yes -o ServerAliveInterval=5 -o ServerAliveCountMax=2 \
		-N -L "127.0.0.1:${TUNNEL_PORT}:127.0.0.1:9001" "$NODE_REMOTE" >"$tunnel_log" 2>&1 &
	TUNNEL_PID=$!
	for _ in $(seq 1 20); do
		if ! kill -0 "$TUNNEL_PID" 2>/dev/null; then
			if wait "$TUNNEL_PID"; then status=0; else status=$?; fi
			TUNNEL_PID=""
			TUNNEL_PORT=""
			echo "SSH proxy tunnel exited before becoming ready (exit ${status})" >&2
			return 1
		fi
		if "${PYTHON[@]}" - "$TUNNEL_PORT" <<'PY' 2>/dev/null
import socket, sys
with socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=0.25): pass
PY
		then ready=true; break; fi
		sleep 0.25
	done
	[ "$ready" = true ] || { stop_tunnel || true; echo "SSH proxy tunnel did not become ready" >&2; return 1; }
	lifecycle_trace tunnel start "pid=$TUNNEL_PID,port=$TUNNEL_PORT"
}

invoke_echo_over_tunnel() {
	local expected="$1" log="$2" error
	error="${log%.json}.stderr"
	[ -n "${TUNNEL_PID:-}" ] && kill -0 "$TUNNEL_PID" 2>/dev/null || { echo "SSH proxy tunnel is unavailable" >&2; return 1; }
	timeout -k 1 5 "${CLI_ARGS[@]}" invoke --json \
		--proxy "http://127.0.0.1:${TUNNEL_PORT}" \
		--destination http://deployment.echo/multinode.EchoService/Echo \
		--source deployment-e2e --source-ns deployment \
		--body "{\"message\":\"$expected\"}" >"$log" 2>"$error"
	"${PYTHON[@]}" - "$log" "$expected" <<'PY'
import json,sys
value=json.load(open(sys.argv[1]))
if value.get("message") != sys.argv[2]: raise SystemExit(f"unexpected echo response: {value!r}")
PY
}

invoke_echo() {
	local expected="$1" log="$2" status
	start_tunnel "${log%.json}.tunnel.log"
	if invoke_echo_over_tunnel "$expected" "$log"; then :; else status=$?; stop_tunnel || true; return "$status"; fi
	stop_tunnel
}

start_probe() {
	local expected="$1" log="$2"
	[ -z "${PROBE_PID:-}" ] || { echo "traffic probe is already active" >&2; return 1; }
	PROBE_STOP_FILE="${log%.jsonl}.stop"
	rm -f "$PROBE_STOP_FILE"
	"${PYTHON[@]}" "$ROOT/dev/deployment-e2e/traffic_probe.py" run --log "$log" \
		--stop-file "$PROBE_STOP_FILE" --expected "$expected" -- \
		timeout -k 1 5 "${CLI_ARGS[@]}" invoke --json \
		--proxy "http://127.0.0.1:${TUNNEL_PORT}" \
		--destination http://deployment.echo/multinode.EchoService/Echo \
		--source deployment-e2e --source-ns deployment \
		--body "{\"message\":\"$expected\"}" &
	PROBE_PID=$!
	lifecycle_trace probe start "pid=$PROBE_PID,log=$log"
}
