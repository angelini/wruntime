#!/usr/bin/env bash
# Live deployment lifecycle validation against repository-configured disposable VMs.
set -Eeuo pipefail

ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$ROOT"
CONFIG="${WRT_DEPLOY_E2E_CONFIG:-dev/deployment-e2e.toml}"
PROVIDER="${WRT_DEPLOY_E2E_PROVIDER:-dev/deployment-e2e/proxmox.py}"
ASSERT="dev/deployment-e2e/assert_cluster.py"
BACKENDS=(systemd docker)
PRIMARY_STATUS=0
CLEANUP_STARTED=false
ACTIVE_BACKEND=""
ERROR_HANDLED=false
TUNNEL_PID=""

usage() {
	cat <<'USAGE'
Usage: dev/validate-deployment-lifecycle.sh [--backend systemd|docker]

All protected inputs are required. With no --backend, systemd and Docker run
serially and each pass begins and ends at the configured baseline snapshot.
USAGE
}
while [ $# -gt 0 ]; do
	case "$1" in
	--backend)
		[ $# -ge 2 ] || {
			usage >&2
			exit 2
		}
		case "$2" in systemd | docker) BACKENDS=("$2") ;; *)
			usage >&2
			exit 2
			;;
		esac
		shift
		;;
	-h | --help)
		usage
		exit 0
		;;
	*)
		usage >&2
		exit 2
		;;
	esac
	shift
done

for name in PVE_HOST PVE_USER PVE_TOKEN_NAME PVE_TOKEN_VALUE WRT_DEPLOY_E2E_SSH_KEY WRT_DEPLOY_E2E_DB_URL WRT_SECRET_ENCRYPTION_KEY; do
	if [ -z "${!name:-}" ]; then
		echo "missing required protected input: $name" >&2
		exit 2
	fi
done
for command in uv flock ssh scp timeout psql cargo-zigbuild; do
	command -v "$command" >/dev/null || {
		echo "missing required command: $command" >&2
		exit 127
	}
done
PYTHON=(uv run --project "$ROOT/dev/deployment-e2e" --locked python)
# shellcheck source=deployment-e2e/lifecycle_logging.sh
source "$ROOT/dev/deployment-e2e/lifecycle_logging.sh"
[ -f "$WRT_DEPLOY_E2E_SSH_KEY" ] || {
	echo "deployment SSH key file is unavailable" >&2
	exit 2
}
KNOWN_HOSTS="${WRT_DEPLOY_E2E_KNOWN_HOSTS:-$HOME/.ssh/wruntime-e2e-known_hosts}"
[ -f "$KNOWN_HOSTS" ] || {
	echo "dedicated deployment known_hosts file is unavailable" >&2
	exit 2
}
REAL_SSH="$(command -v ssh)"
REAL_SCP="$(command -v scp)"

config_value() {
	"${PYTHON[@]}" - "$CONFIG" "$1" <<'PY'
import sys, tomllib
value = tomllib.load(open(sys.argv[1], "rb"))
for part in sys.argv[2].split("."):
    value = value[part]
print(value)
PY
}
MANAGER_HOST="$(config_value manager.host)"
MANAGER_USER="$(config_value manager.ssh_user)"
NODE_HOST="$(config_value node.host)"
NODE_USER="$(config_value node.ssh_user)"
NODE_ID="$(config_value node_id)"
WORKDIR="$(config_value workdir)"
LOCK_FILE="$(config_value lock_file)"
for host in "$MANAGER_HOST" "$NODE_HOST"; do
	case "$host" in localhost | 127.* | ::1)
		echo "refusing deployment target $host" >&2
		exit 2
		;;
	esac
done

LOG_BASE="${WR_VALIDATE_LOG_DIR:-target/deployment-e2e/$(date +%Y%m%d-%H%M%S)}"
RUN_DIR="$(mktemp -d "${TMPDIR:-/tmp}/wr-deployment-e2e.XXXXXX")"
mkdir -p "$LOG_BASE"
chmod 700 "$RUN_DIR"
echo "logs: $LOG_BASE"
mkdir -p "$RUN_DIR/bin"
printf '#!/usr/bin/env bash\nexec %q -o BatchMode=yes -o StrictHostKeyChecking=yes -o UserKnownHostsFile=%q "$@"\n' "$REAL_SSH" "$KNOWN_HOSTS" >"$RUN_DIR/bin/ssh"
printf '#!/usr/bin/env bash\nexec %q -o BatchMode=yes -o StrictHostKeyChecking=yes -o UserKnownHostsFile=%q "$@"\n' "$REAL_SCP" "$KNOWN_HOSTS" >"$RUN_DIR/bin/scp"
chmod 700 "$RUN_DIR/bin/ssh" "$RUN_DIR/bin/scp"
export PATH="$RUN_DIR/bin:$PATH"
CERT_DIR="$RUN_DIR/certs"
MANAGER_BUNDLE="$RUN_DIR/manager.tar.gz"
BUNDLE_A="$RUN_DIR/node-a.tar.gz"
BUNDLE_B="$RUN_DIR/node-b.tar.gz"
MANAGER_ADDR="https://${MANAGER_HOST}:9000"
MANAGER_REMOTE="${MANAGER_USER}@${MANAGER_HOST}"
NODE_REMOTE="${NODE_USER}@${NODE_HOST}"
SSH=(timeout -k 5 60 ssh -i "$WRT_DEPLOY_E2E_SSH_KEY" -o ConnectTimeout=5)
CLI_ARGS=("$ROOT/target/debug/wr-cli" --manager "$MANAGER_ADDR" --ca-cert "$CERT_DIR/ca.crt" --client-cert "$CERT_DIR/${MANAGER_HOST}.crt" --client-key "$CERT_DIR/${MANAGER_HOST}.key")
CLI=(timeout -k 10 600 "${CLI_ARGS[@]}")
NODE_STOP_SSH_ARGS=()
if [ -n "${WRT_DEPLOY_E2E_SSH_PORT:-}" ]; then
	NODE_STOP_SSH_ARGS=(--ssh-port "$WRT_DEPLOY_E2E_SSH_PORT")
fi

mkdir -p "$(dirname "$LOCK_FILE")"
exec 9>"$LOCK_FILE" || {
	echo "cannot open deployment E2E lock: $LOCK_FILE" >&2
	exit 1
}
flock -n 9 || {
	echo "another deployment lifecycle validation owns $LOCK_FILE" >&2
	exit 1
}

provider() { "${PYTHON[@]}" "$PROVIDER" --config "$CONFIG" "$@"; }
record_failure() {
	local status="$1"
	PRIMARY_STATUS="$(preserve_primary_status "$PRIMARY_STATUS" "$status")"
}
redact_logs() {
	"${PYTHON[@]}" - "$LOG_BASE" <<'PY'
import os, pathlib, re, sys
root = pathlib.Path(sys.argv[1])
secrets = [os.environ.get(name, "").encode() for name in (
    "PVE_TOKEN_VALUE", "WRT_DEPLOY_E2E_DB_URL", "WRT_SECRET_ENCRYPTION_KEY"
)]
for path in root.rglob("*"):
    if not path.is_file():
        continue
    try:
        data = path.read_bytes()
    except OSError:
        continue
    original = data
    for secret in secrets:
        if secret:
            data = data.replace(secret, b"<redacted>")
    data = re.sub(rb"postgres(?:ql)?://[^\s]+", b"<redacted-database-url>", data)
    if data != original:
        path.write_bytes(data)
PY
}
stop_tunnel() {
	local status=0 pid="$TUNNEL_PID"
	[ -n "$pid" ] || return 0
	TUNNEL_PID=""
	if kill -0 "$pid" 2>/dev/null; then
		if kill "$pid"; then
			:
		else
			status=$?
		fi
		if wait "$pid"; then
			:
		else
			status=$?
			case "$status" in 130 | 143) status=0 ;; esac
		fi
	elif wait "$pid"; then
		status=0
	else
		status=$?
	fi
	return "$status"
}
record_tunnel_cleanup_failure() {
	local context="$1" status="$2" log="$LOG_BASE/tunnel-cleanup-failures.log"
	printf '%s status=%s\n' "$context" "$status" >>"$log"
	record_promoted_cleanup_failure "$context SSH tunnel" "$status" "$log"
	echo "$context: SSH tunnel cleanup failed with $status (recorded in $log)" >&2
}
# Invoked indirectly by EXIT/INT/TERM traps.
# shellcheck disable=SC2317
cleanup() {
	local incoming=$? cleanup_status=0
	[ "$incoming" -eq 0 ] || record_failure "$incoming"
	[ "$CLEANUP_STARTED" = false ] || return
	CLEANUP_STARTED=true
	if stop_tunnel; then :; else
		cleanup_status=$?
		record_promoted_cleanup_failure "EXIT SSH tunnel" "$cleanup_status"
	fi
	collect_diagnostic "final provider status" \
		"$LOG_BASE/final-provider-status-before-reset.json" provider status
	if provider stop-reset >"$LOG_BASE/final-provider-stop-reset.json" 2>&1; then
		:
	else
		cleanup_status=$?
		record_promoted_cleanup_failure "final provider reset" "$cleanup_status" \
			"$LOG_BASE/final-provider-stop-reset.json"
	fi
	if redact_logs; then :; else
		cleanup_status=$?
		record_promoted_cleanup_failure "final log redaction" "$cleanup_status" "$LOG_BASE"
	fi
	if flock -u 9; then :; else
		cleanup_status=$?
		record_promoted_cleanup_failure "deployment lock release" "$cleanup_status" "$LOCK_FILE"
	fi
	report_diagnostic_failures
	report_cleanup_failures
	if [ "$cleanup_status" -eq 0 ]; then
		if remove_directory_with_cleanup_accounting \
			"deployment run-directory removal" "$RUN_DIR"; then :; else
			cleanup_status=$?
		fi
	fi
	if [ "$PRIMARY_STATUS" -ne 0 ]; then echo "deployment E2E failure logs retained: $LOG_BASE" >&2; fi
	exit "$PRIMARY_STATUS"
}
trap cleanup EXIT
trap 'record_failure 130; exit 130' INT TERM

run_logged() {
	local name="$1"
	shift
	run_to_log "$name" "$LOG_BASE/${name//[^A-Za-z0-9_.-]/_}.log" "$@"
}
status_json() {
	local output="$1"
	"${CLI[@]}" cluster status --node "$NODE_ID" --output json >"$output"
}
revision_from() {
	"${PYTHON[@]}" - "$1" "$NODE_ID" <<'PY'
import json,sys
status=json.load(open(sys.argv[1]))
node=next(n for n in status["nodes"] if n["node_id"] == sys.argv[2])
print(node["desired_deployment"]["revision"])
PY
}
digest_from_inspect() { awk '$1 == "digest:" {print $2; exit}' "$1"; }
invoke_echo() {
	local expected="$1" log="$2" port tunnel_log invoke_error status
	port="$(
		"${PYTHON[@]}" - <<'PY'
import socket
with socket.socket() as listener:
    listener.bind(("127.0.0.1", 0))
    print(listener.getsockname()[1])
PY
	)"
	tunnel_log="${log%.json}.tunnel.log"
	# The deployed proxy's public-data listener intentionally stays loopback-only.
	# Exercise it through a bounded, strict-host-key SSH forward from the runner.
	ssh -i "$WRT_DEPLOY_E2E_SSH_KEY" -o ConnectTimeout=5 \
		-o ExitOnForwardFailure=yes -o ServerAliveInterval=5 -o ServerAliveCountMax=2 \
		-N -L "127.0.0.1:${port}:127.0.0.1:9001" "$NODE_REMOTE" >"$tunnel_log" 2>&1 &
	TUNNEL_PID=$!
	local ready=false
	for _ in $(seq 1 20); do
		if ! kill -0 "$TUNNEL_PID" 2>/dev/null; then
			if wait "$TUNNEL_PID"; then status=0; else status=$?; fi
			TUNNEL_PID=""
			echo "SSH proxy tunnel exited before becoming ready (exit ${status})" >&2
			return 1
		fi
		if "${PYTHON[@]}" - "$port" <<'PY' 2>/dev/null
import socket, sys
with socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=0.25):
    pass
PY
		then
			ready=true
			break
		fi
		sleep 0.25
	done
	if [ "$ready" != true ]; then
		if stop_tunnel; then :; else
			status=$?
			record_tunnel_cleanup_failure "proxy tunnel readiness failure" "$status"
		fi
		echo "SSH proxy tunnel did not become ready" >&2
		return 1
	fi
	invoke_error="${log%.json}.stderr"
	if timeout -k 1 3 "${CLI_ARGS[@]}" invoke --json \
		--proxy "http://127.0.0.1:${port}" \
		--destination http://deployment.echo/multinode.EchoService/Echo \
		--source deployment-e2e --source-ns deployment \
		--body "{\"message\":\"$expected\"}" >"$log" 2>"$invoke_error"; then
		:
	else
		status=$?
		if stop_tunnel; then :; else
			local cleanup_status=$?
			record_tunnel_cleanup_failure "one-shot invoke failure" "$cleanup_status"
		fi
		echo "one-shot guest invocation failed after semantic readiness" >&2
		print_failure_excerpt "$invoke_error"
		return "$status"
	fi
	"${PYTHON[@]}" - "$log" "$expected" <<'PY'
import json,sys
value=json.load(open(sys.argv[1]))
if value.get("message") != sys.argv[2]: raise SystemExit(f"unexpected echo response: {value!r}")
PY
	if stop_tunnel; then :; else
		status=$?
		record_tunnel_cleanup_failure "successful one-shot invoke" "$status"
		return "$status"
	fi
}
assert_db_clean() {
	local count="" ready=false error_log="$LOG_BASE/postgres-readiness.log"
	for _ in $(seq 1 30); do
		if count="$(PGCONNECT_TIMEOUT=5 timeout 10 psql "$WRT_DEPLOY_E2E_DB_URL" -XAtqc "SELECT count(*) FROM information_schema.schemata WHERE schema_name LIKE 'wr\\_\\_%' ESCAPE '\\' OR schema_name='wr_system'; SELECT count(*) FROM information_schema.tables WHERE table_schema='public' AND table_name LIKE 'wr\\_%' ESCAPE '\\'; SELECT count(*) FROM pg_roles WHERE rolname LIKE 'wr\\_ns\\_%' ESCAPE '\\';" 2>"$error_log")"; then
			ready=true
			break
		fi
		sleep 1
	done
	[ "$ready" = true ] || {
		echo "deployment PostgreSQL did not become reachable" >&2
		echo "failure log: $error_log" >&2
		redact_logs
		print_failure_excerpt "$error_log"
		return 1
	}
	[ "$count" = $'0\n0\n0' ] || {
		echo "deployment database is not at the clean baseline" >&2
		return 1
	}
	"${SSH[@]}" "$MANAGER_REMOTE" "sudo systemctl is-active postgresql" >/dev/null
}
collect_diagnostics() {
	local backend="$1"
	local out="$LOG_BASE/$backend-diagnostics"
	mkdir -p "$out"
	collect_diagnostic "cluster status" "$out/cluster.json" \
		"${CLI[@]}" cluster status --node "$NODE_ID" --output json
	collect_diagnostic "inspect bundle A" "$out/bundle-a.txt" \
		"${CLI[@]}" node inspect-bundle "$BUNDLE_A"
	collect_diagnostic "inspect bundle B" "$out/bundle-b.txt" \
		"${CLI[@]}" node inspect-bundle "$BUNDLE_B"
	collect_diagnostic "manager systemd status" "$out/manager-systemd.txt" \
		"${SSH[@]}" "$MANAGER_REMOTE" "sudo systemctl status --no-pager 'wr-*'"
	collect_diagnostic "manager journal" "$out/manager-journal.txt" \
		"${SSH[@]}" "$MANAGER_REMOTE" "sudo journalctl -q -u 'wr-*' -n 300 --no-pager"
	collect_diagnostic "manager files" "$out/manager-files.txt" \
		"${SSH[@]}" "$MANAGER_REMOTE" "sudo find '$WORKDIR' -maxdepth 5 \( -type f -o -type l \) | sort"
	collect_diagnostic "node systemd status" "$out/node-systemd.txt" \
		"${SSH[@]}" "$NODE_REMOTE" "sudo systemctl status --no-pager 'wr-*'"
	collect_diagnostic "node journal" "$out/node-journal.txt" \
		"${SSH[@]}" "$NODE_REMOTE" "sudo journalctl -q -u 'wr-*' -n 300 --no-pager"
	collect_diagnostic "node files" "$out/node-files.txt" \
		"${SSH[@]}" "$NODE_REMOTE" "sudo find '$WORKDIR' -maxdepth 6 \( -type f -o -type l \) | sort"
	if [ "$backend" = docker ]; then
		collect_diagnostic "manager compose" "$out/manager-compose.txt" \
			"${SSH[@]}" "$MANAGER_REMOTE" "cd '$WORKDIR/wr-manager' && sudo docker compose --project-name wruntime-manager -f docker/docker-compose.yml ps -a && sudo docker compose --project-name wruntime-manager -f docker/docker-compose.yml logs --no-color --tail 300"
		collect_diagnostic "node compose" "$out/node-compose.txt" \
			"${SSH[@]}" "$NODE_REMOTE" "cd '$WORKDIR/wr-node/current' && sudo docker compose --project-name wruntime-node -f docker/docker-compose.yml ps -a && sudo docker compose --project-name wruntime-node -f docker/docker-compose.yml images && sudo docker compose --project-name wruntime-node -f docker/docker-compose.yml logs --no-color --tail 300"
	fi
}

# Invoked indirectly by the ERR trap.
# shellcheck disable=SC2317
on_error() {
	local status="$1"
	[ "$ERROR_HANDLED" = false ] || exit "$status"
	ERROR_HANDLED=true
	trap - ERR
	report_active_failure "$status"
	if [ -n "$ACTIVE_BACKEND" ]; then collect_diagnostics "$ACTIVE_BACKEND"; fi
	record_failure "$status"
	exit "$status"
}
trap 'on_error $?' ERR

run_logged provider-preflight provider preflight
run_logged build-echo cargo run --bin wr-cli -- dev build --config examples/multi-node/node-b/engine-1.toml
run_logged build-workspace cargo build
mkdir -p "$CERT_DIR"
chmod 700 "$CERT_DIR"
run_logged cert-init target/debug/wr-cli cert init-ca --output "$CERT_DIR"
run_logged cert-manager target/debug/wr-cli cert generate "$MANAGER_HOST" --ca-dir "$CERT_DIR" --ip "$MANAGER_HOST"
run_logged cert-node target/debug/wr-cli cert generate "$NODE_HOST" --ca-dir "$CERT_DIR" --ip "$NODE_HOST"
run_logged manager-bundle target/debug/wr-cli managers bundle --manager-config wr-tests/deployment/manager.toml --output "$MANAGER_BUNDLE"
run_logged manager-inspect target/debug/wr-cli managers inspect-bundle "$MANAGER_BUNDLE"
cp wr-tests/deployment/engine-a.toml "$RUN_DIR/engine.toml"
run_logged node-a-bundle target/debug/wr-cli node bundle --engine-config "$RUN_DIR/engine.toml" --proxy-config wr-tests/deployment/proxy.toml --output "$BUNDLE_A"
run_logged node-a-inspect target/debug/wr-cli node inspect-bundle "$BUNDLE_A"
cp wr-tests/deployment/engine-b.toml "$RUN_DIR/engine.toml"
run_logged node-b-bundle target/debug/wr-cli node bundle --engine-config "$RUN_DIR/engine.toml" --proxy-config wr-tests/deployment/proxy.toml --skip-build --output "$BUNDLE_B"
run_logged node-b-inspect target/debug/wr-cli node inspect-bundle "$BUNDLE_B"
DIGEST_A="$(digest_from_inspect "$LOG_BASE/node-a-inspect.log")"
DIGEST_B="$(digest_from_inspect "$LOG_BASE/node-b-inspect.log")"
if [ -z "$DIGEST_A" ] || [ -z "$DIGEST_B" ] || [ "$DIGEST_A" = "$DIGEST_B" ]; then
	echo "node bundles do not have distinct inspected digests" >&2
	exit 1
fi

lifecycle() {
	local backend="$1"
	local pass="$LOG_BASE/$backend"
	mkdir -p "$pass"
	echo "==> deployment lifecycle: $backend"
	run_to_log "$backend provider reset" "$pass/provider-reset.json" provider reset
	assert_db_clean
	if [ "$backend" = docker ]; then
		"${SSH[@]}" "$MANAGER_REMOTE" "sudo -n docker info >/dev/null && sudo -n docker compose version >/dev/null"
		"${SSH[@]}" "$NODE_REMOTE" "sudo -n docker info >/dev/null && sudo -n docker compose version >/dev/null"
	fi

	# Secrets are passed directly to wr-cli and are never included in an echoed command transcript.
	run_to_log "$backend manager deploy" "$pass/manager-deploy.log" \
		"${CLI[@]}" managers deploy "$MANAGER_BUNDLE" "$MANAGER_REMOTE" --format "$backend" \
		--db-url "$WRT_DEPLOY_E2E_DB_URL" --secret-key "$WRT_SECRET_ENCRYPTION_KEY" \
		--ssh-key "$WRT_DEPLOY_E2E_SSH_KEY" --cert-dir "$CERT_DIR" \
		--advertise-address "$MANAGER_ADDR" --gossip-address "${MANAGER_HOST}:9010"
	status_json "$pass/manager-status.json"
	"${PYTHON[@]}" "$ASSERT" --input "$pass/manager-status.json" manager --address "$MANAGER_ADDR" >"$pass/manager-assert.json"

	run_to_log "$backend node A deploy" "$pass/deploy-a.log" \
		"${CLI[@]}" node deploy --node-id "$NODE_ID" "$BUNDLE_A" "$NODE_REMOTE" --format "$backend" \
		--db-url "$WRT_DEPLOY_E2E_DB_URL" --ssh-key "$WRT_DEPLOY_E2E_SSH_KEY" --cert-dir "$CERT_DIR"
	status_json "$pass/status-a.json"
	"${PYTHON[@]}" "$ASSERT" --input "$pass/status-a.json" desired --node-id "$NODE_ID" --digest "$DIGEST_A" --version 1.0.0 >"$pass/assert-a.json"
	local revision_a revision_b
	revision_a="$(revision_from "$pass/status-a.json")"
	invoke_echo "hello-$backend-a" "$pass/invoke-a.json"

	local failed_status
	if "${CLI[@]}" node deploy --node-id "$NODE_ID" "$BUNDLE_B" "$NODE_REMOTE" --format "$backend" \
		--db-url "$WRT_DEPLOY_E2E_DB_URL" --ssh-key "$WRT_DEPLOY_E2E_SSH_KEY" --cert-dir "$RUN_DIR/missing-certs" >"$pass/failed-attempt.log" 2>&1; then
		failed_status=0
	else
		failed_status=$?
	fi
	[ "$failed_status" -ne 0 ] || {
		echo "intentionally invalid deployment unexpectedly succeeded" >&2
		return 1
	}
	status_json "$pass/status-failed.json"
	"${PYTHON[@]}" "$ASSERT" --input "$pass/status-failed.json" failed --node-id "$NODE_ID" --serving-digest "$DIGEST_A" --failed-digest "$DIGEST_B" --after-revision "$revision_a" >"$pass/assert-failed.json"
	invoke_echo "hello-$backend-after-failure" "$pass/invoke-after-failure.json"

	run_to_log "$backend node B deploy" "$pass/deploy-b.log" \
		"${CLI[@]}" node deploy --node-id "$NODE_ID" "$BUNDLE_B" "$NODE_REMOTE" --format "$backend" \
		--db-url "$WRT_DEPLOY_E2E_DB_URL" --ssh-key "$WRT_DEPLOY_E2E_SSH_KEY" --cert-dir "$CERT_DIR"
	status_json "$pass/status-b.json"
	"${PYTHON[@]}" "$ASSERT" --input "$pass/status-b.json" desired --node-id "$NODE_ID" --digest "$DIGEST_B" --version 2.0.0 >"$pass/assert-b.json"
	revision_b="$(revision_from "$pass/status-b.json")"
	[ "$revision_b" -gt "$revision_a" ]
	invoke_echo "hello-$backend-b" "$pass/invoke-b.json"

	local stop_record="$pass/engine-stop.json" stop_error="$pass/engine-stop.stderr" stop_status
	echo "==> $backend node engine stop"
	if "${CLI[@]}" node stop "$NODE_REMOTE" --component engine:engine --format "$backend" \
		--workdir "$WORKDIR" --ssh-key "$WRT_DEPLOY_E2E_SSH_KEY" \
		"${NODE_STOP_SSH_ARGS[@]}" --json >"$stop_record" 2>"$stop_error"; then
		:
	else
		stop_status=$?
		echo "$backend node engine stop failed with $stop_status (stderr: $stop_error)" >&2
		print_failure_excerpt "$stop_error"
		return "$stop_status"
	fi
	assert_node_stop_record "$stop_record" "$backend" engine:engine
	"${CLI[@]}" cluster wait --node "$NODE_ID" --severity unhealthy \
		--timeout-secs 30 >"$pass/expect-unhealthy.json"
	"${PYTHON[@]}" - "$pass/expect-unhealthy.json" "$pass/status-unhealthy.json" <<'PY'
import json, pathlib, sys
value = json.load(open(sys.argv[1]))
pathlib.Path(sys.argv[2]).write_text(json.dumps(value["snapshot"], indent=2) + "\n")
PY
	"${PYTHON[@]}" "$ASSERT" --input "$pass/status-unhealthy.json" unhealthy --node-id "$NODE_ID" >"$pass/assert-unhealthy.json"

	run_to_log "$backend node rollback" "$pass/rollback.log" \
		"${CLI[@]}" node rollback "$NODE_REMOTE" --node-id "$NODE_ID" --to "$revision_a" --ssh-key "$WRT_DEPLOY_E2E_SSH_KEY"
	status_json "$pass/status-rollback.json"
	"${PYTHON[@]}" "$ASSERT" --input "$pass/status-rollback.json" rollback --node-id "$NODE_ID" --source-revision "$revision_a" --after-revision "$revision_b" --digest "$DIGEST_A" --version 1.0.0 >"$pass/assert-rollback.json"
	invoke_echo "hello-$backend-rollback" "$pass/invoke-rollback.json"
	collect_diagnostics "$backend"
	run_to_log "$backend provider stop-reset" "$pass/provider-stop-reset.json" provider stop-reset
}

for backend in "${BACKENDS[@]}"; do
	ACTIVE_BACKEND="$backend"
	lifecycle "$backend"
	ACTIVE_BACKEND=""
done

exit 0
