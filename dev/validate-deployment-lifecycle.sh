#!/usr/bin/env bash
# Live deployment lifecycle validation against repository-configured disposable VMs.
set -Eeuo pipefail

ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$ROOT"
# shellcheck source=deployment-e2e/lifecycle_contract.sh
source "$ROOT/dev/deployment-e2e/lifecycle_contract.sh"
# shellcheck source=deployment-e2e/traffic_contract.sh
source "$ROOT/dev/deployment-e2e/traffic_contract.sh"
# Test-only deterministic entry used by deployment-e2e-contract-test. It runs
# the shared production tunnel/probe functions and exact lifecycle command seam
# against fake CLI, SSH, and provider processes without touching protected VMs.
if [ "${WRT_DEPLOY_CONTRACT_MODE:-0}" = 1 ]; then
	lifecycle_contract_fixture
	exit 0
fi
CONFIG="${WRT_DEPLOY_E2E_CONFIG:-dev/deployment-e2e.toml}"
PROVIDER="${WRT_DEPLOY_E2E_PROVIDER:-dev/deployment-e2e/proxmox.py}"
ASSERT="dev/deployment-e2e/assert_cluster.py"
ASSERT_OPERATION="dev/deployment-e2e/assert_operation.py"
BACKENDS=(systemd docker)
PRIMARY_STATUS=0
CLEANUP_STARTED=false
ACTIVE_BACKEND=""
ERROR_HANDLED=false
TUNNEL_PID=""
TUNNEL_PORT=""
PROBE_PID=""
PROBE_STOP_FILE=""

usage() {
	cat <<'USAGE'
Usage: dev/validate-deployment-lifecycle.sh [--backend systemd|docker]

All protected inputs are required. The three disposable deployment targets are
configured in dev/deployment-e2e.toml. With no --backend, the complete node
lifecycle runs under systemd and Docker; authenticated manager A→B→A deploy-set
qualification runs under systemd only. Compose manager deploy-set is unqualified
until an immutable registry or supported image-transfer mechanism exists.
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
MANAGER_B_HOST="$(config_value manager_b.host)"
MANAGER_B_USER="$(config_value manager_b.ssh_user)"
NODE_HOST="$(config_value node.host)"
NODE_USER="$(config_value node.ssh_user)"
NODE_ID="$(config_value node_id)"
WORKDIR="$(config_value workdir)"
LOCK_FILE="$(config_value lock_file)"
for host in "$MANAGER_HOST" "$MANAGER_B_HOST" "$NODE_HOST"; do
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
WRT_LIFECYCLE_TRACE="${WRT_LIFECYCLE_TRACE:-$LOG_BASE/lifecycle-trace.jsonl}"
export WRT_LIFECYCLE_TRACE
echo "logs: $LOG_BASE"
mkdir -p "$RUN_DIR/bin"
printf '#!/usr/bin/env bash\nexec %q -o BatchMode=yes -o StrictHostKeyChecking=yes -o UserKnownHostsFile=%q "$@"\n' "$REAL_SSH" "$KNOWN_HOSTS" >"$RUN_DIR/bin/ssh"
printf '#!/usr/bin/env bash\nexec %q -o BatchMode=yes -o StrictHostKeyChecking=yes -o UserKnownHostsFile=%q "$@"\n' "$REAL_SCP" "$KNOWN_HOSTS" >"$RUN_DIR/bin/scp"
chmod 700 "$RUN_DIR/bin/ssh" "$RUN_DIR/bin/scp"
export PATH="$RUN_DIR/bin:$PATH"
CERT_DIR="$RUN_DIR/certs"
MANAGER_CONFIG="$RUN_DIR/manager-a.toml"
MANAGER_BUNDLE="$RUN_DIR/manager-a.tar.gz"
MANAGER_ROLLOUT_DIR="$RUN_DIR/manager-rollout"
MANAGER_A_TO_B_MANIFEST="$MANAGER_ROLLOUT_DIR/manager-a-to-b-systemd.toml"
MANAGER_B_TO_A_MANIFEST="$MANAGER_ROLLOUT_DIR/manager-b-to-a-systemd.toml"
MANAGER_A_SET="$RUN_DIR/manager-credentials/gen3"
MANAGER_B_SET="$RUN_DIR/manager-credentials/gen2"
BASELINE_ONE="$RUN_DIR/baseline-one.tar.gz"
BASELINE_TWO="$RUN_DIR/baseline-two.tar.gz"
UPGRADE_TWO="$RUN_DIR/upgrade-two.tar.gz"
UPGRADE_ONE="$RUN_DIR/upgrade-one.tar.gz"
SCENARIO_MANIFEST="$RUN_DIR/scenario-manifest.json"
MANAGER_ADDR="https://${MANAGER_HOST}:9000"
MANAGER_B_ADDR="https://${MANAGER_B_HOST}:9000"
MANAGER_REMOTE="${MANAGER_USER}@${MANAGER_HOST}"
MANAGER_B_REMOTE="${MANAGER_B_USER}@${MANAGER_B_HOST}"
NODE_REMOTE="${NODE_USER}@${NODE_HOST}"
SSH=(timeout -k 5 60 ssh -i "$WRT_DEPLOY_E2E_SSH_KEY" -o ConnectTimeout=5)
CLI_ARGS=("$ROOT/target/debug/wr-cli" --manager "$MANAGER_ADDR" --ca-cert "$CERT_DIR/server-root/ca.crt" --client-cert "$CERT_DIR/human-client/leaf.pem" --client-key "$CERT_DIR/human-client/key.pem")
CLI=(lifecycle_run_short 60 "${CLI_ARGS[@]}")
MANAGER_B_CLI_ARGS=("$ROOT/target/debug/wr-cli" --manager "$MANAGER_B_ADDR" --ca-cert "$CERT_DIR/server-root/ca.crt" --client-cert "$CERT_DIR/human-client/leaf.pem" --client-key "$CERT_DIR/human-client/key.pem")

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
	if stop_probe; then :; else
		cleanup_status=$?
		record_promoted_cleanup_failure "EXIT traffic probe" "$cleanup_status"
	fi
	if stop_tunnel; then :; else
		cleanup_status=$?
		record_promoted_cleanup_failure "EXIT SSH tunnel" "$cleanup_status"
	fi
	collect_diagnostic "final provider status" \
		"$LOG_BASE/final-provider-status-before-reset.json" provider status
	if lifecycle_final_cleanup "${PYTHON[@]}" "$PROVIDER" --config "$CONFIG" stop-reset >"$LOG_BASE/final-provider-stop-reset.json" 2>&1; then
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
job_admin() {
	"${CLI[@]}" jobs "$@"
}
assert_job_summary() {
	"${PYTHON[@]}" - "$1" <<'PY'
import json, sys
value = json.load(open(sys.argv[1]))
assert value["total"] == 0, value
assert value["depth"] == 0, value
PY
}
wait_for_manager_set() {
	local output="$1" expected="$2" absent="$3" deadline tmp
	shift 3
	deadline=$((SECONDS + 30))
	tmp="${output}.tmp"
	while true; do
		if lifecycle_run_short 60 "$@" managers list >"$tmp" 2>&1 &&
			grep -Fq "$expected" "$tmp" && ! grep -Fq "$absent" "$tmp"; then
			mv "$tmp" "$output"
			return 0
		fi
		if [ "$SECONDS" -ge "$deadline" ]; then
			mv "$tmp" "$output"
			echo "manager membership did not converge to $expected without $absent" >&2
			return 1
		fi
		sleep 1
	done
}
assert_manager_rollout_trace() {
	"${PYTHON[@]}" - "$1" "$2" <<'PY'
import json, sys
path, generation = sys.argv[1], int(sys.argv[2])
values = [json.loads(line) for line in open(path) if line.strip()]
phases = [item['phase'] for item in values if item.get('event') == 'phase']
expected = ['PREPARED', 'STAGING', 'CLOSING_OLD', 'OLD_CLOSED', 'STARTING_TARGET', 'TARGET_READY_CLOSED', 'ACTIVATING_TARGET', 'COMPLETED']
assert phases == expected, (phases, expected)
assert all(item.get('endpoint_present') is True for item in values), values
assert all(item.get('target_generation') == generation for item in values), values
assert any(item.get('event') == 'barrier-start' for item in values), values
assert any(item.get('event') == 'lease-renewed' for item in values), values
assert any(item.get('event') == 'target-ready-closed' for item in values), values
assert any(item.get('event') == 'target-control-established' for item in values), values
assert any(item.get('event') == 'source-stop-completed' for item in values), values
assert values[-1].get('event') == 'cli-completed', values[-1]
assert all((item.get('barrier_timeout_seconds'), item.get('lease_ttl_seconds'), item.get('lease_renew_seconds')) == (120, 30, 10) for item in values)
PY
}
assert_job_queues() {
	"${PYTHON[@]}" - "$1" <<'PY'
import json, sys
value = json.load(open(sys.argv[1]))
assert value == [{
    "availability": "available",
    "fresh_delegates": 1,
    "job_queue_id": "deployment-jobs",
    "total_delegates": 1,
}], value
PY
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
write_manager_config() {
	"${PYTHON[@]}" - "$1" "$2" "$3" "$4" "$5" <<'PY'
import json, pathlib, sys, tomllib
source, output, policy_output, manager_endpoint, node_id = sys.argv[1:]
source_path = pathlib.Path(source)
config = source_path.read_text()
config_value = tomllib.loads(config)
manager_id = config_value["manager_id"]
configured_policy = pathlib.Path(config_value["authorization"]["policy_file"])
policy_source = configured_policy if configured_policy.is_absolute() else source_path.parent / configured_policy
policy = policy_source.read_text()
policy_value = tomllib.loads(policy)
manager_enrollments = [item for item in policy_value["manager_enrollments"] if item["manager_id"] == manager_id]
if len(manager_enrollments) != 1:
    raise SystemExit("deployment policy must enroll the configured manager exactly once")
if len(policy_value["proxy_enrollments"]) != 1 or len(policy_value["node_agent_enrollments"]) != 1:
    raise SystemExit("deployment policy must contain one proxy and one node-agent enrollment")
manager_enrollment = manager_enrollments[0]
proxy_enrollment = policy_value["proxy_enrollments"][0]
agent_enrollment = policy_value["node_agent_enrollments"][0]
cluster_id = policy_value["cluster_id"]
proxy_principal = f"urn:wruntime:{cluster_id}:proxy:{node_id}"
agent_principal = f"urn:wruntime:{cluster_id}:node-agent:{node_id}"
quote = json.dumps

def replace_exact(value, old, new, count):
    if value.count(old) != count:
        raise SystemExit(f"deployment policy expected {count} occurrences of {old}")
    return value.replace(old, new)

policy = replace_exact(policy, quote(manager_enrollment["endpoint"]), quote(manager_endpoint), 1)
policy = replace_exact(policy, quote(proxy_enrollment["principal"]), quote(proxy_principal), 2)
policy = replace_exact(policy, quote(agent_enrollment["principal"]), quote(agent_principal), 2)
policy = replace_exact(policy, quote(proxy_enrollment["node_id"]), quote(node_id), 2)
pathlib.Path(output).write_text(config)
policy_path = pathlib.Path(policy_output)
policy_path.parent.mkdir(parents=True, exist_ok=True)
policy_path.write_text(policy)
PY
}
assert_manager_b_db_reachable() {
	local quoted
	printf -v quoted '%q' "$WRT_DEPLOY_E2E_DB_URL"
	"${SSH[@]}" "$MANAGER_B_REMOTE" "PGCONNECT_TIMEOUT=5 timeout 10 psql $quoted -XAtqc 'SELECT 1'" | grep -Fx 1 >/dev/null
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
		lifecycle_run_watchdog "${CLI_ARGS[@]}" cluster status --node "$NODE_ID" --output json
	for bundle in "$BASELINE_ONE" "$BASELINE_TWO" "$UPGRADE_TWO" "$UPGRADE_ONE"; do
		collect_diagnostic "inspect $(basename "$bundle")" "$out/$(basename "$bundle").txt" \
			lifecycle_run_watchdog "${CLI_ARGS[@]}" node inspect-bundle "$bundle"
	done
	collect_diagnostic "prepared manifest" "$out/scenario-manifest.json" cat "$SCENARIO_MANIFEST"
	local label remote
	for label in manager-a manager-b; do
		if [ "$label" = manager-a ]; then remote="$MANAGER_REMOTE"; else remote="$MANAGER_B_REMOTE"; fi
		collect_diagnostic "$label SSH" "$out/$label-ssh.txt" "${SSH[@]}" "$remote" "hostname"
		collect_diagnostic "$label systemd status" "$out/$label-systemd.txt" \
			"${SSH[@]}" "$remote" "sudo systemctl status --no-pager 'wr-*'; sudo systemctl show wr-manager.service -p ActiveState -p SubState -p Result -p ExecMainStatus"
		collect_diagnostic "$label journal" "$out/$label-journal.txt" \
			"${SSH[@]}" "$remote" "sudo journalctl -q -u 'wr-*' -n 300 --no-pager"
		collect_diagnostic "$label process listeners" "$out/$label-process-listeners.txt" \
			"${SSH[@]}" "$remote" "ps -ef | grep '[w]r-manager' || true; sudo ss -lntp || true"
		collect_diagnostic "$label activation" "$out/$label-activation.txt" \
			"${SSH[@]}" "$remote" "sudo find /var/lib/wruntime/manager-activation -maxdepth 3 -type f -print -exec sha256sum {} \; -exec cat {} \; 2>/dev/null || true"
		collect_diagnostic "$label files" "$out/$label-files.txt" \
			"${SSH[@]}" "$remote" "sudo find '$WORKDIR' /var/lib/wruntime/manager-config -maxdepth 6 \( -type f -o -type l \) 2>/dev/null | sort"
		collect_diagnostic "$label compose" "$out/$label-compose.txt" \
			"${SSH[@]}" "$remote" "sudo docker ps -a --no-trunc 2>/dev/null || true; for compose in '$WORKDIR'/wr-manager/docker/docker-compose.yml; do test -f \"\$compose\" || continue; sudo docker compose --project-name wruntime-manager -f \"\$compose\" ps -a; sudo docker compose --project-name wruntime-manager -f \"\$compose\" logs --no-color --tail 300; done"
	done
	collect_diagnostic "node systemd status" "$out/node-systemd.txt" \
		"${SSH[@]}" "$NODE_REMOTE" "sudo systemctl status --no-pager 'wr-*'"
	collect_diagnostic "node journal" "$out/node-journal.txt" \
		"${SSH[@]}" "$NODE_REMOTE" "sudo journalctl -q -u 'wr-*' -n 300 --no-pager"
	collect_diagnostic "node files" "$out/node-files.txt" \
		"${SSH[@]}" "$NODE_REMOTE" "sudo find '$WORKDIR' -maxdepth 6 \( -type f -o -type l \) | sort"
	if [ "$backend" = docker ]; then
		collect_diagnostic "node compose" "$out/node-compose.txt" \
			"${SSH[@]}" "$NODE_REMOTE" "for compose in '$WORKDIR'/wr-node/releases/*/docker/docker-compose.yml; do test -f \"\$compose\" || continue; sudo docker compose --project-name wruntime-node -f \"\$compose\" ps -a; sudo docker compose --project-name wruntime-node -f \"\$compose\" images; sudo docker compose --project-name wruntime-node -f \"\$compose\" logs --no-color --tail 300; done"
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
run_logged build-deployment-probe cargo run --bin wr-cli -- dev build --config wr-tests/deployment/scenarios/baseline/engine-1.toml
run_logged build-workspace cargo build
run_logged build-node-host-binaries cargo zigbuild --release --target x86_64-unknown-linux-gnu \
	-p wr-proxy -p wr-engine -p wr-cli
mkdir -p "$CERT_DIR"
chmod 700 "$CERT_DIR"
run_logged cert-server-root target/debug/wr-cli cert init-root server --output "$CERT_DIR/server-root"
run_logged cert-client-root target/debug/wr-cli cert init-root client --output "$CERT_DIR/client-root"
run_logged cert-manager-endpoint target/debug/wr-cli cert issue manager-endpoint --ca-dir "$CERT_DIR/server-root" --endpoint "$MANAGER_HOST" --ip "$MANAGER_HOST" --destination "$CERT_DIR/manager-endpoint"
run_logged cert-manager-client target/debug/wr-cli cert issue manager --ca-dir "$CERT_DIR/client-root" --cluster-id deployment --name manager-a --destination "$CERT_DIR/manager-client"
run_logged cert-manager-b-endpoint target/debug/wr-cli cert issue manager-endpoint --ca-dir "$CERT_DIR/server-root" --endpoint "$MANAGER_B_HOST" --ip "$MANAGER_B_HOST" --destination "$CERT_DIR/manager-b-endpoint"
run_logged cert-manager-b-client target/debug/wr-cli cert issue manager --ca-dir "$CERT_DIR/client-root" --cluster-id deployment --name manager-b --destination "$CERT_DIR/manager-b-client"
run_logged cert-human-client target/debug/wr-cli cert issue human --ca-dir "$CERT_DIR/client-root" --cluster-id deployment --name deployer --destination "$CERT_DIR/human-client"
run_logged cert-proxy-endpoint target/debug/wr-cli cert issue proxy-peer-endpoint --ca-dir "$CERT_DIR/server-root" --endpoint "$NODE_HOST" --ip "$NODE_HOST" --destination "$CERT_DIR/proxy-endpoint"
run_logged cert-proxy-client target/debug/wr-cli cert issue proxy --ca-dir "$CERT_DIR/client-root" --cluster-id deployment --name "$NODE_ID" --destination "$CERT_DIR/proxy-client"
run_logged cert-node-agent target/debug/wr-cli cert issue node-agent --ca-dir "$CERT_DIR/client-root" --cluster-id deployment --name "$NODE_ID" --destination "$CERT_DIR/node-agent"
run_logged cert-engine-admin-endpoint target/debug/wr-cli cert issue engine-admin-endpoint --ca-dir "$CERT_DIR/server-root" --endpoint "$NODE_HOST" --ip "$NODE_HOST" --destination "$CERT_DIR/engine-admin-endpoint"
write_manager_config wr-tests/deployment/manager-a.toml "$MANAGER_CONFIG" "$RUN_DIR/policy/authorization.toml" "$MANAGER_ADDR" "$NODE_ID"
run_logged manager-bundle target/debug/wr-cli managers bundle --manager-config "$MANAGER_CONFIG" --output "$MANAGER_BUNDLE"
run_logged manager-inspect target/debug/wr-cli managers inspect-bundle "$MANAGER_BUNDLE"
MANAGER_EXTRACT="$RUN_DIR/manager-extract"
mkdir -p "$MANAGER_EXTRACT" "$MANAGER_A_SET/endpoint" "$MANAGER_A_SET/client" "$MANAGER_A_SET/roots" "$MANAGER_B_SET/endpoint" "$MANAGER_B_SET/client" "$MANAGER_B_SET/roots" "$MANAGER_ROLLOUT_DIR"
tar xzf "$MANAGER_BUNDLE" -C "$MANAGER_EXTRACT"
cp -a "$CERT_DIR/manager-endpoint/." "$MANAGER_A_SET/endpoint/"
cp -a "$CERT_DIR/manager-client/." "$MANAGER_A_SET/client/"
cp -a "$CERT_DIR/manager-b-endpoint/." "$MANAGER_B_SET/endpoint/"
cp -a "$CERT_DIR/manager-b-client/." "$MANAGER_B_SET/client/"
for set in "$MANAGER_A_SET" "$MANAGER_B_SET"; do
	cp "$CERT_DIR/client-root/ca.crt" "$set/roots/client-ca.crt"
	cp "$CERT_DIR/server-root/ca.crt" "$set/roots/server-ca.crt"
done
MANAGER_A_TEMPLATE="$MANAGER_EXTRACT/wr-manager/config/manager.toml"
MANAGER_B_TEMPLATE="$RUN_DIR/manager-b-template.toml"
INITIAL_A_CONFIG="$RUN_DIR/manager-a-initial-resolved.toml"
"${PYTHON[@]}" - "$MANAGER_A_TEMPLATE" wr-tests/deployment/manager-b.toml "$MANAGER_B_TEMPLATE" "$INITIAL_A_CONFIG" "$WRT_DEPLOY_E2E_DB_URL" "$MANAGER_ADDR" <<'PY'
import pathlib, sys
source, manager_b_source, manager_b, initial, db_url, endpoint = sys.argv[1:]
text = pathlib.Path(source).read_text()
b_text = pathlib.Path(manager_b_source).read_text()
replacements = {
    'policy_file = "policy/authorization.toml"': 'policy_file = "/var/lib/wruntime/manager-config/manager-b/authorization.toml"',
    'url             = "postgres://postgres@127.0.0.1:5432/wruntime_deployment_e2e"': 'url             = "{db_url}"',
    'advertise_grpc_address                = "https://127.0.0.1:9000"': 'advertise_grpc_address                = "{advertise_address}"',
    'cert_path           = "certs/manager.crt"': 'cert_path           = "/etc/wruntime/pki/manager-endpoint/sets/v1/leaf.pem"',
    'key_path            = "certs/manager.key"': 'key_path            = "/etc/wruntime/pki/manager-endpoint/sets/v1/key.pem"',
    'client_ca_cert_path = "certs/client-root/ca.crt"': 'client_ca_cert_path = "/etc/wruntime/pki/roots/client/ca.crt"',
    'cert_path           = "certs/manager-client/leaf.pem"': 'cert_path           = "/etc/wruntime/pki/manager-client/sets/v1/leaf.pem"',
    'key_path            = "certs/manager-client/key.pem"': 'key_path            = "/etc/wruntime/pki/manager-client/sets/v1/key.pem"',
    'server_ca_cert_path = "certs/server-root/ca.crt"': 'server_ca_cert_path = "/etc/wruntime/pki/roots/server/ca.crt"',
}
for old, new in replacements.items():
    if b_text.count(old) != 1:
        raise SystemExit(f'manager B fixture expected one {old}')
    b_text = b_text.replace(old, new)
pathlib.Path(manager_b).write_text(b_text)
resolved = text.replace('{db_url}', db_url).replace('{advertise_address}', endpoint)
if '{' in resolved or '}' in resolved:
    raise SystemExit('initial manager A config contains unresolved variables')
pathlib.Path(initial).write_text(resolved)
PY
run_logged manager-rollout-render "${PYTHON[@]}" dev/deployment-e2e/manager_rollout_fixture.py \
	--base-policy "$RUN_DIR/policy/authorization.toml" --manager-a-template "$MANAGER_A_TEMPLATE" --manager-b-template "$MANAGER_B_TEMPLATE" \
	--initial-a-config "$INITIAL_A_CONFIG" --binary "$MANAGER_EXTRACT/wr-manager/bin/wr-manager" --unit "$MANAGER_EXTRACT/wr-manager/systemd/wr-manager.service" \
	--a-initial-credential "$CERT_DIR/manager-endpoint" --a-set "$MANAGER_A_SET" --b-set "$MANAGER_B_SET" --output-dir "$MANAGER_ROLLOUT_DIR" \
	--a-endpoint "$MANAGER_ADDR" --b-endpoint "$MANAGER_B_ADDR" --a-remote "$MANAGER_REMOTE" --b-remote "$MANAGER_B_REMOTE" \
	--db-url "$WRT_DEPLOY_E2E_DB_URL" --ssh-key "$WRT_DEPLOY_E2E_SSH_KEY"
BASELINE_DIR=wr-tests/deployment/scenarios/baseline
UPGRADE_DIR=wr-tests/deployment/scenarios/upgrade
run_logged baseline-one-bundle target/debug/wr-cli node bundle --engine-config "$BASELINE_DIR/engine-1.toml" --proxy-config wr-tests/deployment/proxy.toml --skip-build --output "$BASELINE_ONE"
run_logged baseline-two-bundle target/debug/wr-cli node bundle --engine-config "$BASELINE_DIR/engine-1.toml" --engine-config "$BASELINE_DIR/engine-2.toml" --proxy-config wr-tests/deployment/proxy.toml --skip-build --output "$BASELINE_TWO"
run_logged upgrade-two-bundle target/debug/wr-cli node bundle --engine-config "$UPGRADE_DIR/engine-1.toml" --engine-config "$UPGRADE_DIR/engine-2.toml" --proxy-config wr-tests/deployment/proxy.toml --skip-build --output "$UPGRADE_TWO"
run_logged upgrade-one-bundle target/debug/wr-cli node bundle --engine-config "$UPGRADE_DIR/engine-1.toml" --proxy-config wr-tests/deployment/proxy.toml --skip-build --output "$UPGRADE_ONE"
for role in baseline-one baseline-two upgrade-two upgrade-one; do
	bundle_var="${role//-/_}"; bundle_var="${bundle_var^^}"
	bundle="${!bundle_var}"
	run_logged "$role-inspect" target/debug/wr-cli node inspect-bundle "$bundle"
done
"${PYTHON[@]}" - "$SCENARIO_MANIFEST" "$LOG_BASE" "$BASELINE_ONE" "$BASELINE_TWO" "$UPGRADE_TWO" "$UPGRADE_ONE" "$MANAGER_BUNDLE" <<'PY'
import hashlib, json, pathlib, re, sys, tomllib
output, logs, *paths = map(pathlib.Path, sys.argv[1:])
manager_path = paths.pop()
roles = ("baseline-one", "baseline-two", "upgrade-two", "upgrade-one")
expected = ({"engine-1"}, {"engine-1", "engine-2"}, {"engine-1", "engine-2"}, {"engine-1"})
configs = {
    "baseline-one": ["wr-tests/deployment/scenarios/baseline/engine-1.toml"],
    "baseline-two": ["wr-tests/deployment/scenarios/baseline/engine-1.toml", "wr-tests/deployment/scenarios/baseline/engine-2.toml"],
    "upgrade-two": ["wr-tests/deployment/scenarios/upgrade/engine-1.toml", "wr-tests/deployment/scenarios/upgrade/engine-2.toml"],
    "upgrade-one": ["wr-tests/deployment/scenarios/upgrade/engine-1.toml"],
}
entries = []
for role, path, wanted in zip(roles, paths, expected):
    inspect = (logs / f"{role}-inspect.log").read_text()
    digest = re.search(r"^\s*digest:\s+(\S+)$", inspect, re.M)
    slots = set(re.findall(r"^  (engine-[12])$", inspect, re.M))
    if digest is None or slots != wanted:
        raise SystemExit(f"{role} inspected identity mismatch: slots={sorted(slots)}")
    endpoints, versions = {}, set()
    for config_path in configs[role]:
        value = tomllib.load(open(config_path, "rb"))
        slot = pathlib.Path(config_path).stem
        endpoints[slot] = {"http": value["listen_address"], "job_admin": value["job_admin"]["listen_address"], "advertise": value["job_admin"]["advertise_address"]}
        versions.update(module["version"] for module in value["module"])
    entries.append({"role": role, "path": str(path.resolve()), "slots": sorted(slots), "bundle_digest": digest.group(1), "module_versions": sorted(versions), "endpoints": endpoints, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
if entries[1]["endpoints"] != entries[2]["endpoints"] or len({v for endpoints in entries[1]["endpoints"].values() for v in (endpoints["http"], endpoints["job_admin"])}) != 4:
    raise SystemExit("slot endpoints are not stable and collision-free")
manager_sha = hashlib.sha256(manager_path.read_bytes()).hexdigest()
entries.append({"role": "manager", "path": str(manager_path.resolve()), "slots": [], "bundle_digest": f"sha256:{manager_sha}", "module_versions": [], "endpoints": {}, "sha256": manager_sha})
output.write_text(json.dumps({"schema_version": 1, "artifacts": entries}, indent=2) + "\n")
PY
"${PYTHON[@]}" - "$SCENARIO_MANIFEST" "$MANAGER_ROLLOUT_DIR" "$MANAGER_A_SET" "$MANAGER_B_SET" "$RUN_DIR/policy/authorization.toml" "$INITIAL_A_CONFIG" "$MANAGER_EXTRACT/wr-manager/bin/wr-manager" "$MANAGER_EXTRACT/wr-manager/systemd/wr-manager.service" <<'PY'
import hashlib, json, pathlib, sys
manifest = pathlib.Path(sys.argv[1])
value = json.loads(manifest.read_text())
paths = []
for name in sys.argv[2:5]:
    paths.extend(path for path in pathlib.Path(name).rglob('*') if path.is_file())
paths.extend(pathlib.Path(name) for name in sys.argv[5:])
for index, path in enumerate(paths):
    data = path.read_bytes()
    value['artifacts'].append({
        'role': f'manager-rollout-{index}-{path.name}', 'path': str(path.resolve()),
        'slots': [], 'bundle_digest': 'sha256:' + hashlib.sha256(data).hexdigest(),
        'module_versions': [], 'endpoints': {}, 'sha256': hashlib.sha256(data).hexdigest(),
    })
manifest.write_text(json.dumps(value, indent=2) + '\n')
PY
chmod 444 "$SCENARIO_MANIFEST" "$MANAGER_BUNDLE" "$BASELINE_ONE" "$BASELINE_TWO" "$UPGRADE_TWO" "$UPGRADE_ONE"
find "$MANAGER_ROLLOUT_DIR" "$MANAGER_A_SET" "$MANAGER_B_SET" -type f -exec chmod 444 {} +
cp "$SCENARIO_MANIFEST" "$LOG_BASE/scenario-manifest.json"
verify_manifest() { lifecycle_verify_manifest "$SCENARIO_MANIFEST"; }
manifest_value() { "${PYTHON[@]}" - "$SCENARIO_MANIFEST" "$1" "$2" <<'PY'
import json, sys
item = next(value for value in json.load(open(sys.argv[1]))["artifacts"] if value["role"] == sys.argv[2])
print(item[sys.argv[3]])
PY
}
DIGEST_A_ONE="$(manifest_value baseline-one bundle_digest)"
DIGEST_A_TWO="$(manifest_value baseline-two bundle_digest)"
DIGEST_B_TWO="$(manifest_value upgrade-two bundle_digest)"
DIGEST_B_ONE="$(manifest_value upgrade-one bundle_digest)"
verify_manifest
lifecycle_artifact prepare "$SCENARIO_MANIFEST" "$(sha256sum "$SCENARIO_MANIFEST" | awk '{print $1}')"

provision_node_agent_fixture() {
	local backend="$1" fixture="$RUN_DIR/node-agent-$1" binary_digest backend_config
	mkdir -p "$fixture"
	tar -xOf "$BASELINE_ONE" wr-node/agent/wr-cli >"$fixture/wr-cli"
	# A valid ELF tolerates trailing bytes. Give the provisioned baseline a distinct
	# digest so the updater must stage and atomically replace it with bundle bytes.
	printf '\0' >>"$fixture/wr-cli"
	tar -xOf "$BASELINE_ONE" wr-node/agent/wr-node-agent.service >"$fixture/wr-node-agent.service"
	chmod 700 "$fixture/wr-cli"
	if [ "$backend" = systemd ]; then
		backend_config=$'backend = "systemd"\ncompose-project = ""\nsystemctl-path = "/usr/bin/systemctl"\ndocker-path = ""'
	else
		backend_config=$'backend = "docker"\ncompose-project = "wruntime-node"\nsystemctl-path = ""\ndocker-path = "/usr/bin/docker"'
	fi
	cat >"$fixture/agent.toml" <<EOF
policy-version = 1
node-id = "$NODE_ID"
manager-endpoint = "$MANAGER_ADDR"
client-cert-path = "$WORKDIR/wr-agent/certs/agent.crt"
client-key-path = "$WORKDIR/wr-agent/certs/agent.key"
ca-cert-path = "$WORKDIR/wr-agent/certs/ca.crt"
deployment-root = "$WORKDIR"
runtime-dir = "/run/wruntime"
$backend_config
poll-interval-seconds = 5
renew-interval-seconds = 5
protocol-version = "operator-engine-lifecycle-v1"
capabilities = ["continuous-lease-v1", "manager-authorized-retention-v1", "release-metadata-v1", "typed-backend-v1"]
EOF
	binary_digest="sha256:$(sha256sum "$fixture/wr-cli" | cut -d' ' -f1)"
	timeout -k 5 60 scp -i "$WRT_DEPLOY_E2E_SSH_KEY" \
		"$fixture/wr-cli" "$fixture/agent.toml" "$fixture/wr-node-agent.service" \
		"$CERT_DIR/node-agent/leaf.pem" "$CERT_DIR/node-agent/key.pem" \
		"$CERT_DIR/server-root/ca.crt" "$NODE_REMOTE:/tmp/"
	"${SSH[@]}" "$NODE_REMOTE" "sudo install -d -o root -g root -m 0755 '$WORKDIR' '$WORKDIR/wr-node' '$WORKDIR/wr-node/slots'; sudo install -d -o root -g root -m 0700 '$WORKDIR/wr-agent' '$WORKDIR/wr-agent/certs' /run/wruntime; sudo install -o root -g root -m 0755 /tmp/wr-cli '$WORKDIR/wr-agent/wr-cli'; sudo install -o root -g root -m 0600 /tmp/agent.toml '$WORKDIR/wr-agent/agent.toml'; sudo install -o root -g root -m 0600 /tmp/leaf.pem '$WORKDIR/wr-agent/certs/agent.crt'; sudo install -o root -g root -m 0600 /tmp/key.pem '$WORKDIR/wr-agent/certs/agent.key'; sudo install -o root -g root -m 0600 /tmp/ca.crt '$WORKDIR/wr-agent/certs/ca.crt'; sudo install -o root -g root -m 0644 /tmp/wr-node-agent.service /etc/systemd/system/wr-node-agent.service"
	PGCONNECT_TIMEOUT=5 psql "$WRT_DEPLOY_E2E_DB_URL" -Xv ON_ERROR_STOP=1 -c "SET search_path=wr_system,public; INSERT INTO wr_nodes(node_id) VALUES ('$NODE_ID') ON CONFLICT DO NOTHING; INSERT INTO wr_node_agent_policies(node_id,protocol_version,backend,retention_count,actor,binary_digest,capabilities) VALUES ('$NODE_ID','operator-engine-lifecycle-v1','$backend',3,'deployment-e2e','$binary_digest',ARRAY['continuous-lease-v1','manager-authorized-retention-v1','release-metadata-v1','typed-backend-v1']) ON CONFLICT(node_id) DO UPDATE SET protocol_version=EXCLUDED.protocol_version,backend=EXCLUDED.backend,retention_count=EXCLUDED.retention_count,actor=EXCLUDED.actor,binary_digest=EXCLUDED.binary_digest,capabilities=EXCLUDED.capabilities,updated_at=NOW();"
	"${SSH[@]}" "$NODE_REMOTE" "sudo systemctl daemon-reload && sudo systemctl enable --now wr-node-agent.service && sudo systemctl is-active --quiet wr-node-agent.service"
}

lifecycle() {
	local backend="$1"
	local pass="$LOG_BASE/$backend"
	mkdir -p "$pass"
	echo "==> deployment lifecycle: $backend"
	lifecycle_reset_boundary "$backend-entry"
	run_to_log "$backend provider reset" "$pass/provider-reset.json" provider reset
	if [ "$backend" = systemd ]; then
		run_to_log "manager-b-db-preflight" "$pass/manager-b-db-preflight.log" assert_manager_b_db_reachable
	fi
	verify_manifest
	lifecycle_artifact "$backend" "$SCENARIO_MANIFEST" "$(sha256sum "$SCENARIO_MANIFEST" | awk '{print $1}')"
	assert_db_clean
	if [ "$backend" = docker ]; then
		"${SSH[@]}" "$MANAGER_REMOTE" "sudo -n docker info >/dev/null && sudo -n docker compose version >/dev/null"
		"${SSH[@]}" "$NODE_REMOTE" "sudo -n docker info >/dev/null && sudo -n docker compose version >/dev/null"
	fi

	# Secrets are passed directly to wr-cli and are never included in an echoed command transcript.
	run_to_log "$backend manager deploy" "$pass/manager-deploy.log" \
		lifecycle_run_short 300 "${CLI_ARGS[@]}" managers deploy "$MANAGER_BUNDLE" "$MANAGER_REMOTE" --format "$backend" \
		--db-url "$WRT_DEPLOY_E2E_DB_URL" --secret-key "$WRT_SECRET_ENCRYPTION_KEY" \
		--ssh-key "$WRT_DEPLOY_E2E_SSH_KEY" --cert-dir "$CERT_DIR" \
		--advertise-address "$MANAGER_ADDR"
	status_json "$pass/manager-status.json"
	"${PYTHON[@]}" "$ASSERT" --input "$pass/manager-status.json" manager --address "$MANAGER_ADDR" >"$pass/manager-assert.json"
	run_to_log "$backend node agent fixture provisioning and initial activation" "$pass/node-agent-provision.log" provision_node_agent_fixture "$backend"
	run_to_log "$backend node agent binary update restart and fresh activation" "$pass/node-agent-install.log" \
		"${CLI[@]}" node agent install "$BASELINE_ONE" "$NODE_REMOTE" --node-id "$NODE_ID" \
		--format "$backend" --ssh-key "$WRT_DEPLOY_E2E_SSH_KEY"
	job_admin queues --format json >"$pass/job-queues-empty.json"
	if "$ROOT/target/debug/wr-cli" --manager "$MANAGER_ADDR" \
		--ca-cert "$CERT_DIR/server-root/ca.crt" \
		--client-cert "$CERT_DIR/proxy-client/leaf.pem" \
		--client-key "$CERT_DIR/proxy-client/key.pem" \
		jobs queues --format json >"$pass/job-role-trust-unexpected.json" 2>&1; then
		echo "proxy workload credential unexpectedly received manager job authorization" >&2
		return 1
	fi

	run_to_log "$backend node A deploy" "$pass/deploy-a.log" \
		lifecycle_run_deploy_operation "$backend-deploy-a" "${CLI_ARGS[@]}" node deploy --node-id "$NODE_ID" "$BASELINE_ONE" "$NODE_REMOTE" --format "$backend" \
		--db-url "$WRT_DEPLOY_E2E_DB_URL" --ssh-key "$WRT_DEPLOY_E2E_SSH_KEY" --cert-dir "$CERT_DIR"
	status_json "$pass/status-a.json"
	"${PYTHON[@]}" "$ASSERT" --input "$pass/status-a.json" desired --node-id "$NODE_ID" --digest "$DIGEST_A_ONE" --version 1.0.0 --engine-slot engine-1 >"$pass/assert-a.json"
	job_admin queues --format json >"$pass/job-queues-a.json"
	assert_job_queues "$pass/job-queues-a.json"
	job_admin summary --queue deployment-jobs --format json >"$pass/job-summary-a.json"
	assert_job_summary "$pass/job-summary-a.json"
	local revision_a revision_b revision_contracted
	revision_a="$(revision_from "$pass/status-a.json")"
	invoke_probe "probe-$backend-a" "$pass/invoke-a.json"

	local failed_status retry_token="$backend-finalized-retry" staged_revision
	if lifecycle_run_deploy_operation "$backend-deploy-finalize" "${CLI_ARGS[@]}" node deploy --node-id "$NODE_ID" "$UPGRADE_TWO" "$NODE_REMOTE" --format "$backend" \
		--request-token "$retry_token" --allow-downtime --exit-after-finalization \
		--db-url "$WRT_DEPLOY_E2E_DB_URL" \
		--ssh-key "$WRT_DEPLOY_E2E_SSH_KEY" --cert-dir "$CERT_DIR" >"$pass/interrupted-after-finalization.log" 2>&1; then
		failed_status=0
	else
		failed_status=$?
	fi
	[ "$failed_status" -eq 1 ] && grep -Fq "deterministic exit after inactive release finalization" "$pass/interrupted-after-finalization.log" || {
		echo "post-finalization fault hook did not produce its documented injected CLI exit" >&2
		return 1
	}
	status_json "$pass/status-finalized-only.json"
	staged_revision="$("${PYTHON[@]}" - "$pass/status-finalized-only.json" "$NODE_ID" "$DIGEST_B_TWO" <<'PY'
import json, sys
value, node_id, digest = json.load(open(sys.argv[1])), sys.argv[2], sys.argv[3]
node = next(item for item in value["nodes"] if item["node_id"] == node_id)
target = node.get("target_deployment") or {}
if target.get("bundle_digest") != digest or not target.get("resolved_release_digest"):
    raise SystemExit("fault hook did not leave one finalized exact target")
print(target["revision"])
PY
)"

	run_to_log "$backend node B same-token retry" "$pass/deploy-b.log" \
		lifecycle_run_deploy_operation "$backend-deploy-retry" "${CLI_ARGS[@]}" node deploy --node-id "$NODE_ID" "$UPGRADE_TWO" "$NODE_REMOTE" --format "$backend" \
		--request-token "$retry_token" --allow-downtime --db-url "$WRT_DEPLOY_E2E_DB_URL" \
		--ssh-key "$WRT_DEPLOY_E2E_SSH_KEY" --cert-dir "$CERT_DIR"
	status_json "$pass/status-b.json"
	"${PYTHON[@]}" "$ASSERT" --input "$pass/status-b.json" desired --node-id "$NODE_ID" --digest "$DIGEST_B_TWO" --version 2.0.0 --engine-slot engine-1 --engine-slot engine-2 >"$pass/assert-b.json"
	revision_b="$(revision_from "$pass/status-b.json")"
	[ "$revision_b" -eq "$staged_revision" ]
	lifecycle_capture_operation_detail "$NODE_ID" "$retry_token" "$pass/operation-deploy-b.json" "${CLI_ARGS[@]}"
	"${PYTHON[@]}" "$ASSERT_OPERATION" --input "$pass/operation-deploy-b.json" \
		--node-id "$NODE_ID" --request-token "$retry_token" --action deployment \
		--target-revision "$staged_revision" --target-digest "$DIGEST_B_TWO" \
		--slot-order engine-2 --slot-order engine-1 --stopped-engine-slot engine-1 --expect-proxy-stop >"$pass/assert-operation-deploy-b.json"
	invoke_probe "probe-$backend-b" "$pass/invoke-b.json"

	run_to_log "$backend durable engine restart" "$pass/restart.log" \
		lifecycle_run_deploy_operation "$backend-restart" "${CLI_ARGS[@]}" engines restart --node-id "$NODE_ID" --slot engine-1 \
		--request-token "$backend-restart"
	lifecycle_capture_operation_detail "$NODE_ID" "$backend-restart" "$pass/operation-restart.json" "${CLI_ARGS[@]}"
	"${PYTHON[@]}" "$ASSERT_OPERATION" --input "$pass/operation-restart.json" \
		--node-id "$NODE_ID" --request-token "$backend-restart" --action restart \
		--slot-order engine-1 --stopped-engine-slot engine-1 >"$pass/assert-operation-restart.json"
	invoke_probe "probe-$backend-restart" "$pass/invoke-restart.json"

	run_to_log "$backend node inventory contraction" "$pass/contract.log" \
		lifecycle_run_deploy_operation "$backend-contract" "${CLI_ARGS[@]}" node deploy --node-id "$NODE_ID" "$BASELINE_ONE" "$NODE_REMOTE" --format "$backend" \
		--request-token "$backend-contract" --db-url "$WRT_DEPLOY_E2E_DB_URL" \
		--ssh-key "$WRT_DEPLOY_E2E_SSH_KEY" --cert-dir "$CERT_DIR"
	status_json "$pass/status-contract.json"
	"${PYTHON[@]}" "$ASSERT" --input "$pass/status-contract.json" desired --node-id "$NODE_ID" --digest "$DIGEST_A_ONE" --version 1.0.0 --engine-slot engine-1 >"$pass/assert-contract.json"
	revision_contracted="$(revision_from "$pass/status-contract.json")"
	lifecycle_capture_operation_detail "$NODE_ID" "$backend-contract" "$pass/operation-contract.json" "${CLI_ARGS[@]}"
	"${PYTHON[@]}" "$ASSERT_OPERATION" --input "$pass/operation-contract.json" \
		--node-id "$NODE_ID" --request-token "$backend-contract" --action deployment \
		--target-digest "$DIGEST_A_ONE" --slot-order engine-1 --slot-order engine-2 \
		--stopped-engine-slot engine-1 --stopped-engine-slot engine-2 --expect-proxy-stop >"$pass/assert-operation-contract.json"
	invoke_probe "probe-$backend-contract" "$pass/invoke-contract.json"

	local rollback_token="$backend-rollback"
	run_to_log "$backend node rollback" "$pass/rollback.log" \
		lifecycle_run_deploy_operation "$backend-rollback" "${CLI_ARGS[@]}" node rollback "$NODE_REMOTE" --node-id "$NODE_ID" --to "$revision_b" \
		--request-token "$rollback_token" --ssh-key "$WRT_DEPLOY_E2E_SSH_KEY"
	lifecycle_capture_operation_detail "$NODE_ID" "$rollback_token" "$pass/operation-rollback.json" "${CLI_ARGS[@]}"
	"${PYTHON[@]}" "$ASSERT_OPERATION" --input "$pass/operation-rollback.json" \
		--node-id "$NODE_ID" --request-token "$rollback_token" --action rollback \
		--target-digest "$DIGEST_B_TWO" --slot-order engine-2 --slot-order engine-1 \
		--stopped-engine-slot engine-1 --expect-proxy-stop >"$pass/assert-operation-rollback.json"
	status_json "$pass/status-rollback.json"
	"${PYTHON[@]}" "$ASSERT" --input "$pass/status-rollback.json" rollback --node-id "$NODE_ID" --source-revision "$revision_b" --after-revision "$revision_contracted" --digest "$DIGEST_B_TWO" --version 2.0.0 --engine-slot engine-1 --engine-slot engine-2 >"$pass/assert-rollback.json"
	invoke_probe "probe-$backend-rollback" "$pass/invoke-rollback.json"
	[ "$revision_a" -lt "$revision_b" ]

	if [ "$backend" = systemd ]; then
		local initial_selector manager_a_trace manager_b_trace
		initial_selector="$("${PYTHON[@]}" -c 'import json,sys; print(json.load(open(sys.argv[1]))["initial_a_selector_digest"])' "$MANAGER_ROLLOUT_DIR/manager-rollout-artifacts.json")"
		"${SSH[@]}" "$MANAGER_REMOTE" "test \"sha256:\$(sudo sha256sum /var/lib/wruntime/manager-activation/current-activation.json | cut -d' ' -f1)\" = '$initial_selector'"
		printf 'WRT_SECRET_ENCRYPTION_KEY=%s\nWRT_LIFECYCLE_INSTANCE_ID=manager-rollout-b\n' "$WRT_SECRET_ENCRYPTION_KEY" | \
			"${SSH[@]}" "$MANAGER_B_REMOTE" "sudo install -d -m 0700 /var/lib/wruntime/manager-secrets && sudo install -m 0600 /dev/stdin /var/lib/wruntime/manager-secrets/runtime.env"
		manager_a_trace="$pass/manager-a-to-b-trace.jsonl"
		manager_b_trace="$pass/manager-b-to-a-trace.jsonl"
		run_to_log "manager A to B deploy-set" "$pass/manager-a-to-b.log" lifecycle_run_manager_rollout a-to-b "$manager_a_trace" \
			"${CLI_ARGS[@]}" managers deploy-set --manifest "$MANAGER_A_TO_B_MANIFEST"
		assert_manager_rollout_trace "$manager_a_trace" 2
		wait_for_manager_set "$pass/managers-through-b.txt" manager-b manager-a "${MANAGER_B_CLI_ARGS[@]}"
		"${SSH[@]}" "$MANAGER_REMOTE" "! sudo systemctl is-active --quiet wr-manager.service && test \"\$(sudo systemctl is-enabled wr-manager.service)\" = masked-runtime"
		"${SSH[@]}" "$MANAGER_B_REMOTE" "sudo systemctl is-active --quiet wr-manager.service && test \"\$(sudo systemctl is-enabled wr-manager.service)\" = enabled"
		run_to_log "manager B to A deploy-set" "$pass/manager-b-to-a.log" lifecycle_run_manager_rollout b-to-a "$manager_b_trace" \
			"${MANAGER_B_CLI_ARGS[@]}" managers deploy-set --manifest "$MANAGER_B_TO_A_MANIFEST"
		assert_manager_rollout_trace "$manager_b_trace" 3
		wait_for_manager_set "$pass/managers-through-a-restored.txt" manager-a manager-b "${CLI_ARGS[@]}"
		"${SSH[@]}" "$MANAGER_B_REMOTE" "! sudo systemctl is-active --quiet wr-manager.service && test \"\$(sudo systemctl is-enabled wr-manager.service)\" = masked-runtime"
		"${SSH[@]}" "$MANAGER_REMOTE" "sudo systemctl is-active --quiet wr-manager.service && test \"\$(sudo systemctl is-enabled wr-manager.service)\" = enabled"
	fi
	verify_manifest
	collect_diagnostics "$backend"
}

for backend in "${BACKENDS[@]}"; do
	ACTIVE_BACKEND="$backend"
	lifecycle "$backend"
	ACTIVE_BACKEND=""
done

exit 0
