#!/usr/bin/env bash
# Shared deployment lifecycle deadlines, ordering, reset, and artifact trace seam.
# This file is sourced by the protected harness and executed through its
# contract-only entry mode by the fast test suite.

LIFECYCLE_DURABLE_SECONDS=1800
LIFECYCLE_CLI_WAIT_SECONDS=1860
LIFECYCLE_WATCHDOG_SECONDS=1920
LIFECYCLE_CLEANUP_SECONDS=300
MANAGER_ROLLOUT_BARRIER_SECONDS=120
MANAGER_ROLLOUT_WATCHDOG_SECONDS=180

lifecycle_now() {
	if [ -n "${WRT_CONTRACT_CLOCK:-}" ]; then
		"$WRT_CONTRACT_CLOCK"
	else
		date +%s
	fi
}

lifecycle_trace() {
	local kind="$1" name="$2" detail="${3:-}"
	[ -n "${WRT_LIFECYCLE_TRACE:-}" ] || return 0
	python3 - "$WRT_LIFECYCLE_TRACE" "$kind" "$name" "$detail" <<'PY'
import json, pathlib, sys
path = pathlib.Path(sys.argv[1])
with path.open("a", encoding="utf-8") as stream:
    stream.write(json.dumps({"event": sys.argv[2], "name": sys.argv[3], "detail": sys.argv[4]}, sort_keys=True) + "\n")
PY
}

lifecycle_begin_operation() {
	local name="$1" now
	now="$(lifecycle_now)"
	LIFECYCLE_OPERATION_SUBMITTED_AT="$now"
	LIFECYCLE_DURABLE_CUTOFF=$((now + LIFECYCLE_DURABLE_SECONDS))
	LIFECYCLE_CLI_WAIT_CUTOFF=$((now + LIFECYCLE_CLI_WAIT_SECONDS))
	LIFECYCLE_WATCHDOG_CUTOFF=$((now + LIFECYCLE_WATCHDOG_SECONDS))
	export LIFECYCLE_OPERATION_SUBMITTED_AT LIFECYCLE_DURABLE_CUTOFF LIFECYCLE_CLI_WAIT_CUTOFF LIFECYCLE_WATCHDOG_CUTOFF
	lifecycle_trace operation "$name" "submitted=$now,durable=$LIFECYCLE_DURABLE_CUTOFF,cli_wait=$LIFECYCLE_CLI_WAIT_CUTOFF,watchdog=$LIFECYCLE_WATCHDOG_CUTOFF"
}

lifecycle_remaining() {
	local cutoff="$1" now remaining
	now="$(lifecycle_now)"
	remaining=$((cutoff - now))
	[ "$remaining" -gt 0 ] || {
		echo "deployment lifecycle deadline expired before mutation" >&2
		return 1
	}
	printf '%s\n' "$remaining"
}

lifecycle_run_short() {
	local budget="$1"
	shift
	timeout -k 5 "$budget" "$@"
}

lifecycle_run_watchdog() {
	local remaining
	if [ -n "${LIFECYCLE_WATCHDOG_CUTOFF:-}" ]; then
		remaining="$(lifecycle_remaining "$LIFECYCLE_WATCHDOG_CUTOFF")" || return
	else
		remaining=60
	fi
	timeout -k 5 "$remaining" "$@"
}

lifecycle_run_deploy_operation() {
	local name="$1"
	shift
	lifecycle_begin_operation "$name"
	local cli_wait watchdog
	cli_wait="$(lifecycle_remaining "$LIFECYCLE_CLI_WAIT_CUTOFF")" || return
	watchdog="$(lifecycle_remaining "$LIFECYCLE_WATCHDOG_CUTOFF")" || return
	lifecycle_trace mutation "$name" "deadline=$LIFECYCLE_DURABLE_SECONDS,wait_timeout=$cli_wait,watchdog=$watchdog"
	timeout -k 10 "$watchdog" "$@" --deadline "$LIFECYCLE_DURABLE_SECONDS" --wait-timeout "$cli_wait"
}

lifecycle_run_manager_rollout() {
	local stage="$1" trace="$2"
	shift 2
	lifecycle_trace manager-rollout "$stage" "barrier=$MANAGER_ROLLOUT_BARRIER_SECONDS,watchdog=$MANAGER_ROLLOUT_WATCHDOG_SECONDS,trace=$trace"
	WRT_MANAGER_ROLLOUT_TRACE="$trace" timeout -k 10 "$MANAGER_ROLLOUT_WATCHDOG_SECONDS" "$@"
}

lifecycle_expect_manager_failure() {
	local stage="$1" trace="$2" status
	shift 2
	if lifecycle_run_manager_rollout "$stage" "$trace" "$@"; then
		echo "manager stage unexpectedly succeeded: $stage" >&2
		return 1
	else
		status=$?
	fi
	lifecycle_trace manager-failure "$stage" "status=$status"
}

lifecycle_capture_operation_detail() {
	local node_id="$1" request_token="$2" artifact="$3"
	shift 3
	local list_artifact="${artifact%.json}.list.json" operation_id
	lifecycle_run_watchdog "$@" operations list --node-id "$node_id" --include-terminal --json >"$list_artifact"
	operation_id="$(python3 - "$list_artifact" "$node_id" "$request_token" <<'PY'
import json, sys
operations = json.load(open(sys.argv[1]))
matches = [item for item in operations
           if item.get("node_id") == sys.argv[2]
           and item.get("request_token") == sys.argv[3]]
if len(matches) != 1:
    raise SystemExit(f"expected exactly one operation for request token, found {len(matches)}")
if matches[0].get("state") in ("queued", "running"):
    raise SystemExit("operation detail capture requires a terminal operation")
print(matches[0]["operation_id"])
PY
)" || return
	lifecycle_run_watchdog "$@" operations get "$operation_id" --json >"$artifact"
	lifecycle_trace operation-detail "$request_token" "artifact=$artifact,operation_id=$operation_id"
}

lifecycle_artifact() {
	local role="$1" path="$2" digest="${3:-}"
	lifecycle_trace artifact "$role" "path=$path,digest=$digest"
}

lifecycle_verify_manifest() {
	local manifest="$1"
	python3 - "$manifest" <<'PY'
import hashlib, json, pathlib, sys
value = json.load(open(sys.argv[1]))
if value.get("schema_version") != 1 or not isinstance(value.get("artifacts"), list):
    raise SystemExit("invalid deployment scenario manifest")
for item in value["artifacts"]:
    path = pathlib.Path(item["path"])
    if not path.is_absolute():
        raise SystemExit(f"manifest path is not absolute: {path}")
    actual = hashlib.sha256(path.read_bytes()).hexdigest()
    if actual != item.get("sha256"):
        raise SystemExit(f"prepared artifact mutated: {item.get('role')}")
PY
}

lifecycle_reset_boundary() {
	lifecycle_trace reset "$1" clean-entry
}

lifecycle_final_cleanup() {
	lifecycle_trace cleanup final "budget=$LIFECYCLE_CLEANUP_SECONDS"
	timeout -k 10 "$LIFECYCLE_CLEANUP_SECONDS" "$@"
}

# Deterministic executable contract used only through the real harness entry.
lifecycle_contract_fixture() {
	: "${WRT_CONTRACT_PROVIDER:?contract provider is required}"
	: "${WRT_CONTRACT_CLI:?contract CLI is required}"
	if [ -n "${WRT_CONTRACT_TRAFFIC_DIR:-}" ]; then
		PYTHON=(python3)
		CLI_ARGS=("$WRT_CONTRACT_CLI")
		WRT_DEPLOY_E2E_SSH_KEY="${WRT_CONTRACT_SSH_KEY:?contract SSH key is required}"
		NODE_REMOTE=contract@127.0.0.1
		TUNNEL_PID=""
		TUNNEL_PORT=""
		PROBE_PID=""
		PROBE_STOP_FILE=""
	fi
	lifecycle_artifact prepare manifest-a sha256:fixture
	for backend in systemd docker; do
		lifecycle_reset_boundary "$backend-entry"
		"$WRT_CONTRACT_PROVIDER" reset
		lifecycle_artifact "$backend" manifest-a sha256:fixture
		lifecycle_run_deploy_operation "$backend-addition" "$WRT_CONTRACT_CLI" node deploy --bundle baseline-two.tar.gz
		local injected_status submitted_at completed_at probe_log=""
		if [ -n "${WRT_CONTRACT_TRAFFIC_DIR:-}" ]; then
			probe_log="$WRT_CONTRACT_TRAFFIC_DIR/$backend-probe.jsonl"
			start_tunnel "$WRT_CONTRACT_TRAFFIC_DIR/$backend-tunnel.log"
			invoke_probe_over_tunnel probe "$WRT_CONTRACT_TRAFFIC_DIR/$backend-pre.json"
			start_probe probe "$probe_log"
		fi
		if lifecycle_run_deploy_operation "$backend-replacement-finalize" "$WRT_CONTRACT_CLI" node deploy --request-token replacement-token --exit-after-finalization; then
			injected_status=0
		else
			injected_status=$?
		fi
		[ "$injected_status" -eq 1 ] || { echo "documented finalization fault was not observed" >&2; return 1; }
		submitted_at="$(python3 -c 'import time; print(time.time())')"
		lifecycle_run_deploy_operation "$backend-replacement-retry" "$WRT_CONTRACT_CLI" node deploy --bundle upgrade-two.tar.gz --request-token replacement-token
		completed_at="$(python3 -c 'import time; print(time.time())')"
		lifecycle_capture_operation_detail node-a replacement-token "${WRT_CONTRACT_ARTIFACT:-/tmp/wruntime-operation-detail.json}" "$WRT_CONTRACT_CLI"
		if [ -n "$probe_log" ]; then
			invoke_probe_over_tunnel probe "$WRT_CONTRACT_TRAFFIC_DIR/$backend-post.json"
			stop_probe
			python3 "$ROOT/dev/deployment-e2e/traffic_probe.py" evaluate --log "$probe_log" --submitted-at "$submitted_at" --completed-at "$completed_at" >"$WRT_CONTRACT_TRAFFIC_DIR/$backend-summary.json"
			stop_probe
			stop_tunnel
			stop_tunnel
		fi
		lifecycle_run_deploy_operation "$backend-removal" "$WRT_CONTRACT_CLI" node deploy --bundle upgrade-one.tar.gz
		lifecycle_run_deploy_operation "$backend-empty-inventory" "$WRT_CONTRACT_CLI" node deploy --bundle empty-inventory.tar.gz --allow-downtime
		lifecycle_run_deploy_operation "$backend-rollback" "$WRT_CONTRACT_CLI" node rollback --to revision_one_a --allow-downtime
		if [ "$backend" = systemd ]; then
			local manager_trace_root="${WRT_CONTRACT_TRAFFIC_DIR:-/tmp}"
			lifecycle_run_manager_rollout a-to-b "$manager_trace_root/manager-a-to-b.jsonl" "$WRT_CONTRACT_CLI" managers deploy-set --manifest manager-a-to-b-systemd.toml
			lifecycle_run_manager_rollout b-to-a "$manager_trace_root/manager-b-to-a.jsonl" "$WRT_CONTRACT_CLI" managers deploy-set --manifest manager-b-to-a-systemd.toml
			lifecycle_expect_manager_failure failed-closed "$manager_trace_root/manager-failed-closed.jsonl" "$WRT_CONTRACT_CLI" managers deploy-set --manifest manager-failed-closed-systemd.toml
			lifecycle_expect_manager_failure reset-active "$manager_trace_root/manager-reset-active.jsonl" "$WRT_CONTRACT_CLI" managers reset-failed-rollout --manifest manager-failed-closed-systemd.toml --rollout-id failed-rollout
			lifecycle_expect_manager_failure reset-mixed "$manager_trace_root/manager-reset-mixed.jsonl" "$WRT_CONTRACT_CLI" managers reset-failed-rollout --manifest manager-failed-closed-systemd.toml --rollout-id failed-rollout
			lifecycle_run_manager_rollout reset-complete "$manager_trace_root/manager-reset-complete.jsonl" "$WRT_CONTRACT_CLI" managers reset-failed-rollout --manifest manager-failed-closed-systemd.toml --rollout-id failed-rollout
			lifecycle_run_manager_rollout closed-after-reset "$manager_trace_root/manager-closed-after-reset.jsonl" "$WRT_CONTRACT_CLI" lifecycle status
			lifecycle_run_manager_rollout fresh "$manager_trace_root/manager-fresh.jsonl" "$WRT_CONTRACT_CLI" managers deploy-set --manifest manager-fresh-systemd.toml
		fi
	done
	lifecycle_final_cleanup "$WRT_CONTRACT_PROVIDER" stop-reset
}
