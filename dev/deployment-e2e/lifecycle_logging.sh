#!/usr/bin/env bash
# Shared failure-reporting helpers for the deployment lifecycle harness.

ACTIVE_STEP=""
ACTIVE_LOG=""
DIAGNOSTIC_FAILURES=()
CLEANUP_FAILURES=()

print_failure_excerpt() {
	local log="$1"
	[ -s "$log" ] || return
	"${PYTHON[@]}" - "$log" <<'PY' >&2
import pathlib, sys
lines = pathlib.Path(sys.argv[1]).read_text(errors="replace").splitlines()
limit = 30
for line in lines[:limit]:
    print(line)
if len(lines) > limit:
    print(f"... {len(lines) - limit} more lines in log")
PY
}

run_to_log() {
	local name="$1" log="$2" status
	shift 2
	ACTIVE_STEP="$name"
	ACTIVE_LOG="$log"
	echo "==> $name"
	if "$@" >"$log" 2>&1; then
		ACTIVE_STEP=""
		ACTIVE_LOG=""
		return 0
	else
		status=$?
		return "$status"
	fi
}

assert_node_stop_record() {
	local record="$1" backend="$2" component="$3"
	"${PYTHON[@]}" - "$record" "$backend" "$component" <<'PY'
import json, pathlib, sys
path, backend, component = pathlib.Path(sys.argv[1]), sys.argv[2], sys.argv[3]
value = json.loads(path.read_text())
if component != value.get("component"):
    raise SystemExit(f"wrong stop component: {value!r}")
if backend != value.get("backend"):
    raise SystemExit(f"wrong stop backend: {value!r}")
kind, separator, slot = component.partition(":")
if kind != "engine" or not separator or not slot:
    raise SystemExit(f"invalid expected component: {component!r}")
expected_target = f"wr-engine-{slot}.service" if backend == "systemd" else f"engine-{slot}"
if value.get("target") != expected_target:
    raise SystemExit(f"wrong stop target: {value!r}")
if value.get("final_exited") is not True:
    raise SystemExit(f"stop did not prove final exit: {value!r}")
elapsed = value.get("elapsed_ms")
if isinstance(elapsed, bool) or not isinstance(elapsed, int) or not 0 <= elapsed <= 45_000:
    raise SystemExit(f"invalid stop elapsed budget: {value!r}")
result = value.get("graceful_result")
if result not in {"already-stopped", "stopped", "escalated"}:
    raise SystemExit(f"invalid graceful disposition: {value!r}")
actions = (value.get("graceful_action"), value.get("force_action"))
for action in actions:
    if not isinstance(action, dict) or action.get("status") not in {"not-attempted", "succeeded", "failed"}:
        raise SystemExit(f"invalid stop action evidence: {value!r}")
force_status = actions[1]["status"]
if value.get("forced") is not (force_status == "succeeded"):
    raise SystemExit(f"forced flag contradicts force action: {value!r}")
if result == "already-stopped" and any(action["status"] != "not-attempted" for action in actions):
    raise SystemExit(f"already-stopped record contains attempted actions: {value!r}")
if result == "stopped" and force_status != "not-attempted":
    raise SystemExit(f"graceful stop record contains force action: {value!r}")
if result == "escalated" and force_status == "not-attempted":
    raise SystemExit(f"escalated record omits force action: {value!r}")
PY
}

collect_diagnostic() {
	local name="$1" log="$2" status
	shift 2
	if "$@" >"$log" 2>&1; then
		return 0
	else
		status=$?
		DIAGNOSTIC_FAILURES+=("${name}=${status}:${log}")
		return 0
	fi
}

report_diagnostic_failures() {
	local failure
	for failure in "${DIAGNOSTIC_FAILURES[@]}"; do
		echo "diagnostic collection unavailable: ${failure}" >&2
	done
}

remaining_deadline_seconds() {
	local deadline="$1" cap="$2" reserve="${3:-0}" remaining
	remaining=$((deadline - SECONDS - reserve))
	[ "$remaining" -gt 0 ] || return 1
	if [ "$remaining" -gt "$cap" ]; then remaining="$cap"; fi
	printf '%s\n' "$remaining"
}

preserve_primary_status() {
	local primary="$1" candidate="$2"
	if [ "$primary" -ne 0 ]; then
		printf '%s\n' "$primary"
	else
		printf '%s\n' "$candidate"
	fi
}

record_cleanup_failure() {
	local name="$1" status="$2" log="${3:-}"
	CLEANUP_FAILURES+=("${name}=${status}${log:+:${log}}")
}

record_promoted_cleanup_failure() {
	local name="$1" status="$2" log="${3:-}"
	record_cleanup_failure "$name" "$status" "$log"
	PRIMARY_STATUS="$(preserve_primary_status "${PRIMARY_STATUS:-0}" "$status")"
}

remove_directory_with_cleanup_accounting() {
	local name="$1" path="$2" status
	if rm -rf "$path"; then
		return 0
	else
		status=$?
	fi
	record_promoted_cleanup_failure "$name" "$status" "$path"
	return "$status"
}

report_cleanup_failures() {
	local failure
	for failure in "${CLEANUP_FAILURES[@]}"; do
		echo "cleanup failure: ${failure}" >&2
	done
}

report_active_failure() {
	local status="$1"
	if [ -n "$ACTIVE_STEP" ]; then
		echo "deployment E2E step failed: $ACTIVE_STEP (exit $status)" >&2
		if [ -n "$ACTIVE_LOG" ]; then
			echo "failure log: $ACTIVE_LOG" >&2
			if redact_logs; then :; else echo "failure-log redaction was unavailable" >&2; fi
			if print_failure_excerpt "$ACTIVE_LOG"; then :; else echo "failure excerpt was unavailable" >&2; fi
		fi
	else
		echo "deployment E2E command failed (exit $status)" >&2
	fi
}
