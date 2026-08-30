#!/usr/bin/env bash
# Shared failure-reporting helpers for the deployment lifecycle harness.

ACTIVE_STEP=""
ACTIVE_LOG=""

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

run_with_retry() {
	local output="$1" error_log="$2" attempts="$3" delay="$4" retry_pattern="$5"
	local status=1 attempt attempt_error="${error_log}.attempt"
	shift 5
	: >"$error_log"
	for ((attempt = 1; attempt <= attempts; attempt++)); do
		if "$@" >"$output" 2>"$attempt_error"; then
			rm -f "$attempt_error"
			return 0
		else
			status=$?
		fi
		{
			printf '%s\n' "--- attempt $attempt failed (exit $status) ---"
			cat "$attempt_error"
		} >>"$error_log"
		if ! grep -Eq -- "$retry_pattern" "$attempt_error"; then
			rm -f "$attempt_error"
			return "$status"
		fi
		[ "$attempt" -eq "$attempts" ] || sleep "$delay"
	done
	rm -f "$attempt_error"
	return "$status"
}

report_active_failure() {
	local status="$1"
	if [ -n "$ACTIVE_STEP" ]; then
		echo "deployment E2E step failed: $ACTIVE_STEP (exit $status)" >&2
		if [ -n "$ACTIVE_LOG" ]; then
			echo "failure log: $ACTIVE_LOG" >&2
			redact_logs
			print_failure_excerpt "$ACTIVE_LOG"
		fi
	else
		echo "deployment E2E command failed (exit $status)" >&2
	fi
}
