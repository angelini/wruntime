#!/usr/bin/env bash
# Comprehensive validation runner for wruntime.
#
# Orchestrates existing Just recipes for formatting, compile checks, lints,
# WASM guest builds, the Rust test suite, and fixed-port E2E examples. Stages
# fail fast, while independent tasks inside a stage run in parallel.

set -u -o pipefail

ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$ROOT" || exit 1

RUN_E2E=true
RUN_CODEGEN_E2E=auto
RUN_DEPLOYMENT_E2E="unset"
E2E_ONLY=false
START_DEV=true
SKIPPED_CODEGEN_E2E=false
LOG_ROOT="${WR_VALIDATE_LOG_DIR:-target/validate-all/$(date +%Y%m%d-%H%M%S)}"
TAIL_LINES=40
WARN_MATCH_LIMIT=40
WARN_PATTERN='(^|[^[:alnum:]_])(WARN|WARNING)([^[:alnum:]_]|$)|level="?warn(ing)?"?|"level":"warn(ing)?"'

usage() {
	cat <<'USAGE'
Usage: dev/validate-all.sh [OPTIONS]

Options:
  --no-e2e             Skip fixed-port local E2E examples (not deployment E2E).
  --e2e-only           Run only setup and enabled E2E stages.
  --deployment-e2e    Require serial systemd and Docker deployment lifecycle E2E.
  --no-deployment-e2e Deliberately skip deployment lifecycle E2E.
  --codegen-e2e        Require codegen E2E; fails if ANTHROPIC_API_KEY is unset.
  --no-codegen-e2e     Skip codegen E2E even if ANTHROPIC_API_KEY is set.
  --skip-dev-up     Do not run `just dev-up` before DB/S3-backed tests.
  -h, --help        Show this help.

Environment:
  ANTHROPIC_API_KEY   Enables codegen E2E in default auto mode.
  WR_VALIDATE_LOG_DIR Override the log directory (default: target/validate-all/<timestamp>).

Exactly one deployment E2E flag is required. --no-e2e may be combined with
--deployment-e2e; --e2e-only runs whichever E2E stages were explicitly enabled.
USAGE
}

while [ $# -gt 0 ]; do
	case "$1" in
	--no-e2e)
		RUN_E2E=false
		;;
	--e2e-only)
		E2E_ONLY=true
		;;
	--deployment-e2e)
		if [ "$RUN_DEPLOYMENT_E2E" = false ]; then
			echo "--deployment-e2e contradicts --no-deployment-e2e" >&2
			exit 2
		fi
		RUN_DEPLOYMENT_E2E=true
		;;
	--no-deployment-e2e)
		if [ "$RUN_DEPLOYMENT_E2E" = true ]; then
			echo "--no-deployment-e2e contradicts --deployment-e2e" >&2
			exit 2
		fi
		RUN_DEPLOYMENT_E2E=false
		;;
	--codegen-e2e)
		RUN_CODEGEN_E2E=true
		;;
	--no-codegen-e2e)
		RUN_CODEGEN_E2E=false
		;;
	--skip-dev-up)
		START_DEV=false
		;;
	-h | --help)
		usage
		exit 0
		;;
	*)
		echo "unknown option: $1" >&2
		usage >&2
		exit 2
		;;
	esac
	shift
done

if [ "$RUN_DEPLOYMENT_E2E" = unset ]; then
	echo "choose --deployment-e2e or --no-deployment-e2e explicitly" >&2
	usage >&2
	exit 2
fi

if [ "$E2E_ONLY" = true ]; then
	RUN_E2E=true
fi

mkdir -p "$LOG_ROOT"
SUMMARY_FILE="$LOG_ROOT/summary.txt"
TASK_NAMES=()
TASK_STATUSES=()
TASK_LOGS=()
TASK_NOTES=()

section() {
	printf '\n==> %s\n' "$1"
}

append_result() {
	TASK_NAMES+=("$1")
	TASK_STATUSES+=("$2")
	TASK_LOGS+=("${3:-}")
	TASK_NOTES+=("${4:-}")
}

write_summary() {
	final_status="$1"
	{
		printf 'validate-all summary\n'
		printf 'status: %s\n' "$final_status"
		printf 'logs: %s\n' "$LOG_ROOT"
		printf 'generated_at: %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
		printf '\nTasks:\n'
		for i in "${!TASK_NAMES[@]}"; do
			printf -- '- [%s] %s' "${TASK_STATUSES[$i]}" "${TASK_NAMES[$i]}"
			if [ -n "${TASK_LOGS[$i]}" ]; then
				printf ' — %s' "${TASK_LOGS[$i]}"
			fi
			if [ -n "${TASK_NOTES[$i]}" ]; then
				printf ' — %s' "${TASK_NOTES[$i]}"
			fi
			printf '\n'
		done
	} >"$SUMMARY_FILE"
}

print_failed_tasks() {
	printed=false
	for i in "${!TASK_NAMES[@]}"; do
		if [ "${TASK_STATUSES[$i]}" = FAILED ]; then
			if [ "$printed" = false ]; then
				printf '\nFailed tasks:\n' >&2
				printed=true
			fi
			printf '  - %s' "${TASK_NAMES[$i]}" >&2
			if [ -n "${TASK_LOGS[$i]}" ]; then
				printf ' (%s)' "${TASK_LOGS[$i]}" >&2
			fi
			if [ -n "${TASK_NOTES[$i]}" ]; then
				printf ': %s' "${TASK_NOTES[$i]}" >&2
			fi
			printf '\n' >&2
		fi
	done
}

finish_failure() {
	status="${1:-1}"
	write_summary failed
	print_failed_tasks
	printf '\nsummary: %s\n' "$SUMMARY_FILE" >&2
	exit "$status"
}

finish_success() {
	write_summary passed
	section "complete"
	if [ "$SKIPPED_CODEGEN_E2E" = true ]; then
		printf 'all required validations passed; codegen E2E skipped (ANTHROPIC_API_KEY unset); logs: %s\n' "$LOG_ROOT"
	else
		printf 'all validations passed; logs: %s\n' "$LOG_ROOT"
	fi
	printf 'summary: %s\n' "$SUMMARY_FILE"
}

require_cmd() {
	if ! command -v "$1" >/dev/null 2>&1; then
		echo "missing required command: $1" >&2
		append_result "required command: $1" FAILED "" "command not found"
		finish_failure 127
	fi
}

run_cmd() {
	name="$1"
	cmd="$2"
	log="$LOG_ROOT/${name//[^A-Za-z0-9_.-]/_}.log"
	reported_log="${3:-$log}"

	printf '  • %s\n' "$name"
	printf '$ %s\n' "$cmd" >"$log"
	bash -c "$cmd" >>"$log" 2>&1
	status=$?
	if [ "$status" -eq 0 ]; then
		printf '    ok (%s)\n' "$log"
		append_result "$name" OK "$reported_log" ""
		return 0
	fi

	printf '    FAILED (%s, exit %s)\n' "$log" "$status" >&2
	tail -n "$TAIL_LINES" "$log" >&2
	append_result "$name" FAILED "$reported_log" "exit $status (runner log: $log)"
	finish_failure "$status"
}

run_cmd_no_warn() {
	name="$1"
	cmd="$2"
	log="$LOG_ROOT/${name//[^A-Za-z0-9_.-]/_}.log"

	printf '  • %s\n' "$name"
	printf '$ %s\n' "$cmd" >"$log"
	bash -c "$cmd" >>"$log" 2>&1
	status=$?
	if [ "$status" -ne 0 ]; then
		printf '    FAILED (%s, exit %s)\n' "$log" "$status" >&2
		tail -n "$TAIL_LINES" "$log" >&2
		append_result "$name" FAILED "$log" "exit $status"
		finish_failure "$status"
	fi

	warning_count=$(grep -Ec "$WARN_PATTERN" "$log")
	grep_status=$?
	if [ "$grep_status" -eq 1 ]; then
		warning_count=0
	elif [ "$grep_status" -ne 0 ]; then
		printf '    FAILED (warning scan failed for %s, exit %s)\n' "$log" "$grep_status" >&2
		append_result "$name" FAILED "$log" "warning scan exit $grep_status"
		finish_failure "$grep_status"
	fi
	if [ "$warning_count" -gt 0 ]; then
		printf '    FAILED (%s contains %s warning match(es))\n' "$log" "$warning_count" >&2
		grep -En "$WARN_PATTERN" "$log" | head -n "$WARN_MATCH_LIMIT" >&2
		if [ "$warning_count" -gt "$WARN_MATCH_LIMIT" ]; then
			printf '    ... showing first %s warning matches; see %s for full output\n' "$WARN_MATCH_LIMIT" "$log" >&2
		fi
		append_result "$name" FAILED "$log" "$warning_count warning match(es)"
		finish_failure 1
	fi

	printf '    ok (%s)\n' "$log"
	append_result "$name" OK "$log" ""
}

run_parallel() {
	stage="$1"
	shift
	section "$stage"

	pids=()
	names=()
	logs=()
	while [ $# -gt 0 ]; do
		name="$1"
		cmd="$2"
		shift 2
		log="$LOG_ROOT/${name//[^A-Za-z0-9_.-]/_}.log"
		names+=("$name")
		logs+=("$log")
		printf '  • %s\n' "$name"
		(
			printf '$ %s\n' "$cmd" >"$log"
			bash -c "$cmd" >>"$log" 2>&1
		) &
		pids+=("$!")
	done

	failed=0
	for i in "${!pids[@]}"; do
		if wait "${pids[$i]}"; then
			printf '    ok: %s (%s)\n' "${names[$i]}" "${logs[$i]}"
			append_result "${names[$i]}" OK "${logs[$i]}" ""
		else
			status=$?
			printf '    FAILED: %s (%s, exit %s)\n' "${names[$i]}" "${logs[$i]}" "$status" >&2
			tail -n "$TAIL_LINES" "${logs[$i]}" >&2
			append_result "${names[$i]}" FAILED "${logs[$i]}" "exit $status"
			failed=1
		fi
	done

	if [ "$failed" -ne 0 ]; then
		echo "stage failed: $stage" >&2
		finish_failure 1
	fi
}

require_cmd just
if [ "$START_DEV" = true ]; then
	require_cmd docker
fi
if [ "$RUN_E2E" = true ] || [ "$RUN_DEPLOYMENT_E2E" = true ]; then
	require_cmd psql
fi
if [ "$RUN_E2E" = true ]; then
	require_cmd aws
fi
if [ "$RUN_DEPLOYMENT_E2E" = true ]; then
	for command in uv flock ssh scp timeout cargo-zigbuild; do
		require_cmd "$command"
	done
fi

section "setup"
printf 'logs: %s\n' "$LOG_ROOT"
if [ ! -f certs/ca.crt ] || [ ! -f certs/127.0.0.1.crt ] || [ ! -f certs/manager.crt ]; then
	run_cmd "generate certs" "just certs"
fi
if [ "$START_DEV" = true ]; then
	run_cmd "start dev infrastructure" "just dev-up"
fi

if [ "$E2E_ONLY" != true ]; then
	run_parallel "format checks" \
		"workspace format" "just fmt-check" \
		"guest format" "just fmt-examples-check"

	run_parallel "early compile checks" \
		"workspace check" "just check"

	run_parallel "lints" \
		"workspace lint" "just lint" \
		"guest lint" "just lint-examples"

	section "wasm and example builds"
	run_cmd "all wasm guests" "just build-wasm-guests"

	section "rust tests"
	run_cmd "workspace tests including wasm host tests" "just test"
fi

section "deployment lifecycle E2E"
if [ "$RUN_DEPLOYMENT_E2E" = true ]; then
	run_cmd "deployment E2E systemd" "WR_VALIDATE_LOG_DIR='$LOG_ROOT/deployment-e2e-systemd' bash dev/validate-deployment-lifecycle.sh --backend systemd" "$LOG_ROOT/deployment-e2e-systemd"
	run_cmd "deployment E2E docker" "WR_VALIDATE_LOG_DIR='$LOG_ROOT/deployment-e2e-docker' bash dev/validate-deployment-lifecycle.sh --backend docker" "$LOG_ROOT/deployment-e2e-docker"
else
	printf '  • deployment E2E systemd skipped: --no-deployment-e2e\n'
	append_result "deployment E2E systemd" SKIPPED "" "--no-deployment-e2e"
	printf '  • deployment E2E docker skipped: --no-deployment-e2e\n'
	append_result "deployment E2E docker" SKIPPED "" "--no-deployment-e2e"
fi

if [ "$RUN_E2E" = true ]; then
	section "semantic lifecycle E2E examples"
	run_cmd "reset example db for multi-node" "just dev-reset-db"
	run_cmd "multi-node cross-node echo" "just multi-node-inline"

	run_cmd "reset example db for ecommerce" "just dev-reset-db"
	run_cmd "ecommerce validation" "just validate-ecommerce"

	run_cmd "reset example db for stockmarket" "just dev-reset-db"
	run_cmd "reset stockmarket blobstore" "just dev-reset-blobstore stockmarket"
	run_cmd_no_warn "stockmarket inline" "just stockmarket-inline"

	should_run_codegen=false
	if [ "$RUN_CODEGEN_E2E" = true ]; then
		if [ -z "${ANTHROPIC_API_KEY:-}" ]; then
			echo "ANTHROPIC_API_KEY is required for --codegen-e2e" >&2
			append_result "codegen inline" FAILED "" "ANTHROPIC_API_KEY is required for --codegen-e2e"
			finish_failure 1
		fi
		should_run_codegen=true
	elif [ "$RUN_CODEGEN_E2E" = auto ] && [ -n "${ANTHROPIC_API_KEY:-}" ]; then
		should_run_codegen=true
	elif [ "$RUN_CODEGEN_E2E" = auto ]; then
		SKIPPED_CODEGEN_E2E=true
		printf '  • codegen inline skipped: ANTHROPIC_API_KEY is unset\n'
		append_result "codegen inline" SKIPPED "" "ANTHROPIC_API_KEY is unset"
	fi

	if [ "$should_run_codegen" = true ]; then
		run_cmd "reset example db for codegen" "just dev-reset-db"
		run_cmd "reset codegen blobstore" "just dev-reset-blobstore codegen"
		run_cmd_no_warn "codegen inline" "just codegen-inline"
	fi
fi

finish_success
