#!/usr/bin/env bash
# Shared same-host exclusion for wruntime's fixed-port local E2E examples.
# Source this file, then call wrt_acquire_local_e2e_lock with a short context.

wrt_acquire_local_e2e_lock() {
	local context="${1:-local E2E}"
	local runtime_dir holder

	if ! command -v flock >/dev/null 2>&1; then
		echo "missing required command for local E2E exclusion: flock" >&2
		return 127
	fi

	# A parent runner can hold the lock across several child E2E scripts. The
	# exported descriptor refers to the same open file description in children,
	# so re-locking it verifies that the descriptor survived exec.
	if [ -n "${WRT_LOCAL_E2E_LOCK_FD:-}" ]; then
		if flock -n "$WRT_LOCAL_E2E_LOCK_FD" 2>/dev/null; then
			return 0
		fi
		echo "inherited local E2E lock descriptor is unavailable: $WRT_LOCAL_E2E_LOCK_FD" >&2
		return 1
	fi

	runtime_dir="${XDG_RUNTIME_DIR:-/tmp}"
	WRT_LOCAL_E2E_LOCK_FILE="${WRT_LOCAL_E2E_LOCK_FILE:-${runtime_dir}/wruntime-local-e2e-${UID}.lock}"
	if ! mkdir -p "$(dirname "$WRT_LOCAL_E2E_LOCK_FILE")"; then
		echo "cannot create local E2E lock directory for $WRT_LOCAL_E2E_LOCK_FILE" >&2
		return 1
	fi
	if ! exec {WRT_LOCAL_E2E_LOCK_FD}>>"$WRT_LOCAL_E2E_LOCK_FILE"; then
		echo "cannot open local E2E lock: $WRT_LOCAL_E2E_LOCK_FILE" >&2
		return 1
	fi
	if ! flock -n "$WRT_LOCAL_E2E_LOCK_FD"; then
		holder="$(<"$WRT_LOCAL_E2E_LOCK_FILE")"
		printf 'another wruntime local E2E run owns %s\n' "$WRT_LOCAL_E2E_LOCK_FILE" >&2
		if [ -n "$holder" ]; then
			printf 'lock owner:\n%s\n' "$holder" >&2
		fi
		exec {WRT_LOCAL_E2E_LOCK_FD}>&-
		unset WRT_LOCAL_E2E_LOCK_FD
		return 1
	fi

	if ! printf 'pid=%s\nworktree=%s\ncontext=%s\nstarted_at=%s\n' \
		"$$" "$(pwd -P)" "$context" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
		>"$WRT_LOCAL_E2E_LOCK_FILE"; then
		exec {WRT_LOCAL_E2E_LOCK_FD}>&-
		unset WRT_LOCAL_E2E_LOCK_FD
		echo "cannot write local E2E lock metadata: $WRT_LOCAL_E2E_LOCK_FILE" >&2
		return 1
	fi

	export WRT_LOCAL_E2E_LOCK_FILE WRT_LOCAL_E2E_LOCK_FD
	printf 'local E2E lock: %s\n' "$WRT_LOCAL_E2E_LOCK_FILE"
}
