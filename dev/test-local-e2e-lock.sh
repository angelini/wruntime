#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOCK_HELPER="$ROOT/dev/local-e2e-lock.sh"
RUN_DIR="$(mktemp -d "${TMPDIR:-/tmp}/wr-local-e2e-lock-test.XXXXXX")"
LOCK_FILE="$RUN_DIR/local-e2e.lock"
trap 'rm -rf "$RUN_DIR"' EXIT

command -v flock >/dev/null 2>&1 || {
	echo "missing required test command: flock" >&2
	exit 127
}

# Simulate an unrelated owner without passing its descriptor to the contender.
exec 9>>"$LOCK_FILE"
flock -n 9
printf 'pid=test-owner\nworktree=/test/worktree\ncontext=test fixture\n' >"$LOCK_FILE"
set +e
contender_output="$({
	exec 9>&-
	WRT_LOCAL_E2E_LOCK_FILE="$LOCK_FILE" bash -c '
		source "$1"
		wrt_acquire_local_e2e_lock "contender"
	' _ "$LOCK_HELPER"
} 2>&1)"
contender_status=$?
set -e
if [ "$contender_status" -eq 0 ]; then
	echo "contender unexpectedly acquired the local E2E lock" >&2
	exit 1
fi
grep -F "another wruntime local E2E run owns $LOCK_FILE" <<<"$contender_output" >/dev/null
grep -F "pid=test-owner" <<<"$contender_output" >/dev/null

flock -u 9
exec 9>&-

# The lock is reusable after its owner exits, and an inherited descriptor lets
# a suite runner invoke individually guarded E2E scripts without deadlocking.
WRT_LOCAL_E2E_LOCK_FILE="$LOCK_FILE" bash -c '
	source "$1"
	wrt_acquire_local_e2e_lock "outer runner"
	bash -c '\''
		source "$1"
		wrt_acquire_local_e2e_lock "inherited child"
	'\'' _ "$1"
' _ "$LOCK_HELPER" >/dev/null

echo "local E2E lock tests passed"
