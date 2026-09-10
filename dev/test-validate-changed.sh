#!/usr/bin/env bash
# Hermetic regression tests for dev/validate-changed.sh.

set -u -o pipefail

SOURCE_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
SELECTOR="$SOURCE_ROOT/dev/validate-changed.sh"
TEST_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/wr-test-validate-changed.XXXXXX") || exit 1
trap 'rm -rf "$TEST_ROOT"' EXIT
REAL_GIT=$(command -v git)
TESTS=0
CURRENT_REPO=
CURRENT_STATE=
CURRENT_BIN=
OUTPUT=
STATUS=0

fail() {
	echo "not ok: $*" >&2
	if [[ -n "$OUTPUT" ]]; then printf '%s\n' "$OUTPUT" >&2; fi
	exit 1
}

pass() {
	TESTS=$((TESTS + 1))
}

new_fixture() {
	local name=$1
	CURRENT_REPO="$TEST_ROOT/repo-$name-$TESTS"
	CURRENT_STATE="$TEST_ROOT/state-$name-$TESTS"
	CURRENT_BIN="$TEST_ROOT/bin-$name-$TESTS"
	mkdir -p "$CURRENT_REPO" "$CURRENT_STATE/calls" "$CURRENT_BIN"
	(
		cd "$CURRENT_REPO" || exit
		"$REAL_GIT" init -q
		"$REAL_GIT" config user.email fixture@example.invalid
		"$REAL_GIT" config user.name Fixture
		"$REAL_GIT" config commit.gpgsign false
		mkdir -p seed
		printf 'baseline\n' >seed/file.txt
		"$REAL_GIT" add .
		"$REAL_GIT" commit -qm baseline
	)
	cat >"$CURRENT_BIN/just" <<'FAKE'
#!/usr/bin/env bash
set -u
state=${FAKE_JUST_STATE:?}
count=0
[[ ! -f "$state/count" ]] || read -r count <"$state/count"
count=$((count + 1))
printf '%s\n' "$count" >"$state/count"
printf '%s\0' "$@" >"$state/calls/$count"
if [[ -n "${FAKE_JUST_MUTATE_TRACKED:-}" && $count -eq ${FAKE_JUST_MUTATE_ON_CALL:-1} ]]; then
	printf 'mutation\n' >>"$FAKE_JUST_MUTATE_TRACKED"
fi
if [[ -n "${FAKE_JUST_MUTATE_UNTRACKED:-}" && $count -eq ${FAKE_JUST_MUTATE_ON_CALL:-1} ]]; then
	printf 'mutation\n' >>"$FAKE_JUST_MUTATE_UNTRACKED"
fi
if [[ -n "${FAKE_JUST_REMOVE_UNTRACKED:-}" && $count -eq ${FAKE_JUST_MUTATE_ON_CALL:-1} ]]; then
	rm -f "$FAKE_JUST_REMOVE_UNTRACKED"
fi
if [[ -n "${FAKE_JUST_FAIL_COMMAND:-}" ]]; then
	if [[ "${1:-}" == "$FAKE_JUST_FAIL_COMMAND" ]]; then
		exit "${FAKE_JUST_STATUS:-1}"
	fi
	exit 0
fi
exit "${FAKE_JUST_STATUS:-0}"
FAKE
	chmod +x "$CURRENT_BIN/just"
}

run_selector() {
	local old_errexit=false
	[[ $- == *e* ]] && old_errexit=true && set +e
	OUTPUT=$(cd "$CURRENT_REPO" && PATH="$CURRENT_BIN:$PATH" FAKE_JUST_STATE="$CURRENT_STATE" bash "$SELECTOR" "$@" 2>&1)
	STATUS=$?
	[[ "$old_errexit" == true ]] && set -e
}

add_path() {
	local path=$1
	if [[ "$path" == */* ]]; then
		mkdir -p "$CURRENT_REPO/${path%/*}"
	fi
	printf 'changed\n' >"$CURRENT_REPO/$path"
}

assert_status() {
	[[ "$STATUS" -eq "$1" ]] || fail "expected status $1, got $STATUS"
}

assert_contains() {
	[[ "$OUTPUT" == *"$1"* ]] || fail "missing output: $1"
}

assert_not_contains() {
	[[ "$OUTPUT" != *"$1"* ]] || fail "unexpected output: $1"
}

assert_profile() {
	assert_contains "selected profile: $1"
}

assert_call_count() {
	local actual=0
	[[ ! -f "$CURRENT_STATE/count" ]] || read -r actual <"$CURRENT_STATE/count"
	[[ "$actual" -eq "$1" ]] || fail "expected $1 calls, got $actual"
}

assert_call() {
	local number=$1
	shift
	local -a actual=()
	[[ -f "$CURRENT_STATE/calls/$number" ]] || fail "missing call $number"
	mapfile -d '' -t actual <"$CURRENT_STATE/calls/$number"
	[[ "${actual[*]}" == "$*" ]] || fail "call $number: expected [$*], got [${actual[*]}]"
}

profile_case() {
	local expected=$1 path=$2
	new_fixture "profile"
	add_path "$path"
	(
		cd "$CURRENT_REPO" || exit
		"$REAL_GIT" add -f -- "$path"
		"$REAL_GIT" commit -qm owned
		printf 'modified\n' >>"$path"
	)
	run_selector --explain
	assert_status 0
	assert_profile "$expected"
	assert_call_count 0
	pass
}

# Clean trees accept valid passthrough flags but dispatch nothing.
new_fixture clean
run_selector --no-deployment-e2e
assert_status 0
assert_contains "no changes relative to"
assert_call_count 0
pass

# Discovery includes committed-since-base, index/worktree/deletion, both rename paths,
# and shell-unambiguous non-ignored untracked names, while excluding ignored files.
new_fixture discovery
(
	cd "$CURRENT_REPO" || exit
	mkdir -p docs
	printf 'rename\n' >docs/rename-old.md
	printf 'delete\n' >docs/delete.md
	printf '*.ignored\n' >.gitignore
	"$REAL_GIT" add . && "$REAL_GIT" commit -qm setup
	base=$("$REAL_GIT" rev-parse HEAD)
	printf 'old\n' >docs/committed.md
	"$REAL_GIT" add docs/committed.md && "$REAL_GIT" commit -qm second
	printf 'new\n' >>docs/committed.md
	"$REAL_GIT" add docs/committed.md
	printf 'worktree\n' >>docs/committed.md
	mv docs/rename-old.md docs/rename-new.md
	rm docs/delete.md
	printf 'ignored\n' >hidden.ignored
	printf 'space\n' >'docs/space name.md'
	printf 'newline\n' >$'docs/line\nbreak.md'
	printf '%s\n' "$base" >"$CURRENT_STATE/base"
)
read -r discovery_base <"$CURRENT_STATE/base"
run_selector --base "$discovery_base" --explain
assert_status 0
assert_profile docs
for path in docs/committed.md docs/rename-old.md docs/rename-new.md docs/delete.md 'docs/space name.md' $'docs/line\nbreak.md'; do
	printf -v quoted '%q' "$path"
	assert_contains "  $quoted"
done
assert_not_contains hidden.ignored
# The lexical order is deterministic, including quoted unusual names.
first_output=$OUTPUT
run_selector --base "$discovery_base" --explain
[[ "$OUTPUT" == "$first_output" ]] || fail "evidence changed between identical snapshots"
pass

# Exact normal focused command sequences.
new_fixture docs-sequence
add_path docs/guide.md
run_selector
assert_status 0
assert_call_count 1
assert_call 1 fmt-check
assert_contains "manual: review documentation links and navigation"
pass

new_fixture workspace-sequence
add_path wr-manager/src/lib.rs
run_selector
assert_status 0
assert_call_count 4
for spec in '1 fmt-check' '2 check' '3 lint' '4 test'; do set -- $spec; assert_call "$1" "$2"; done
pass

new_fixture wasm-sequence
add_path wit/world.wit
run_selector
assert_status 0
assert_call_count 7
for spec in '1 fmt-check' '2 fmt-examples-check' '3 check' '4 lint' '5 lint-examples' '6 build-wasm-guests' '7 test'; do set -- $spec; assert_call "$1" "$2"; done
assert_not_contains 'just test-wasm'
pass

# Owned profile boundaries, including the wr-tests WASM exceptions.
for path in README.md NOTES.md docs/guide.md docs/deployment.md docs/agents/wruntime-maintainer/invariants.md; do profile_case docs "$path"; done
for path in wr-manager/src/lib.rs wr-proxy/README.md wr-tests/tests/manager_test.rs; do profile_case workspace "$path"; done
for path in wit/world.wit wr-sdk/src/lib.rs wr-sdk-macros/src/lib.rs wr-build/src/lib.rs wr-engine/src/lib.rs wr-tests/guests/demo/src/lib.rs wr-tests/tests/wasm_db_host_test.rs; do profile_case wasm "$path"; done

# Every literal/root/cross-cutting/unknown class conservatively selects full.
for path in Justfile AGENTS.md CLAUDE.md dev/validate-all.sh dev/validate-changed.sh dev/test-validate-changed.sh docs/testing.md docs/agents/wruntime-maintainer/validation.md Cargo.toml Cargo.lock rust-toolchain rust-toolchain.toml taplo.toml bacon.toml .cargo/config.toml proto/control.proto wr-common/src/lib.rs wr-cli/src/lib.rs examples/demo/file.txt unknown/file.txt newdir/spec.md; do profile_case full "$path"; done

# Mixed optimized profiles are broad.
new_fixture mixed
add_path docs/guide.md
add_path wr-manager/src/lib.rs
run_selector --explain --no-deployment-e2e
assert_status 0
assert_profile full
pass

# Every protected root/literal has absolute precedence, including broad/unknown mixtures.
protected_paths=(
	dev/deployment-e2e/case.py dev/deployment-e2e.toml dev/validate-deployment-lifecycle.sh wr-tests/deployment/case.toml
	wr-cli/src/cmd/bundle.rs wr-cli/src/cmd/bundle_integrity.rs wr-cli/src/cmd/deploy_config.rs wr-cli/src/cmd/managers.rs
	wr-cli/src/cmd/node.rs wr-cli/src/cmd/node_agent.rs wr-cli/src/cmd/node_backend.rs wr-cli/src/cmd/service_gen.rs
)
for path in "${protected_paths[@]}"; do
	new_fixture protected
	add_path "$path"
	add_path Cargo.toml
	add_path wr-manager/src/lib.rs
	add_path unknown/spec.txt
	run_selector --explain --deployment-e2e
	assert_status 0
	assert_profile protected-full
	pass
done

# All allowed broad flags are selector errors on every focused profile.
allowed_flags=(--no-e2e --e2e-only --deployment-e2e --no-deployment-e2e --codegen-e2e --no-codegen-e2e --skip-dev-up)
for flag in "${allowed_flags[@]}"; do
	new_fixture focused-option
	add_path docs/guide.md
	run_selector --explain "$flag"
	assert_status 2
	assert_call_count 0
	pass
done

# Full delegates once and preserves argv boundaries/order without second-guessing contradictions.
new_fixture full-forward
add_path Cargo.toml
run_selector --no-codegen-e2e --no-e2e --deployment-e2e --skip-dev-up
assert_status 0
assert_call_count 1
assert_call 1 validate-all --no-codegen-e2e --no-e2e --deployment-e2e --skip-dev-up
pass

# Protected selection owns its deployment requirement and forwards compatible options once.
new_fixture protected-require
add_path dev/deployment-e2e.toml
run_selector --explain
assert_status 2
assert_call_count 0
run_selector --explain --no-deployment-e2e
assert_status 2
assert_call_count 0
rm -f "$CURRENT_STATE/count" "$CURRENT_STATE/calls"/*
run_selector --codegen-e2e --deployment-e2e --skip-dev-up
assert_status 0
assert_call_count 1
assert_call 1 validate-all --codegen-e2e --deployment-e2e --skip-dev-up
pass

# Explain never executes, but prints paths, profile, reason, and complete commands.
new_fixture explain
add_path wr-manager/src/lib.rs
run_selector --explain
assert_status 0
assert_profile workspace
assert_contains 'reason:'
assert_contains 'just fmt-check'
assert_contains 'just test'
assert_call_count 0
pass

# Invalid base has locked normal/explain behavior.
new_fixture invalid-base
run_selector --base does-not-exist --explain --no-deployment-e2e
assert_status 2
assert_profile full
assert_contains 'resolved base: unresolvable'
assert_contains 'just validate-all --no-deployment-e2e'
assert_call_count 0
run_selector --base does-not-exist --no-deployment-e2e
assert_status 0
assert_call_count 1
assert_call 1 validate-all --no-deployment-e2e
pass

# Malformed input is status 2 with usage and cannot dispatch.
for args in '--unknown' '--base'; do
	new_fixture malformed
	# Intentional word splitting gives the zero/one argument forms above.
	run_selector $args
	assert_status 2
	assert_contains 'Usage:'
	assert_call_count 0
	pass
done

# Focused commands stop immediately and preserve the exact child status.
new_fixture child-status
add_path wr-manager/src/lib.rs
FAKE_JUST_FAIL_COMMAND=check FAKE_JUST_STATUS=37 run_selector
assert_status 37
assert_call_count 2
assert_call 2 check
pass

# A child mutation of tracked or untracked input invalidates otherwise successful work.
new_fixture mutate-tracked
add_path wr-manager/src/lib.rs
(
	cd "$CURRENT_REPO" || exit
	"$REAL_GIT" add . && "$REAL_GIT" commit -qm owned
)
printf 'dirty\n' >>"$CURRENT_REPO/wr-manager/src/lib.rs"
FAKE_JUST_MUTATE_TRACKED="$CURRENT_REPO/wr-manager/src/lib.rs" run_selector
assert_status 1
assert_contains 'older snapshot'
pass

new_fixture mutate-untracked
add_path docs/guide.md
FAKE_JUST_MUTATE_UNTRACKED="$CURRENT_REPO/docs/guide.md" run_selector
assert_status 1
assert_contains 'older snapshot'
pass

new_fixture disappear-untracked
add_path docs/guide.md
FAKE_JUST_REMOVE_UNTRACKED="$CURRENT_REPO/docs/guide.md" run_selector
assert_status 1
assert_contains 'older snapshot'
pass

# A Git wrapper mutating after fingerprint one proves pre-dispatch instability runs nothing.
new_fixture pre-instability
mkdir -p "$CURRENT_REPO/wr-manager/src"
printf 'base\n' >"$CURRENT_REPO/wr-manager/src/lib.rs"
(
	cd "$CURRENT_REPO" || exit
	"$REAL_GIT" add . && "$REAL_GIT" commit -qm tracked
)
printf 'dirty\n' >>"$CURRENT_REPO/wr-manager/src/lib.rs"
mv "$CURRENT_BIN/just" "$CURRENT_BIN/just.real"
cat >"$CURRENT_BIN/git" <<FAKEGIT
#!/usr/bin/env bash
"$REAL_GIT" "\$@"
status=\$?
if [[ \$status -eq 0 && " \$* " == *' diff --binary --no-renames '* ]]; then
	count=0
	[[ ! -f '$CURRENT_STATE/git-diff-count' ]] || read -r count <'$CURRENT_STATE/git-diff-count'
	count=\$((count + 1))
	printf '%s\n' "\$count" >'$CURRENT_STATE/git-diff-count'
	if [[ \$count -eq 1 ]]; then printf 'raced\n' >>'$CURRENT_REPO/wr-manager/src/lib.rs'; fi
fi
exit \$status
FAKEGIT
chmod +x "$CURRENT_BIN/git"
mv "$CURRENT_BIN/just.real" "$CURRENT_BIN/just"
run_selector
assert_status 1
assert_contains 'sources are changing; rerun'
assert_call_count 0
pass

# Git inspection errors can only produce a broad fallback (or a non-running explain failure).
new_fixture inspect-failure
mv "$CURRENT_BIN/just" "$CURRENT_BIN/just.real"
cat >"$CURRENT_BIN/git" <<FAKEGIT
#!/usr/bin/env bash
if [[ " \$* " == *' ls-files --others '* ]]; then exit 71; fi
exec "$REAL_GIT" "\$@"
FAKEGIT
chmod +x "$CURRENT_BIN/git"
mv "$CURRENT_BIN/just.real" "$CURRENT_BIN/just"
run_selector --explain --no-deployment-e2e
assert_status 1
assert_profile full
assert_call_count 0
run_selector --no-deployment-e2e
assert_status 0
assert_call_count 1
assert_call 1 validate-all --no-deployment-e2e
pass

printf 'ok: %d validate-changed cases passed\n' "$TESTS"
