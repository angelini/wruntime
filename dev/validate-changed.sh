#!/usr/bin/env bash
# Conservatively select focused validation for a stable, locally inspectable change set.

set -u -o pipefail

usage() {
	cat <<'USAGE'
Usage: dev/validate-changed.sh [--base REF] [--explain] [validate-all flags]

Selector options:
  --base REF             Compare with local REF (default: HEAD).
  --explain              Print the selection and commands without running them.
  -h, --help             Show this help.

Allowed validate-all flags:
  --no-e2e
  --e2e-only
  --deployment-e2e
  --no-deployment-e2e
  --codegen-e2e
  --no-codegen-e2e
  --skip-dev-up
USAGE
}

requested_base=HEAD
explain=false
passthrough=()
while (( $# > 0 )); do
	case "$1" in
	--base)
		if (( $# < 2 )) || [[ "$2" == --* ]]; then
			echo "--base requires a value" >&2
			usage >&2
			exit 2
		fi
		requested_base=$2
		shift 2
		;;
	--explain)
		explain=true
		shift
		;;
	-h | --help)
		usage
		exit 0
		;;
	--no-e2e | --e2e-only | --deployment-e2e | --no-deployment-e2e | --codegen-e2e | --no-codegen-e2e | --skip-dev-up)
		passthrough+=("$1")
		shift
		;;
	*)
		echo "unknown option: $1" >&2
		usage >&2
		exit 2
		;;
	esac
done

ROOT=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
cd "$ROOT" || exit 1
TMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/wr-validate-changed.XXXXXX") || exit 1
trap 'rm -rf "$TMP_DIR"' EXIT

print_command() {
	printf '  '
	printf '%q ' "$@"
	printf '\n'
}

print_fallback_evidence() {
	local resolved=$1 reason=$2
	printf 'requested base: %q\n' "$requested_base"
	printf 'resolved base: %s\n' "$resolved"
	printf 'changed paths: unavailable\n'
	printf 'selected profile: full\n'
	printf 'reason: %s\n' "$reason"
	printf 'commands:\n'
	print_command just validate-all "${passthrough[@]}"
}

run_unresolved_fallback() {
	local resolved=$1 reason=$2 explain_status=$3
	print_fallback_evidence "$resolved" "$reason"
	if [[ "$explain" == true ]]; then
		return "$explain_status"
	fi
	just validate-all "${passthrough[@]}"
}

if ! base_oid=$(git rev-parse --verify "${requested_base}^{commit}" 2>/dev/null); then
	run_unresolved_fallback unresolvable "requested base is not a locally resolvable commit; conservatively falling back" 2
	exit $?
fi

# Write a digest of the complete tracked/untracked source snapshot to the named file.
# Binary diffs and NUL-delimited names always stay in files or arrays.
snapshot_fingerprint() {
	local output=$1 tag=$2 head_oid path object_oid
	local dir="$TMP_DIR/fingerprint-$tag"
	mkdir "$dir" || return 1
	git rev-parse --verify 'HEAD^{commit}' >"$dir/head" 2>/dev/null || return 1
	IFS= read -r head_oid <"$dir/head" || return 1
	git diff --binary --no-renames "$base_oid" -- >"$dir/tracked.diff" || return 1
	git ls-files --others --exclude-standard -z >"$dir/untracked" || return 1
	LC_ALL=C sort -z -u "$dir/untracked" >"$dir/untracked.sorted" || return 1
	: >"$dir/untracked.hashes" || return 1
	while IFS= read -r -d '' path; do
		[[ -f "$path" && -r "$path" ]] || return 1
		object_oid=$(git hash-object -- "$path") || return 1
		printf '%s\0%s\0' "$path" "$object_oid" >>"$dir/untracked.hashes" || return 1
	done <"$dir/untracked.sorted"
	{
		printf 'base\0%s\0head\0%s\0tracked\0' "$base_oid" "$head_oid"
		cat "$dir/tracked.diff"
		printf '\0untracked\0'
		cat "$dir/untracked.hashes"
	} >"$dir/input" || return 1
	sha256sum <"$dir/input" >"$output" || return 1
}

collect_paths() {
	local path
	declare -gA changed_set=()
	declare -ga changed_paths=()
	git diff --name-only -z --no-renames "$base_oid" -- >"$TMP_DIR/tracked.paths" || return 1
	git ls-files --others --exclude-standard -z >"$TMP_DIR/untracked.paths" || return 1
	cat "$TMP_DIR/tracked.paths" "$TMP_DIR/untracked.paths" >"$TMP_DIR/all.paths" || return 1
	while IFS= read -r -d '' path; do
		changed_set["$path"]=1
	done <"$TMP_DIR/all.paths"
	: >"$TMP_DIR/unique.paths"
	for path in "${!changed_set[@]}"; do
		printf '%s\0' "$path" >>"$TMP_DIR/unique.paths" || return 1
	done
	LC_ALL=C sort -z "$TMP_DIR/unique.paths" >"$TMP_DIR/sorted.paths" || return 1
	mapfile -d '' -t changed_paths <"$TMP_DIR/sorted.paths"
}

inspection_fallback() {
	local reason=$1
	run_unresolved_fallback "$base_oid" "$reason; conservatively falling back" 1
}

if ! snapshot_fingerprint "$TMP_DIR/fingerprint-1.sum" one; then
	inspection_fallback "Git or source inspection failed"
	exit $?
fi
if ! collect_paths; then
	inspection_fallback "changed-path inspection failed"
	exit $?
fi
if ! snapshot_fingerprint "$TMP_DIR/fingerprint-2.sum" two; then
	inspection_fallback "Git or source inspection failed"
	exit $?
fi
if ! cmp -s "$TMP_DIR/fingerprint-1.sum" "$TMP_DIR/fingerprint-2.sum"; then
	echo "sources are changing; rerun" >&2
	exit 1
fi

is_protected_path() {
	case "$1" in
	dev/deployment-e2e/* | dev/deployment-e2e.toml | dev/validate-deployment-lifecycle.sh | wr-tests/deployment/* | \
		wr-cli/src/cmd/bundle.rs | wr-cli/src/cmd/bundle_integrity.rs | wr-cli/src/cmd/deploy_config.rs | \
		wr-cli/src/cmd/managers.rs | wr-cli/src/cmd/node.rs | wr-cli/src/cmd/node_agent.rs | \
		wr-cli/src/cmd/node_backend.rs | wr-cli/src/cmd/service_gen.rs) return 0 ;;
	esac
	return 1
}

classify_path() {
	local path=$1
	case "$path" in
	Justfile | AGENTS.md | CLAUDE.md | dev/validate-all.sh | dev/validate-changed.sh | dev/test-validate-changed.sh | \
		docs/testing.md | docs/agents/wruntime-maintainer/validation.md | Cargo.toml | Cargo.lock | rust-toolchain | \
		rust-toolchain.toml | taplo.toml | bacon.toml | .cargo/* | proto/* | wr-common/* | wr-cli/* | examples/*)
		printf 'full' ;;
	wr-tests/guests/* | wr-tests/tests/wasm_*_host_test.rs | wit/* | wr-sdk/* | wr-sdk-macros/* | wr-build/* | wr-engine/*)
		printf 'wasm' ;;
	wr-manager/* | wr-proxy/* | wr-tests/*)
		printf 'workspace' ;;
	docs/*)
		printf 'docs' ;;
	*)
		if [[ "$path" != */* && "$path" == *.md ]]; then
			printf 'docs'
		else
			printf 'full'
		fi
		;;
	esac
}

profile=docs
reason="all changed paths are documentation-owned"
if (( ${#changed_paths[@]} == 0 )); then
	profile=none
	reason="no changed paths"
else
	protected=false
	full=false
	declare -A optimized=()
	for path in "${changed_paths[@]}"; do
		if is_protected_path "$path"; then
			protected=true
			continue
		fi
		owner=$(classify_path "$path")
		if [[ "$owner" == full ]]; then
			full=true
		else
			optimized["$owner"]=1
		fi
	done
	if [[ "$protected" == true ]]; then
		profile=protected-full
		reason="a deployment-sensitive protected path changed"
	elif [[ "$full" == true ]]; then
		profile=full
		reason="a broad, cross-cutting, or unknown path changed"
	elif (( ${#optimized[@]} != 1 )); then
		profile=full
		reason="changed paths span multiple focused profiles"
	else
		for profile in "${!optimized[@]}"; do :; done
		reason="all changed paths are ${profile}-owned"
	fi
fi

printf 'requested base: %q\n' "$requested_base"
printf 'resolved base: %s\n' "$base_oid"
printf 'changed paths:\n'
if (( ${#changed_paths[@]} == 0 )); then
	printf '  (none)\n'
else
	for path in "${changed_paths[@]}"; do
		printf '  %q\n' "$path"
	done
fi
printf 'selected profile: %s\n' "$profile"
printf 'reason: %s\n' "$reason"
printf 'commands:\n'
case "$profile" in
none)
	printf '  (none)\n'
	printf 'no changes relative to %s\n' "$base_oid"
	exit 0
	;;
docs)
	print_command git diff --check "$base_oid" --
	print_command just fmt-check
	printf '  %s\n' 'manual: review documentation links and navigation'
	;;
workspace)
	print_command git diff --check "$base_oid" --
	for command in fmt-check check lint test; do print_command just "$command"; done
	;;
wasm)
	print_command git diff --check "$base_oid" --
	for command in fmt-check fmt-examples-check check lint lint-examples build-wasm-guests test; do print_command just "$command"; done
	;;
full | protected-full)
	print_command just validate-all "${passthrough[@]}"
	;;
esac

if [[ "$profile" == docs || "$profile" == workspace || "$profile" == wasm ]]; then
	if (( ${#passthrough[@]} > 0 )); then
		echo "validate-all flags are not accepted by the $profile focused profile" >&2
		exit 2
	fi
elif [[ "$profile" == protected-full ]]; then
	has_deployment=false
	for option in "${passthrough[@]}"; do
		if [[ "$option" == --no-deployment-e2e ]]; then
			echo "protected-full rejects --no-deployment-e2e" >&2
			exit 2
		elif [[ "$option" == --deployment-e2e ]]; then
			has_deployment=true
		fi
	done
	if [[ "$has_deployment" != true ]]; then
		echo "protected-full requires --deployment-e2e" >&2
		exit 2
	fi
fi

if [[ "$explain" == true ]]; then
	exit 0
fi

run_command() {
	"$@"
	local status=$?
	if (( status != 0 )); then
		exit "$status"
	fi
}

case "$profile" in
docs)
	run_command git diff --check "$base_oid" --
	run_command just fmt-check
	printf '%s\n' 'manual: review documentation links and navigation'
	;;
workspace)
	run_command git diff --check "$base_oid" --
	for command in fmt-check check lint test; do run_command just "$command"; done
	;;
wasm)
	run_command git diff --check "$base_oid" --
	for command in fmt-check fmt-examples-check check lint lint-examples build-wasm-guests test; do run_command just "$command"; done
	;;
full | protected-full)
	run_command just validate-all "${passthrough[@]}"
	;;
esac

if ! snapshot_fingerprint "$TMP_DIR/fingerprint-3.sum" three; then
	echo "validation completed, but the source snapshot can no longer be inspected" >&2
	exit 1
fi
if ! cmp -s "$TMP_DIR/fingerprint-2.sum" "$TMP_DIR/fingerprint-3.sum"; then
	echo "validation commands succeeded for an older snapshot; sources changed during validation" >&2
	exit 1
fi
