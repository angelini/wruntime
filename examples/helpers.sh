#!/usr/bin/env bash
# Common helpers for example run.sh scripts.
# Source this file: source "$(dirname "$0")/../helpers.sh"

set -euo pipefail

# ── Parse --inline flag ──────────────────────────────────────────────────────
# Consumed by scripts that source this helper.
# shellcheck disable=SC2034
INLINE=false
HELPER_CONTRACT_TEST=false
for arg in "$@"; do
	case "$arg" in
	--inline) INLINE=true ;;
	--helper-contract-test) HELPER_CONTRACT_TEST=true ;;
	esac
done
export INLINE

# ── Repo root ────────────────────────────────────────────────────────────────
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"
# shellcheck source=dev/shared-dev-state.sh
source "$REPO_ROOT/dev/shared-dev-state.sh"
if [ "$HELPER_CONTRACT_TEST" != true ]; then
	wrt_require_compatible_fixture
fi
wrt_export_dev_endpoints

# ── Worktree endpoints ────────────────────────────────────────────────────────
DB_URL="$WRT_EXAMPLE_DB_URL"
JOB_DB_URL="$WRT_JOBS_DB_URL"
S3_ENDPOINT="$WRT_TEST_S3_ENDPOINT"
S3_ACCESS_KEY="$WRT_TEST_S3_ACCESS_KEY"
S3_SECRET_KEY="$WRT_TEST_S3_SECRET_KEY"
export RUST_LOG="${RUST_LOG:-info}"
export WR_MANAGER="https://127.0.0.1:${WRT_MANAGER_PORT}"

RUN_DIR="${WR_EXAMPLE_RUN_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/wr-example.XXXXXX")}"
CONFIG_DIR="${RUN_DIR}/config"
mkdir -p "${CONFIG_DIR}"

remove_example_run_directory() {
	rm -rf "$1"
}

assert_no_example_runtime_residue() {
	local residue
	residue=$(find "$RUN_DIR" -mindepth 1 -maxdepth 1 ! -name config -print)
	if [ -n "$residue" ]; then
		echo "Unexpected foreground-run residue under ${RUN_DIR}:" >&2
		printf '%s\n' "$residue" >&2
		return 1
	fi
}

cleanup_example_run() {
	local primary_status=$?
	local cleanup_status=0
	local final_status=$primary_status
	trap - EXIT INT TERM
	assert_no_example_runtime_residue || cleanup_status=$?
	if [ "$cleanup_status" -eq 0 ]; then
		remove_example_run_directory "${RUN_DIR}" || cleanup_status=$?
		if [ "$cleanup_status" -ne 0 ]; then
			echo "Cleanup failed: run-directory removal=${cleanup_status}:${RUN_DIR}" >&2
		fi
	else
		echo "Cleanup failed; retained example run directory: ${RUN_DIR}" >&2
	fi
	if [ "$primary_status" -eq 0 ] && [ "$cleanup_status" -ne 0 ]; then
		final_status=$cleanup_status
	elif [ "$primary_status" -ne 0 ] && [ "$cleanup_status" -ne 0 ]; then
		echo "Primary run failed with ${primary_status}; cleanup also failed with ${cleanup_status}." >&2
	fi
	exit "$final_status"
}
trap cleanup_example_run EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# ── Foreground topology arguments ────────────────────────────────────────────
DEV_RUN_ARGS=()

configure_dev_run() {
	local manager_config="$1"
	DEV_RUN_ARGS=(--manager-config "$manager_config")
}

add_dev_proxy() {
	local name="$1" config="$2"
	DEV_RUN_ARGS+=(--proxy-config "${name}=${config}")
}

add_dev_engine() {
	local config="$1"
	DEV_RUN_ARGS+=(--engine-config "$config")
}

run_dev_topology() {
	if [ "${#DEV_RUN_ARGS[@]}" -eq 0 ]; then
		echo "run_dev_topology requires configure_dev_run" >&2
		return 2
	fi
	"$CERT_CLI" dev run "${DEV_RUN_ARGS[@]}" -- "$@"
}

run_example_scenario() {
	local scenario="$1"
	shift
	local scenario_args=("$@")
	if [ "$INLINE" = true ]; then
		scenario_args+=(--inline)
	fi
	run_dev_topology bash "$scenario" "${scenario_args[@]}"
}

# ── Generate immutable profile-specific TLS credentials if missing ───────────
CERT_CLI="${CARGO_TARGET_DIR:-target}/debug/wr-cli"
EXAMPLE_CERTS_DIR="${WRT_EXAMPLE_CERTS_DIR:-certs}"
export WRT_EXAMPLE_CERTS_DIR="${EXAMPLE_CERTS_DIR}"

ensure_example_root() {
	local kind="$1" root="$2"
	if [ -e "$root/ca.crt" ] || [ -e "$root/ca.key" ]; then
		if [ ! -f "$root/ca.crt" ] || [ ! -f "$root/ca.key" ]; then
			echo "incomplete certificate root at $root; refusing to overwrite it" >&2
			return 1
		fi
		return
	fi
	"$CERT_CLI" cert init-root "$kind" --output "$root"
}

ensure_example_credential() {
	local destination="$1"
	shift
	local ca_dir=""
	local previous=""
	for argument in "$@"; do
		if [ "$previous" = "--ca-dir" ]; then
			ca_dir="$argument"
			break
		fi
		previous="$argument"
	done
	if [ -e "$destination" ] || [ -L "$destination" ]; then
		if ! "$CERT_CLI" cert verify "$destination" >/dev/null 2>&1; then
			echo "existing credential at $destination failed intrinsic verification; refusing to overwrite it" >&2
			return 1
		fi
		if [ -z "$ca_dir" ] || [ ! -f "$ca_dir/ca.crt" ]; then
			echo "active certificate root is unavailable for $destination; refusing to overwrite it" >&2
			return 1
		fi
		if ! openssl verify -CAfile "$ca_dir/ca.crt" "$destination/leaf.pem" >/dev/null 2>&1; then
			echo "existing credential at $destination does not match the active root; refusing to overwrite it" >&2
			return 1
		fi
		"$CERT_CLI" cert verify "$destination"
		return
	fi
	"$CERT_CLI" cert issue "$@" --destination "$destination"
}

ensure_example_root server "${EXAMPLE_CERTS_DIR}/runtime-server-root"
ensure_example_root client "${EXAMPLE_CERTS_DIR}/runtime-client-root"
ensure_example_credential "${EXAMPLE_CERTS_DIR}/runtime-manager-endpoint" manager-endpoint \
	--endpoint localhost --ip 127.0.0.1 --ca-dir "${EXAMPLE_CERTS_DIR}/runtime-server-root"
ensure_example_credential "${EXAMPLE_CERTS_DIR}/runtime-proxy-endpoint" proxy-peer-endpoint \
	--endpoint localhost --ip 127.0.0.1 --ca-dir "${EXAMPLE_CERTS_DIR}/runtime-server-root"
ensure_example_credential "${EXAMPLE_CERTS_DIR}/job-admin-engine-endpoint" engine-admin-endpoint \
	--endpoint localhost --ip 127.0.0.1 --ca-dir "${EXAMPLE_CERTS_DIR}/runtime-server-root"
ensure_example_credential "${EXAMPLE_CERTS_DIR}/runtime-human-client" human \
	--cluster-id default --name deployer --ca-dir "${EXAMPLE_CERTS_DIR}/runtime-client-root"
ensure_example_credential "${EXAMPLE_CERTS_DIR}/runtime-manager-client" manager \
	--cluster-id default --name manager-a --ca-dir "${EXAMPLE_CERTS_DIR}/runtime-client-root"
ensure_example_credential "${EXAMPLE_CERTS_DIR}/runtime-proxy-client" proxy \
	--cluster-id default --name proxy-a --ca-dir "${EXAMPLE_CERTS_DIR}/runtime-client-root"
ensure_example_credential "${EXAMPLE_CERTS_DIR}/runtime-node-agent-client" node-agent \
	--cluster-id default --name node-a --ca-dir "${EXAMPLE_CERTS_DIR}/runtime-client-root"

# All manager-facing orchestration probes use the named human identity generated
# alongside the rendered topology rather than process-global default paths.
export WR_CA_CERT="${EXAMPLE_CERTS_DIR}/runtime-server-root/ca.crt"
export WR_CLIENT_CERT="${EXAMPLE_CERTS_DIR}/runtime-human-client/leaf.pem"
export WR_CLIENT_KEY="${EXAMPLE_CERTS_DIR}/runtime-human-client/key.pem"

# ── Config rendering ─────────────────────────────────────────────────────────
render_config() {
	local src="$1" dest="$2"
	shift 2
	python3 - "$src" "$dest" "$@" <<'PY'
import os
import pathlib
import sys

src = pathlib.Path(sys.argv[1])
dest = pathlib.Path(sys.argv[2])
pairs = sys.argv[3:]
if len(pairs) % 2:
    raise SystemExit("render_config requires OLD NEW replacement pairs")
text = src.read_text()
certs_dir = os.environ.get("WRT_EXAMPLE_CERTS_DIR", "certs").rstrip("/")
text = text.replace('"certs/', f'"{certs_dir}/')
for old, new in zip(pairs[0::2], pairs[1::2]):
    if old not in text:
        raise SystemExit(f"{src}: expected template value not found: {old!r}")
    text = text.replace(old, new)
ports = {
    8080: int(os.environ["WRT_EXTERNAL_PORT"]),
    9000: int(os.environ["WRT_MANAGER_PORT"]),
    9001: int(os.environ["WRT_PROXY_PORT"]),
    9002: int(os.environ["WRT_PROXY_CONTROL_PORT"]),
    9003: int(os.environ["WRT_SECOND_PROXY_PORT"]),
    9004: int(os.environ["WRT_SECOND_PROXY_CONTROL_PORT"]),
    9100: int(os.environ["WRT_ENGINE_BASE_PORT"]),
    9101: int(os.environ["WRT_ENGINE_BASE_PORT"]) + 1,
    9150: int(os.environ["WRT_JOB_ADMIN_BASE_PORT"]),
    9151: int(os.environ["WRT_JOB_ADMIN_BASE_PORT"]) + 1,
    9200: int(os.environ["WRT_SIMULATOR_PORT"]),
    9443: int(os.environ["WRT_PROXY_PEER_PORT"]),
    9444: int(os.environ["WRT_SECOND_PROXY_PEER_PORT"]),
}
for original, allocated in ports.items():
    text = text.replace(f"127.0.0.1:{original}", f"127.0.0.1:{allocated}")
    text = text.replace(f"0.0.0.0:{original}", f"0.0.0.0:{allocated}")
dest.parent.mkdir(parents=True, exist_ok=True)
dest.write_text(text)
PY
}

copy_config() {
	render_config "$1" "$2"
}

# Consume this worktree's host-owned fixture. This command validates published artifacts
# and materializes tenant expectations into the caller's run-directory config copies.
prepare_example_tenant_native() {
	if [ "$#" -eq 0 ]; then
		echo "prepare_example_tenant_native requires engine config paths" >&2
		return 2
	fi
	local command=(cargo run --quiet -p wr-tests --example postgres_tenant_consume --
		--fixture "$WRT_POSTGRES_FIXTURE_DIR")
	local config
	for config in "$@"; do command+=(--config "$config"); done
	"${command[@]}"
	echo "==> Consumed host-prepared PostgreSQL tenant fixture"
}

# ── Prepare manager config ───────────────────────────────────────────────────
# Copies manager.toml and its relative policy dependency to the run config dir,
# then substitutes DB_URL. Returns the manager config path via stdout.
prepare_manager_config() {
	local dest="${CONFIG_DIR}/manager.toml"
	local policy_dest="${CONFIG_DIR}/policy/authorization.toml"
	copy_config examples/config/policy/authorization.toml "$policy_dest"
	render_config examples/config/manager.toml "$dest" "postgres://postgres@localhost:5433/wruntime_example" "${DB_URL}"
	echo "$dest"
}

# ── Prepare proxy config ─────────────────────────────────────────────────────
# Copies proxy.toml to the run config dir and substitutes DB_URL.
# Returns the path via stdout. Caller can append extra config after.
prepare_proxy_config() {
	local dest="${1:-${CONFIG_DIR}/proxy.toml}"
	render_config examples/config/proxy.toml "$dest" "postgres://postgres@localhost:5433/wruntime_example" "${DB_URL}"
	echo "$dest"
}

# ── Clean stale manager state ────────────────────────────────────────────────
clean_manager_state() {
	local manager_table
	echo "==> Cleaning manager state..."
	manager_table=$(psql "${DB_URL}" -Atqc "SELECT to_regclass('wr_system.wr_managers')")
	if [ -z "$manager_table" ]; then
		echo "   manager tables do not exist yet — first run"
		return 0
	fi
	psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
		"UPDATE wr_system.wr_manager_rollout_guard SET accepted_generation = NULL, accepted_digest = NULL, active_rollout_id = NULL WHERE singleton = TRUE; TRUNCATE wr_system.wr_manager_rollouts, wr_system.wr_manager_rollout_members, wr_system.wr_manager_rollout_events, wr_system.wr_engines, wr_system.wr_routing_rules, wr_system.wr_schemas, wr_system.wr_managers, wr_system.wr_secrets, wr_system.wr_nodes, wr_system.wr_node_deployments, wr_system.wr_node_operations CASCADE; INSERT INTO wr_system.wr_manager_rollout_guard (singleton) VALUES (TRUE) ON CONFLICT DO NOTHING"
}

# ── Create S3 bucket ─────────────────────────────────────────────────────────
# Usage: create_s3_bucket <bucket_name>
create_s3_bucket() {
	local bucket="$1" buckets
	echo "==> Ensuring S3 bucket '${bucket}' exists"
	buckets=$(AWS_ACCESS_KEY_ID="${S3_ACCESS_KEY}" AWS_SECRET_ACCESS_KEY="${S3_SECRET_KEY}" \
		aws --endpoint-url "${S3_ENDPOINT}" s3api list-buckets \
		--query 'Buckets[].Name' --output text)
	if grep -Fxq "$bucket" <<<"${buckets//$'\t'/$'\n'}"; then
		echo "   bucket already exists"
		return 0
	fi
	AWS_ACCESS_KEY_ID="${S3_ACCESS_KEY}" AWS_SECRET_ACCESS_KEY="${S3_SECRET_KEY}" \
		aws --endpoint-url "${S3_ENDPOINT}" s3 mb "s3://${bucket}"
}
