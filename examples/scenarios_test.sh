#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RUN_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/wr-example-scenarios-test.XXXXXX")"
MOCK_BIN="${RUN_ROOT}/bin"
scenario_pid=""

cleanup() {
	if [ -n "$scenario_pid" ] && kill -0 "$scenario_pid" 2>/dev/null; then
		kill -TERM -- "-${scenario_pid}" 2>/dev/null || true
		wait "$scenario_pid" 2>/dev/null || true
	fi
	rm -rf "$RUN_ROOT"
}
trap cleanup EXIT

mkdir -p "$MOCK_BIN"
cat >"${MOCK_BIN}/just" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"${JUST_CALLS:?}"
MOCK
chmod +x "${MOCK_BIN}/just"

names=(multi-node ecommerce stockmarket codegen)
scripts=(
	"examples/multi-node/scenario.sh"
	"examples/ecommerce/scenario.sh"
	"examples/stockmarket/scenario.sh"
	"examples/codegen/scenario.sh"
)
usage_needles=(
	"Node A engines: http://127.0.0.1:21010 and :21011"
	"Inventory: http://127.0.0.1:21010 + :21011"
	"Exchange  : :21010 + :21011 + :21012 (3 engine(s), DB-backed order book)"
	"External API: http://127.0.0.1:21020"
)
scenario_arguments=("" "" "--exchanges 3" "")

for index in "${!names[@]}"; do
	name="${names[$index]}"
	output="${RUN_ROOT}/${name}.output"
	calls="${RUN_ROOT}/${name}.calls"
	: >"$calls"
	read -r -a args <<<"${scenario_arguments[$index]}"

	setsid env PATH="${MOCK_BIN}:${PATH}" JUST_CALLS="$calls" \
		WRT_MANAGER_PORT=21000 WRT_PROXY_PORT=21001 WRT_PROXY_CONTROL_PORT=21002 \
		WRT_PROXY_PEER_PORT=21003 WRT_SECOND_PROXY_PORT=21004 \
		WRT_SECOND_PROXY_CONTROL_PORT=21005 WRT_SECOND_PROXY_PEER_PORT=21006 \
		WRT_ENGINE_BASE_PORT=21010 WRT_SIMULATOR_PORT=21015 WRT_EXTERNAL_PORT=21020 \
		bash "$REPO_ROOT/${scripts[$index]}" "${args[@]}" >"$output" 2>&1 &
	scenario_pid=$!
	for _ in $(seq 1 50); do
		if grep -Fq "${usage_needles[$index]}" "$output"; then
			break
		fi
		if ! kill -0 "$scenario_pid" 2>/dev/null; then
			echo "$name non-inline scenario exited before printing usage" >&2
			cat "$output" >&2
			exit 1
		fi
		sleep 0.1
	done
	if ! grep -Fq "${usage_needles[$index]}" "$output"; then
		echo "$name non-inline scenario did not print expected usage" >&2
		cat "$output" >&2
		exit 1
	fi
	if ! kill -0 "$scenario_pid" 2>/dev/null; then
		echo "$name non-inline scenario did not remain alive for interruption" >&2
		exit 1
	fi
	if [ -s "$calls" ]; then
		echo "$name non-inline scenario invoked an application action" >&2
		cat "$calls" >&2
		exit 1
	fi

	kill -TERM -- "-${scenario_pid}"
	wait "$scenario_pid"
	scenario_pid=""
done

# Exercise the stockmarket runner's cardinality loop without launching services.
# A fake helper records the engine configs handed to the shared foreground owner.
FAKE_ROOT="${RUN_ROOT}/stockmarket-forwarding"
FAKE_CONFIG_DIR="${FAKE_ROOT}/config"
FORWARD_CALLS="${FAKE_ROOT}/forward-calls"
mkdir -p "${FAKE_ROOT}/examples/stockmarket" "$FAKE_CONFIG_DIR"
cp "$REPO_ROOT/examples/stockmarket/run.sh" "${FAKE_ROOT}/examples/stockmarket/run.sh"
cat >"${FAKE_ROOT}/examples/helpers.sh" <<'HELPERS'
#!/usr/bin/env bash
set -euo pipefail
CONFIG_DIR="${FAKE_CONFIG_DIR:?}"
DB_URL=postgres://manager.test
JOB_DB_URL=postgres://jobs.test
S3_ENDPOINT=http://s3.test
S3_ACCESS_KEY=test
S3_SECRET_KEY=test
WRT_ENGINE_BASE_PORT=19100
WRT_SIMULATOR_PORT=19200
WRT_JOB_ADMIN_BASE_PORT=19300
render_config() {
	mkdir -p "$(dirname "$2")"
	: >"$2"
}
copy_config() { render_config "$@"; }
prepare_example_tenant_native() { :; }
prepare_manager_config() { printf '%s\n' "${CONFIG_DIR}/manager.toml"; }
prepare_proxy_config() { printf '%s\n' "$1"; }
create_s3_bucket() { :; }
clean_manager_state() { :; }
configure_dev_run() { printf 'manager=%s\n' "$1" >>"${FORWARD_CALLS:?}"; }
add_dev_proxy() { printf 'proxy=%s=%s\n' "$1" "$2" >>"${FORWARD_CALLS:?}"; }
add_dev_engine() { printf 'engine=%s\n' "$1" >>"${FORWARD_CALLS:?}"; }
run_example_scenario() { printf 'scenario=%s\n' "$*" >>"${FORWARD_CALLS:?}"; }
HELPERS

FAKE_CONFIG_DIR="$FAKE_CONFIG_DIR" FORWARD_CALLS="$FORWARD_CALLS" \
	bash "${FAKE_ROOT}/examples/stockmarket/run.sh" --exchanges 3 >/dev/null
mapfile -t forwarded_engines < <(grep '^engine=' "$FORWARD_CALLS")
expected_engines=(
	"engine=${FAKE_CONFIG_DIR}/stockmarket-exchange-0.toml"
	"engine=${FAKE_CONFIG_DIR}/stockmarket-exchange-1.toml"
	"engine=${FAKE_CONFIG_DIR}/stockmarket-exchange-2.toml"
	"engine=${FAKE_CONFIG_DIR}/stockmarket-ledger.toml"
	"engine=${FAKE_CONFIG_DIR}/stockmarket-simulator.toml"
)
if [ "${forwarded_engines[*]}" != "${expected_engines[*]}" ]; then
	printf 'stockmarket N=3 forwarded unexpected engine configs:\n%s\n' "${forwarded_engines[*]}" >&2
	exit 1
fi
if ! grep -Fxq 'scenario=examples/stockmarket/scenario.sh --exchanges 3' "$FORWARD_CALLS"; then
	echo "stockmarket runner did not forward N=3 to its scenario" >&2
	cat "$FORWARD_CALLS" >&2
	exit 1
fi

printf 'all non-inline scenarios print usage and remain interruptible; stockmarket forwards all N=3 engines\n'
