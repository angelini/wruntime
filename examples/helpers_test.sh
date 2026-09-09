#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RUN_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/wr-example-helper-test.XXXXXX")"
TEST_CERTS_DIR="${RUN_ROOT}/config/certs"
CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$REPO_ROOT/target}" \
	WRT_EXAMPLE_CERTS_DIR="$TEST_CERTS_DIR" \
	WR_EXAMPLE_RUN_DIR="$RUN_ROOT" \
	source "$REPO_ROOT/examples/helpers.sh" --inline

for credential in runtime-manager-endpoint runtime-proxy-endpoint job-admin-engine-endpoint runtime-human-client runtime-manager-client runtime-proxy-client; do
	"$CERT_CLI" cert verify "${TEST_CERTS_DIR}/${credential}" >/dev/null
done
valid_credential="${TEST_CERTS_DIR}/runtime-human-client"
valid_before="${TEST_CERTS_DIR}/valid-before"
cp -R "$valid_credential" "$valid_before"
ensure_example_credential "$valid_credential" human \
	--cluster-id default --name deployer --ca-dir "${TEST_CERTS_DIR}/runtime-client-root" >/dev/null
if ! diff -r "$valid_before" "$valid_credential" >/dev/null; then
	echo "helper mutated a valid reusable credential" >&2
	exit 1
fi
if grep -Eq 'cert (init-ca|generate)' "$REPO_ROOT/examples/helpers.sh"; then
	echo "helper retains removed certificate bootstrap commands" >&2
	exit 1
fi
stale_credential="${RUN_ROOT}/config/stale-credential"
cp -R "${TEST_CERTS_DIR}/runtime-human-client" "$stale_credential"
printf 'invalid metadata\n' >"${stale_credential}/metadata.json"
stale_before="${RUN_ROOT}/config/stale-before"
cp -R "$stale_credential" "$stale_before"
if ensure_example_credential "$stale_credential" human \
	--cluster-id default --name deployer --ca-dir "${TEST_CERTS_DIR}/runtime-client-root" >/dev/null 2>&1; then
	echo "helper accepted a stale immutable credential" >&2
	exit 1
fi
if ! diff -r "$stale_before" "$stale_credential" >/dev/null; then
	echo "helper mutated a stale immutable credential while failing closed" >&2
	exit 1
fi

wrong_root="${RUN_ROOT}/config/wrong-client-root"
wrong_root_credential="${RUN_ROOT}/config/wrong-root-credential"
"$CERT_CLI" cert init-root client --output "$wrong_root" >/dev/null
"$CERT_CLI" cert issue human --cluster-id default --name deployer \
	--ca-dir "$wrong_root" --destination "$wrong_root_credential" >/dev/null
wrong_root_before="${RUN_ROOT}/config/wrong-root-before"
cp -R "$wrong_root_credential" "$wrong_root_before"
if ensure_example_credential "$wrong_root_credential" human \
	--cluster-id default --name deployer --ca-dir "${TEST_CERTS_DIR}/runtime-client-root" >/dev/null 2>&1; then
	echo "helper accepted a credential issued by the wrong active root" >&2
	exit 1
fi
if ! diff -r "$wrong_root_before" "$wrong_root_credential" >/dev/null; then
	echo "helper mutated a wrong-root credential while failing closed" >&2
	exit 1
fi

manager="${CONFIG_DIR}/manager.toml"
prepared_manager="$(prepare_manager_config)"
if [ "$prepared_manager" != "$manager" ]; then
	echo "prepare_manager_config returned an unexpected path: $prepared_manager" >&2
	exit 1
fi
if ! cmp -s examples/config/policy/authorization.toml "${CONFIG_DIR}/policy/authorization.toml"; then
	echo "prepare_manager_config did not stage the relative authorization policy" >&2
	exit 1
fi
if ! grep -Fq 'policy_file = "policy/authorization.toml"' "$manager"; then
	echo "prepare_manager_config changed the settled relative policy path" >&2
	exit 1
fi
proxy_a="${CONFIG_DIR}/proxy-a.toml"
proxy_b="${CONFIG_DIR}/proxy-b.toml"
configure_dev_run "$manager"
add_dev_proxy primary "$proxy_a"
add_dev_proxy node-b "$proxy_b"
for index in 1 2 3; do
	add_dev_engine "${CONFIG_DIR}/engine-${index}.toml"
done

expected=(
	--manager-config "$manager"
	--proxy-config "primary=${proxy_a}"
	--proxy-config "node-b=${proxy_b}"
	--engine-config "${CONFIG_DIR}/engine-1.toml"
	--engine-config "${CONFIG_DIR}/engine-2.toml"
	--engine-config "${CONFIG_DIR}/engine-3.toml"
)
if [ "${DEV_RUN_ARGS[*]}" != "${expected[*]}" ]; then
	printf 'unexpected dev run args:\n actual: %q\nexpected: %q\n' "${DEV_RUN_ARGS[*]}" "${expected[*]}" >&2
	exit 1
fi

if find "$RUN_ROOT" -mindepth 1 -maxdepth 1 ! -name config -print -quit | grep -q .; then
	echo "helper created persistent state outside its temporary config directory" >&2
	exit 1
fi
if grep -Eq 'state-dir|dev-state|supervisor|wr-cli dev (up|deploy|down|status|wait|start-proxy)' "$REPO_ROOT/examples/helpers.sh"; then
	echo "helper retains removed cross-command lifecycle state or commands" >&2
	exit 1
fi
if grep -Eq 'wr-cli dev build' "$REPO_ROOT/examples/helpers.sh"; then
	echo "helper must not build guest artifacts during foreground execution" >&2
	exit 1
fi

touch "$RUN_ROOT/dev-supervisor.sock"
if assert_no_example_runtime_residue >/dev/null 2>&1; then
	echo "helper did not detect persistent runtime residue" >&2
	exit 1
fi
rm "$RUN_ROOT/dev-supervisor.sock"
assert_no_example_runtime_residue

printf 'foreground helper arguments preserve named proxies and 3 rendered engines with no persistent residue\n'
