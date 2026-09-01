#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
RUN_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/wr-ecommerce-scenario-test.XXXXXX")"
MOCK_BIN="${RUN_ROOT}/bin"
JUST_CALLS="${RUN_ROOT}/just-calls"
trap 'rm -rf "$RUN_ROOT"' EXIT

mkdir -p "$MOCK_BIN"
cat >"${MOCK_BIN}/just" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"${JUST_CALLS:?}"
MOCK
chmod +x "${MOCK_BIN}/just"

inline_output=$(PATH="${MOCK_BIN}:${PATH}" JUST_CALLS="$JUST_CALLS" \
	bash "$REPO_ROOT/examples/ecommerce/scenario.sh" --inline)
mapfile -t inline_calls <"$JUST_CALLS"
if [ "${#inline_calls[@]}" -ne 2 ]; then
	printf 'inline ecommerce scenario made %s calls, expected seed then client\n' "${#inline_calls[@]}" >&2
	exit 1
fi
if [[ "${inline_calls[0]}" != *"ecommerce.InventoryService/Seed"* ]] ||
	[[ "${inline_calls[1]}" != *"ecommerce.ClientService/Run"* ]]; then
	printf 'unexpected inline call order:\n%s\n' "${inline_calls[*]}" >&2
	exit 1
fi
if [ "$(grep -Fc '==> Seeding inventory...' <<<"$inline_output")" -ne 1 ]; then
	echo "inline ecommerce scenario did not report exactly one seed" >&2
	exit 1
fi

printf 'ecommerce inline seeds once before the client\n'
