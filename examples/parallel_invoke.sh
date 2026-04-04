#!/usr/bin/env bash
# Run the wr-cli invoke command 50 times in parallel and verify all succeeded.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$REPO_ROOT"

export WR_MANAGER="${WR_MANAGER:-http://127.0.0.1:9000}"

PIDS=()
TMPDIR_OUT="$(mktemp -d)"

for i in $(seq 1 50); do
    cargo run -p wr-cli -- invoke \
        --proxy http://127.0.0.1:9001 \
        --destination http://ecommerce.client/Run \
        --source loadtest --source-ns ecommerce \
        --body '{"count": 1000}' \
        >"${TMPDIR_OUT}/${i}.out" 2>&1 &
    PIDS+=($!)
done

echo "Waiting for 50 parallel invocations..."

FAILED=0
for i in "${!PIDS[@]}"; do
    pid="${PIDS[$i]}"
    run_num=$((i + 1))
    if wait "$pid"; then
        echo "[${run_num}/50] OK"
    else
        echo "[${run_num}/50] FAILED — output:"
        cat "${TMPDIR_OUT}/${run_num}.out"
        FAILED=$((FAILED + 1))
    fi
done

rm -rf "$TMPDIR_OUT"

if [ "$FAILED" -gt 0 ]; then
    echo ""
    echo "ERROR: ${FAILED}/50 invocations failed."
    exit 1
fi

echo ""
echo "All 50 invocations succeeded."
