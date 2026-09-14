#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"
# shellcheck source=shared-dev-state.sh
source "$root/dev/shared-dev-state.sh"
wrt_require_compatible_fixture
stage="$(mktemp -d "${TMPDIR:-/tmp}/wr-tenant-e2e.XXXXXX")"
trap 'rm -rf "$stage"' EXIT INT TERM
mkdir -p "$stage/config"
cp examples/stockmarket/engine-exchange.toml "$stage/config/stock-exchange.toml"
cp examples/stockmarket/engine-ledger.toml "$stage/config/stock-ledger.toml"
cp examples/ecommerce/engine-inventory-1.toml "$stage/config/ecommerce.toml"

cargo run --quiet -p wr-tests --example postgres_tenant_consume -- \
  --fixture "$WRT_POSTGRES_FIXTURE_DIR" \
  --config "$stage/config/stock-exchange.toml" \
  --config "$stage/config/stock-ledger.toml" \
  --config "$stage/config/ecommerce.toml"

cargo test -p wr-tests --features native-postgres-e2e \
  --test tenant_isolation_e2e_test -- --nocapture

# Exercise real engines through the production certificate connector and inspect their
# environments. The example independently consumes the same fixed ready fixture.
WRT_SECRET_ENCRYPTION_KEY="${WRT_SECRET_ENCRYPTION_KEY:-$(openssl rand -hex 32)}" \
  WRT_ASSERT_ENGINE_ADMIN_ENV_ABSENT=1 \
  bash examples/ecommerce/run.sh --inline
