#!/usr/bin/env bash
set -euo pipefail
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo"
# shellcheck source=shared-dev-state.sh
source "$repo/dev/shared-dev-state.sh"
# shellcheck source=postgres-pki-permissions.sh
source "$repo/dev/postgres-pki-permissions.sh"
root="$WRT_POSTGRES_PKI_DIR"
cli="${CARGO_TARGET_DIR:-target}/debug/wr-cli"
cargo build -p wr-cli
mkdir -p "$root"
if [ ! -f "$root/root/ca.crt" ]; then
  "$cli" cert init-root server --output "$root/root"
fi
if [ ! -d "$root/server" ]; then
  "$cli" cert issue postgres-server --endpoint postgres.internal --ip 127.0.0.1 --ca-dir "$root/root" --destination "$root/server"
fi
if [ ! -d "$root/node-a" ]; then
  "$cli" cert issue postgres-client --cluster-id default --name node-a --ca-dir "$root/root" --destination "$root/node-a"
fi
"$cli" cert verify "$root/server"
"$cli" cert verify "$root/node-a"
cat "$root/node-a/leaf.pem" "$root/node-a/chain.pem" >"$root/node-a-public.pem"
# Only the combined leaf+issuer bundle crosses into the UID-70 provisioner.
# Issuance/server/client private directories and every key remain owner-only.
wrt_set_postgres_pki_permissions "$root"
