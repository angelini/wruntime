#!/usr/bin/env bash
set -euo pipefail
if [ "$#" -ne 2 ]; then
  echo "usage: dev/postgres-provision-native.sh <manifest-directory> <owner-worktree>" >&2
  exit 2
fi
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=shared-dev-state.sh
source "$repo/dev/shared-dev-state.sh"
manifest_root="$(cd "$1" && pwd)"
owner="$(cd "$2" && pwd)"
[ -f "$manifest_root/provisioning.toml" ] || { echo "missing $manifest_root/provisioning.toml" >&2; exit 2; }
exec env WRT_SHARED_DEV_STATE_ROOT="$WRT_SHARED_DEV_STATE_ROOT" docker compose \
  --project-name wruntime-dev --project-directory "$owner" -f "$owner/docker-compose.yml" \
  -f "$WRT_POSTGRES_COMPOSE_OVERRIDE" --profile postgres-tools run --rm \
  -v "$manifest_root:/work:ro" postgres-provisioner provision \
  --manifest /work/provisioning.toml \
  --admin-url-file /var/lib/postgresql/wruntime-config/admin-url \
  --pg-ident-target /var/lib/postgresql/wruntime-config/pg_ident.conf --output json
