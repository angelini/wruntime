#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"
# shellcheck source=shared-dev-state.sh
source "$root/dev/shared-dev-state.sh"
wrt_acquire_postgres_fixture_lock "$*"
wrt_require_compatible_fixture
exec "$@"
