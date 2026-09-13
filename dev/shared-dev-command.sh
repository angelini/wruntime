#!/usr/bin/env bash
set -euo pipefail
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo"
# shellcheck source=shared-dev-state.sh
source "$repo/dev/shared-dev-state.sh"
action="${1:-}"
shift || true
case "$action" in
up) exec bash dev/postgres-fixture-up.sh up ;;
reprepare) exec bash dev/postgres-fixture-up.sh reprepare ;;
down)
  wrt_acquire_postgres_fixture_lock "dev-down"
  wrt_compose down "$@"
  ;;
ps)
  wrt_acquire_postgres_fixture_lock "dev-ps"
  wrt_compose ps "$@"
  ;;
logs)
  wrt_acquire_postgres_fixture_lock "dev-logs"
  wrt_compose logs -f "$@"
  ;;
reset-db)
  wrt_acquire_postgres_fixture_lock "dev-reset-db"
  wrt_require_compatible_fixture
  psql 'postgres://wr_manager_platform:wruntime-dev-manager@localhost:5433/wruntime_manager' -v ON_ERROR_STOP=1 -c "
    DO \$\$DECLARE r RECORD;
    BEGIN
      FOR r IN SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE 'wr\\_\\_%' ESCAPE '\\' LOOP
        EXECUTE 'DROP SCHEMA \"' || r.schema_name || '\" CASCADE';
      END LOOP;
      DROP SCHEMA IF EXISTS wr_system CASCADE;
      DROP TABLE IF EXISTS refinery_schema_history CASCADE;
      FOR r IN SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename LIKE 'wr\\_%' ESCAPE '\\' LOOP
        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
      END LOOP;
    END\$\$;"
  ;;
*) echo "usage: dev/shared-dev-command.sh up|reprepare|down|ps|logs|reset-db" >&2; exit 2 ;;
esac
