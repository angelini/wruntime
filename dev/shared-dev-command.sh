#!/usr/bin/env bash
set -euo pipefail
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo"
# shellcheck source=shared-dev-state.sh
source "$repo/dev/shared-dev-state.sh"
action="${1:-}"
shift || true
case "$action" in
up) exec bash dev/postgres-fixture-up.sh ;;
down)
  wrt_acquire_postgres_fixture_lock "dev-down"
  wrt_compose down "$@"
  ;;
ps)
  wrt_compose ps "$@"
  ;;
logs)
  wrt_compose logs -f "$@"
  ;;
endpoints)
  wrt_export_dev_endpoints
  printf 'Compose:     %s (slot %s)\n' "$WRT_COMPOSE_PROJECT_NAME" "$WRT_WORKTREE_SLOT"
  printf 'Postgres:    127.0.0.1:%s\n' "$WRT_POSTGRES_PORT"
  printf '             example: %s\n' "$WRT_EXAMPLE_DB_URL"
  printf '             test:    %s\n' "$WRT_TEST_DB_URL"
  printf 'Grafana:     http://127.0.0.1:%s (admin/admin)\n' "$WRT_GRAFANA_PORT"
  printf 'OTLP gRPC:   127.0.0.1:%s\n' "$WRT_OTLP_GRPC_PORT"
  printf 'OTLP HTTP:   127.0.0.1:%s\n' "$WRT_OTLP_HTTP_PORT"
  printf 'RustFS S3:   %s\n' "$WRT_TEST_S3_ENDPOINT"
  printf 'RustFS Web:  http://127.0.0.1:%s\n' "$WRT_S3_CONSOLE_PORT"
  printf 'S3 buckets:  test-bucket, stockmarket, codegen (ready)\n'
  ;;
reset-db)
  wrt_acquire_postgres_fixture_lock "dev-reset-db"
  wrt_require_compatible_fixture
  wrt_export_dev_endpoints
  psql "$WRT_EXAMPLE_DB_URL" -v ON_ERROR_STOP=1 -c "
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
reset-blobstore)
  bucket="${1:-codegen}"
  wrt_require_compatible_fixture
  wrt_export_dev_endpoints
  export AWS_ACCESS_KEY_ID="$WRT_TEST_S3_ACCESS_KEY" AWS_SECRET_ACCESS_KEY="$WRT_TEST_S3_SECRET_KEY"
  aws --endpoint-url "$WRT_TEST_S3_ENDPOINT" s3 mb "s3://$bucket" 2>/dev/null || true
  aws --endpoint-url "$WRT_TEST_S3_ENDPOINT" s3 rm "s3://$bucket" --recursive
  printf 'Cleared s3://%s\n' "$bucket"
  ;;
*) echo "usage: dev/shared-dev-command.sh up|down|ps|logs|endpoints|reset-db|reset-blobstore [bucket]" >&2; exit 2 ;;
esac
