#!/usr/bin/env bash
set -euo pipefail

INLINE=false
for arg in "$@"; do
	case "$arg" in
	--inline) INLINE=true ;;
	esac
done

if [ "$INLINE" = true ]; then
	if [ "${WRT_ASSERT_ENGINE_ADMIN_ENV_ABSENT:-0}" = 1 ]; then
		engine_count=0
		for environ in /proc/[0-9]*/environ; do
			cmdline="${environ%/environ}/cmdline"
			[ -r "$cmdline" ] && tr '\0' ' ' <"$cmdline" | grep -Eq '(^|[/ ])wr-engine([[:space:]]|$)' || continue
			engine_count=$((engine_count + 1))
			if tr '\0' '\n' <"$environ" | grep -Eq '^WRT_.*(ADMIN|IDENT|POSTGRES.*(URL|CERT|KEY))=|postgres-admin-url|/wruntime-config/pg_ident'; then
				echo "engine process inherited PostgreSQL administrative material" >&2
				exit 1
			fi
		done
		[ "$engine_count" -gt 0 ] || { echo "no engine process found for environment inspection" >&2; exit 1; }
		echo "==> Verified engine environments contain no PostgreSQL administrative inputs"
	fi
	# The inline application seed runs exactly once, after dev run has proved
	# every service READY and every proxy has crossed the routing barrier.
	echo "==> Seeding inventory..."
	just cli invoke \
		--proxy http://127.0.0.1:9001 \
		--destination http://ecommerce.inventory/ecommerce.InventoryService/Seed \
		--source bootstrap \
		--source-ns ecommerce \
		--body ''

	echo "==> Running client inline with {\"count\": 1}..."
	just cli invoke \
		--proxy http://127.0.0.1:9001 \
		--destination http://ecommerce.client/ecommerce.ClientService/Run \
		--source loadtest --source-ns ecommerce \
		--body '{"count": 1}'
	exit $?
fi

cat <<'USAGE'

All services running. Press Ctrl-C to stop.
  Manager  : https://127.0.0.1:9000
  Proxy    : http://127.0.0.1:9001
  Inventory: http://127.0.0.1:9100 + :9101 (2 engines, shared Postgres)
  Client   : http://127.0.0.1:9200 (3 instances, ServiceGuest)

Trigger a load run (default 100 iterations):
  just cli invoke \
    --manager https://127.0.0.1:9000 \
    --proxy http://127.0.0.1:9001 \
    --destination http://ecommerce.client/ecommerce.ClientService/Run \
    --source loadtest --source-ns ecommerce \
    --body ''

Trigger with a custom request count (e.g. 1000):
  just cli invoke \
    --manager https://127.0.0.1:9000 \
    --proxy http://127.0.0.1:9001 \
    --destination http://ecommerce.client/ecommerce.ClientService/Run \
    --source loadtest --source-ns ecommerce \
    --body '{"count": 1000}'

Inspect metrics:
  just cli --manager https://127.0.0.1:9000 metrics summary
USAGE

trap 'exit 0' INT TERM
while true; do
	sleep 3600 &
	wait $! || exit 0
done
