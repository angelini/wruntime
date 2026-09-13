#!/bin/sh
set -eu

# docker-library/postgres intentionally clears PGHOST and PGHOSTADDR in
# docker_process_sql. Compose therefore configures the server with both the
# image-default /var/run/postgresql socket used during official initialization
# and this named-volume socket used by the co-located provisioner.
postgres_socket=/var/run/wruntime-postgresql

# The official image enters as root. Install immutable input credentials into
# PostgreSQL-owned volumes before dropping privileges in docker-entrypoint.sh.
install -d -o postgres -g postgres -m 0700 /var/lib/postgresql/wruntime-tls
install -d -o postgres -g postgres -m 0700 /var/lib/postgresql/wruntime-config
install -d -o postgres -g postgres -m 0770 "$postgres_socket"
install -o postgres -g postgres -m 0600 /wr-input/server/key.pem /var/lib/postgresql/wruntime-tls/server.key
install -o postgres -g postgres -m 0644 /wr-input/server/leaf.pem /var/lib/postgresql/wruntime-tls/server.crt
install -o postgres -g postgres -m 0644 /wr-input/server/chain.pem /var/lib/postgresql/wruntime-tls/ca.crt
install -o postgres -g postgres -m 0600 /wr-config/pg_hba.conf /var/lib/postgresql/wruntime-config/pg_hba.conf
if [ ! -e /var/lib/postgresql/wruntime-config/pg_ident.conf ]; then
  printf '%s\n' '# wruntime postgres tenant mappings v1' > /var/lib/postgresql/wruntime-config/pg_ident.conf
fi
chown postgres:postgres /var/lib/postgresql/wruntime-config/pg_ident.conf
chmod 0600 /var/lib/postgresql/wruntime-config/pg_ident.conf
printf 'host=%s user=postgres dbname=postgres\n' "$postgres_socket" > /var/lib/postgresql/wruntime-config/admin-url
chown postgres:postgres /var/lib/postgresql/wruntime-config/admin-url
chmod 0600 /var/lib/postgresql/wruntime-config/admin-url
exec /usr/local/bin/docker-entrypoint.sh "$@"
