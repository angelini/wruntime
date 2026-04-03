#!/usr/bin/env bash
set -e
psql -U postgres -c "CREATE DATABASE wruntime_example;"
psql -U postgres -c "CREATE DATABASE wruntime_test;"

# Lower-privilege role for guest WASM module database pools.
# Modules connect as wr_guest; schema provisioning and migrations use postgres.
psql -U postgres <<'SQL'
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'wr_guest') THEN
        CREATE ROLE wr_guest LOGIN;
    END IF;
END
$$;

-- Deny access to control-plane tables in public schema.
REVOKE ALL ON SCHEMA public FROM wr_guest;
GRANT USAGE ON SCHEMA public TO wr_guest;

-- Apply the same to both databases.
\c wruntime_example
REVOKE ALL ON SCHEMA public FROM wr_guest;
GRANT USAGE ON SCHEMA public TO wr_guest;

\c wruntime_test
REVOKE ALL ON SCHEMA public FROM wr_guest;
GRANT USAGE ON SCHEMA public TO wr_guest;
SQL
