#!/usr/bin/env bash
set -euo pipefail
# Greenfield platform bootstrap only. Namespace databases, roles, schemas,
# migration ledgers, and pg_ident mappings belong exclusively to the offline
# provision/migrate phase.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<'SQL'
CREATE ROLE wr_manager_platform LOGIN PASSWORD 'wruntime-dev-manager';
CREATE ROLE wr_jobs_platform LOGIN PASSWORD 'wruntime-dev-jobs';
CREATE DATABASE wruntime_manager OWNER wr_manager_platform;
CREATE DATABASE wruntime_jobs OWNER wr_jobs_platform;
CREATE DATABASE wruntime_test;
CREATE DATABASE wruntime_example;
REVOKE CONNECT,TEMPORARY ON DATABASE wruntime_manager FROM PUBLIC;
REVOKE CONNECT,TEMPORARY ON DATABASE wruntime_jobs FROM PUBLIC;
GRANT CONNECT,TEMPORARY ON DATABASE wruntime_manager TO wr_manager_platform;
GRANT CONNECT,TEMPORARY ON DATABASE wruntime_jobs TO wr_jobs_platform;
SQL
