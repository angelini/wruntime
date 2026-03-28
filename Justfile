# wruntime Justfile — common development tasks

# Default: list available recipes
default:
    @just --list

# ── Build ─────────────────────────────────────────────────────────────────────

# Build all workspace crates (debug)
build:
    cargo build

# Build all workspace crates (release)
build-release:
    cargo build --release

# Check for compile errors without producing artifacts
check:
    cargo check

# ── Lint & Format ─────────────────────────────────────────────────────────────

# Format all source code
fmt:
    cargo fmt --all

# Check formatting without modifying files
fmt-check:
    cargo fmt --all -- --check

# Run Clippy lints across the workspace
lint:
    cargo clippy --all-targets --all-features -- -D warnings

# Format and lint
tidy: fmt lint

# ── Test ──────────────────────────────────────────────────────────────────────

# Run all tests
test:
    cargo test

# Run integration tests only
test-integration:
    cargo test -p wr-tests

# Run a single test by name
test-one name:
    cargo test {{name}}

# ── Run services ──────────────────────────────────────────────────────────────

# Run wr-manager
manager:
    cargo run -p wr-manager -- --config manager.toml

# Run wr-proxy
proxy:
    cargo run -p wr-proxy -- --config proxy.toml

# Run wr-engine
engine:
    cargo run -p wr-engine -- --config engine.toml

# Run wr-manager (release build)
manager-release:
    cargo run --release -p wr-manager -- --config manager.toml

# Run wr-proxy (release build)
proxy-release:
    cargo run --release -p wr-proxy -- --config proxy.toml

# Run wr-engine (release build)
engine-release:
    cargo run --release -p wr-engine -- --config engine.toml

# ── Database (test) ───────────────────────────────────────────────────────────

pg_data := ".pg-test-data"
pg_port := "5433"
db_name := "wruntime_test"
db_url  := "postgres://postgres@localhost:" + pg_port + "/" + db_name

# Initialise a local Postgres data directory — run once before db-start
db-init:
    initdb -D {{pg_data}} --auth=trust --username=postgres
    echo "port = {{pg_port}}" >> {{pg_data}}/postgresql.conf
    @echo "Initialised — run 'just db-start' to start"

# Start the local Postgres instance
db-start:
    pg_ctl -D {{pg_data}} -l {{pg_data}}/postgres.log start
    @until pg_isready -p {{pg_port}} -U postgres -q; do sleep 0.5; done
    createdb -p {{pg_port}} -U postgres {{db_name}} 2>/dev/null || true
    @echo "Ready — WRUNTIME_TEST_DB_URL={{db_url}}"

# Stop the local Postgres instance
db-stop:
    pg_ctl -D {{pg_data}} stop

# Run integration tests with a temporary local Postgres instance (init + start + test + stop)
test-db:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ ! -d "{{pg_data}}" ]; then
        initdb -D {{pg_data}} --auth=trust --username=postgres
        echo "port = {{pg_port}}" >> {{pg_data}}/postgresql.conf
    fi
    pg_ctl -D {{pg_data}} -l {{pg_data}}/postgres.log start
    trap 'pg_ctl -D {{pg_data}} stop -m fast' EXIT
    until pg_isready -p {{pg_port}} -U postgres -q; do sleep 0.5; done
    createdb -p {{pg_port}} -U postgres {{db_name}} 2>/dev/null || true
    WRUNTIME_TEST_DB_URL="{{db_url}}" cargo test -p wr-tests

# ── Housekeeping ──────────────────────────────────────────────────────────────

# Remove build artifacts
clean:
    cargo clean
