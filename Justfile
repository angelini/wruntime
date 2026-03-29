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
db_url  := "postgres://postgres@localhost:" + pg_port

# Initialise a local Postgres data directory — run once before db-start
db-init:
    initdb -D {{pg_data}} --auth=trust --username=postgres
    echo "port = {{pg_port}}" >> {{pg_data}}/postgresql.conf
    @echo "Initialised — run 'just db-start' to start"

# Start the local Postgres instance and create the test DB
db-start-tests:
    pg_ctl -D {{pg_data}} -l {{pg_data}}/postgres.log start
    @until pg_isready -p {{pg_port}} -U postgres -q; do sleep 0.5; done
    createdb -p {{pg_port}} -U postgres wruntime_test 2>/dev/null || true
    @echo "Ready — WRUNTIME_TEST_DB_URL={{db_url}}/wruntime_test"

# Start the local Postgres instance and create the example DB
db-start-example:
    pg_ctl -D {{pg_data}} -l {{pg_data}}/postgres.log start
    @until pg_isready -p {{pg_port}} -U postgres -q; do sleep 0.5; done
    createdb -p {{pg_port}} -U postgres wruntime_example 2>/dev/null || true
    @echo "Ready — WRUNTIME_EXAMPLE_DB_URL={{db_url}}/wruntime_example"

# Stop the local Postgres instance
db-stop:
    pg_ctl -D {{pg_data}} stop

# ── Ecommerce Example ─────────────────────────────────────────────────────────

# Build WASM components for the ecommerce example
build-example:
    (cd ecommerce-example/inventory && cargo component build --release --target wasm32-wasip2)
    (cd ecommerce-example/client && cargo component build --release --target wasm32-wasip2)

# Run the full ecommerce example (requires Postgres — see `just db-start-example`)
example:
    bash ecommerce-example/run.sh

# ── Housekeeping ──────────────────────────────────────────────────────────────

# Remove build artifacts
clean:
    cargo clean
