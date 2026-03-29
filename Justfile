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

# ── Multi-node local development ──────────────────────────────────────────────

# Start node A proxy (listens :9001, proxy_address = "http://127.0.0.1:9001")
node-a-proxy:
    cargo run -p wr-proxy -- --config node-a/proxy.toml

# Start node A engine 1 (listens :9100)
node-a-engine-1:
    cargo run -p wr-engine -- --config node-a/engine-1.toml

# Start node A engine 2 (listens :9101)
node-a-engine-2:
    cargo run -p wr-engine -- --config node-a/engine-2.toml

# Start node B proxy (listens :9002, proxy_address = "http://127.0.0.1:9002")
node-b-proxy:
    cargo run -p wr-proxy -- --config node-b/proxy.toml

# Start node B engine 1 (listens :9200)
node-b-engine-1:
    cargo run -p wr-engine -- --config node-b/engine-1.toml

# ── Database (test) ───────────────────────────────────────────────────────────

pg_data := ".pg-test-data"
pg_port := "5433"
db_url  := "postgres://postgres@localhost:" + pg_port

# Initialise a local Postgres data directory — run once before db-start
db-init:
    initdb -D {{pg_data}} --auth=trust --username=postgres
    echo "port = {{pg_port}}" >> {{pg_data}}/postgresql.conf
    @echo "Initialised — run 'just db-start' to start"

# Start the local Postgres instance and create the example and test DBs
db-start:
    pg_ctl -D {{pg_data}} -l {{pg_data}}/postgres.log start
    @until pg_isready -p {{pg_port}} -U postgres -q; do sleep 0.5; done
    createdb -p {{pg_port}} -U postgres wruntime_example 2>/dev/null || true
    createdb -p {{pg_port}} -U postgres wruntime_test 2>/dev/null || true
    @echo "Ready — WRUNTIME_EXAMPLE_DB_URL={{db_url}}/wruntime_example"
    @echo "Ready — WRUNTIME_TEST_DB_URL={{db_url}}/wruntime_test"

# Stop the local Postgres instance
db-stop:
    pg_ctl -D {{pg_data}} stop

# Check whether Postgres is accepting connections on db_port
db-status:
    pg_isready -p {{pg_port}} -U postgres

# ── Ecommerce Example ─────────────────────────────────────────────────────────

# Compile ecommerce protobuf schemas to FileDescriptorSet binaries (.binpb)
build-schemas:
    protoc --descriptor_set_out=ecommerce-example/schemas/inventory.binpb \
           --include_imports \
           ecommerce-example/schemas/inventory.proto
    protoc --descriptor_set_out=ecommerce-example/schemas/client.binpb \
           --include_imports \
           ecommerce-example/schemas/client.proto

# Build WASM components and schemas for the ecommerce example
build-example: build-schemas
    (cd ecommerce-example/inventory && cargo component build --release --target wasm32-wasip2)
    (cd ecommerce-example/client && cargo component build --release --target wasm32-wasip2)

# Run the full ecommerce example (requires Postgres — see `just db-start-example`)
example: build-example build
    bash ecommerce-example/run.sh

# ── Observability (LGTM stack) ────────────────────────────────────────────────

lgtm_dir := "observability"

# Start the Grafana LGTM stack (Loki, Grafana, Tempo, Mimir)
obs-up:
    mkdir -p {{lgtm_dir}}/data
    docker run --rm -d \
        --name lgtm \
        -p 3000:3000 \
        -p 4317:4317 \
        -p 4318:4318 \
        -v $(pwd)/{{lgtm_dir}}/data:/data \
        grafana/otel-lgtm
    @echo "Grafana: http://localhost:3000 (admin/admin)"
    @echo "OTLP gRPC: localhost:4317  OTLP HTTP: localhost:4318"

# Stop the LGTM stack
obs-down:
    docker stop lgtm

# Tail logs from the LGTM container
obs-logs:
    docker logs -f lgtm

# Print LGTM container status
obs-status:
    docker ps --filter name=lgtm

# ── Housekeeping ──────────────────────────────────────────────────────────────

# Remove build artifacts
clean:
    cargo clean
