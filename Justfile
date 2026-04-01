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
    WRT_TEST_DB_URL={{db_url_test}} \
    WRT_TEST_S3_ENDPOINT={{s3_endpoint}} \
    WRT_TEST_S3_ACCESS_KEY={{s3_access_key}} \
    WRT_TEST_S3_SECRET_KEY={{s3_secret_key}} \
    cargo test

# Run integration tests only
test-integration:
    WRT_TEST_DB_URL={{db_url_test}} \
    WRT_TEST_S3_ENDPOINT={{s3_endpoint}} \
    WRT_TEST_S3_ACCESS_KEY={{s3_access_key}} \
    WRT_TEST_S3_SECRET_KEY={{s3_secret_key}} \
    cargo test -p wr-tests

# Run a single test by name
test-one name:
    WRT_TEST_DB_URL={{db_url_test}} \
    WRT_TEST_S3_ENDPOINT={{s3_endpoint}} \
    WRT_TEST_S3_ACCESS_KEY={{s3_access_key}} \
    WRT_TEST_S3_SECRET_KEY={{s3_secret_key}} \
    cargo test {{name}}

# ── Run services ──────────────────────────────────────────────────────────────

# Run wr-manager
manager config="examples/config/manager.toml":
    cargo run -p wr-manager -- --config {{config}}

# Run wr-proxy
proxy config="examples/config/proxy.toml":
    cargo run -p wr-proxy -- --config {{config}}

# Run wr-engine
engine config="examples/config/engine.toml":
    cargo run -p wr-engine -- --config {{config}}

# Run wr-manager (release build)
manager-release config="examples/config/manager.toml":
    cargo run --release -p wr-manager -- --config {{config}}

# Run wr-proxy (release build)
proxy-release config="examples/config/proxy.toml":
    cargo run --release -p wr-proxy -- --config {{config}}

# Run wr-engine (release build)
engine-release config="examples/config/engine.toml":
    cargo run --release -p wr-engine -- --config {{config}}

# ── Multi-node local development ──────────────────────────────────────────────

# Start node A proxy (listens :9001, proxy_address = "http://127.0.0.1:9001")
node-a-proxy:
    cargo run -p wr-proxy -- --config examples/multi-node/node-a/proxy.toml

# Start node A engine 1 (listens :9100)
node-a-engine-1:
    cargo run -p wr-engine -- --config examples/multi-node/node-a/engine-1.toml

# Start node A engine 2 (listens :9101)
node-a-engine-2:
    cargo run -p wr-engine -- --config examples/multi-node/node-a/engine-2.toml

# Start node B proxy (listens :9002, proxy_address = "http://127.0.0.1:9002")
node-b-proxy:
    cargo run -p wr-proxy -- --config examples/multi-node/node-b/proxy.toml

# Start node B engine 1 (listens :9200)
node-b-engine-1:
    cargo run -p wr-engine -- --config examples/multi-node/node-b/engine-1.toml

# ── Dev infrastructure (Docker Compose) ──────────────────────────────────────

db_url_example := "postgres://postgres@localhost:5433/wruntime_example"
db_url_test    := "postgres://postgres@localhost:5433/wruntime_test"
s3_endpoint    := "http://localhost:8900"
s3_access_key  := "rustfsadmin"
s3_secret_key  := "rustfsadmin"

# Start all dev services (Postgres, LGTM, RustFS S3) and create test buckets
dev-up:
    mkdir -p dev/observability/data
    docker compose up -d
    @echo "Postgres:   localhost:5433"
    @echo "            example: {{db_url_example}}"
    @echo "            test:    {{db_url_test}}"
    @echo "Grafana:    http://localhost:3000  (admin/admin)"
    @echo "OTLP gRPC:  localhost:4317"
    @echo "OTLP HTTP:  localhost:4318"
    @echo "RustFS S3:  {{s3_endpoint}}"
    @echo "RustFS Web: http://localhost:8901"
    @sleep 2
    -AWS_ACCESS_KEY_ID={{s3_access_key}} AWS_SECRET_ACCESS_KEY={{s3_secret_key}} \
        aws --endpoint-url {{s3_endpoint}} s3 mb s3://test-bucket 2>/dev/null
    @echo "S3 bucket:  test-bucket (created)"

# Stop all dev services
dev-down:
    docker compose down

# Tail logs — optionally filter to one service: just dev-logs postgres
dev-logs service="":
    docker compose logs -f {{service}}

# Show running container status
dev-ps:
    docker compose ps

# ── WASM Guest Test Harness ───────────────────────────────────────────────────

# Compile test guest protobuf schemas to FileDescriptorSet binaries (.binpb)
build-test-schemas:
    protoc --descriptor_set_out=wr-tests/guests/schemas/db_test.binpb \
           --include_imports wr-tests/guests/schemas/db_test.proto
    protoc --descriptor_set_out=wr-tests/guests/schemas/tracing_test.binpb \
           --include_imports wr-tests/guests/schemas/tracing_test.proto
    protoc --descriptor_set_out=wr-tests/guests/schemas/blobstore_test.binpb \
           --include_imports wr-tests/guests/schemas/blobstore_test.proto
    protoc --descriptor_set_out=wr-tests/guests/schemas/http_test.binpb \
           --include_imports wr-tests/guests/schemas/http_test.proto
    protoc --descriptor_set_out=wr-tests/guests/schemas/llm_test.binpb \
           --include_imports wr-tests/guests/schemas/llm_test.proto

# Build WASM test guest components
build-test-guests: build-test-schemas
    (cd wr-tests/guests/db-guest && cargo component build --release --target wasm32-wasip2)
    (cd wr-tests/guests/tracing-guest && cargo component build --release --target wasm32-wasip2)
    (cd wr-tests/guests/blobstore-guest && cargo component build --release --target wasm32-wasip2)
    (cd wr-tests/guests/http-guest && cargo component build --release --target wasm32-wasip2)
    (cd wr-tests/guests/llm-guest && cargo component build --release --target wasm32-wasip2)

# Run all WASM host binding tests (sets env vars for dev infrastructure automatically)
test-wasm: build-test-guests
    WRT_TEST_DB_URL={{db_url_test}} \
    WRT_TEST_S3_ENDPOINT={{s3_endpoint}} \
    WRT_TEST_S3_ACCESS_KEY={{s3_access_key}} \
    WRT_TEST_S3_SECRET_KEY={{s3_secret_key}} \
    cargo test -p wr-tests --test wasm_host_test

# Run a subset of WASM tests by filter (e.g. `just test-wasm-one db`, `just test-wasm-one tracing`, `just test-wasm-one blobstore`)
test-wasm-one filter: build-test-guests
    WRT_TEST_DB_URL={{db_url_test}} \
    WRT_TEST_S3_ENDPOINT={{s3_endpoint}} \
    WRT_TEST_S3_ACCESS_KEY={{s3_access_key}} \
    WRT_TEST_S3_SECRET_KEY={{s3_secret_key}} \
    cargo test -p wr-tests --test wasm_host_test wasm_{{filter}}

# ── Ecommerce Example ─────────────────────────────────────────────────────────

# Compile ecommerce protobuf schemas to FileDescriptorSet binaries (.binpb)
build-schemas:
    protoc --descriptor_set_out=examples/ecommerce/schemas/inventory.binpb \
           --include_imports \
           examples/ecommerce/schemas/inventory.proto
    protoc --descriptor_set_out=examples/ecommerce/schemas/client.binpb \
           --include_imports \
           examples/ecommerce/schemas/client.proto

# Build WASM components and schemas for the ecommerce example
build-example: build-schemas
    (cd examples/ecommerce/inventory && cargo component build --release --target wasm32-wasip2)
    (cd examples/ecommerce/client && cargo component build --release --target wasm32-wasip2)

# Run the full ecommerce example (requires Postgres — see `just dev-up`)
example: build-example build
    DB_URL={{db_url_example}} bash examples/ecommerce/run.sh

# Run the ecommerce example inline (single invocation, exits on failure)
example-inline: build-example build
    DB_URL={{db_url_example}} bash examples/ecommerce/run.sh --inline

# ── Stock Market Example ──────────────────────────────────────────────────────

# Compile stockmarket protobuf schemas to FileDescriptorSet binaries (.binpb)
build-stockmarket-schemas:
    protoc --descriptor_set_out=examples/stockmarket/schemas/exchange.binpb \
           --include_imports \
           examples/stockmarket/schemas/exchange.proto
    protoc --descriptor_set_out=examples/stockmarket/schemas/ledger.binpb \
           --include_imports \
           examples/stockmarket/schemas/ledger.proto
    protoc --descriptor_set_out=examples/stockmarket/schemas/simulator.binpb \
           --include_imports \
           examples/stockmarket/schemas/simulator.proto

# Build WASM components and schemas for the stockmarket example
build-stockmarket: build-stockmarket-schemas
    (cd examples/stockmarket/exchange && cargo component build --release --target wasm32-wasip2)
    (cd examples/stockmarket/ledger && cargo component build --release --target wasm32-wasip2)
    (cd examples/stockmarket/simulator && cargo component build --release --target wasm32-wasip2)

# Run the full stockmarket example (requires Postgres + RustFS S3 — see `just dev-up`)
stockmarket: build-stockmarket build
    DB_URL={{db_url_example}} bash examples/stockmarket/run.sh

# Run the stockmarket example inline (single invocation, exits on failure)
stockmarket-inline: build-stockmarket build
    DB_URL={{db_url_example}} bash examples/stockmarket/run.sh --inline

# ── Housekeeping ──────────────────────────────────────────────────────────────

# Remove build artifacts
clean:
    cargo clean
