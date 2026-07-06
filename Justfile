# wruntime Justfile — common development tasks

export RUST_BACKTRACE := "1"

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

# ── Certificates ──────────────────────────────────────────────────────────────

# Generate local CA + localhost certs for development
certs:
    just cli cert init-ca --output certs/
    just cli cert generate 127.0.0.1 --ca-dir certs/
    just cli cert generate manager --ca-dir certs/

# ── Lint & Format ─────────────────────────────────────────────────────────────

guest_crates := "examples/ecommerce/client examples/ecommerce/inventory examples/codegen/agent examples/codegen/collector examples/codegen/coordinator examples/codegen/worker examples/stockmarket/exchange examples/stockmarket/ledger examples/stockmarket/simulator"

# Format workspace source code
fmt:
    cargo fmt --all
    taplo fmt

# Format example guest crates
fmt-examples:
    for d in {{guest_crates}}; do (cd "$d" && cargo fmt); done

# Run Clippy lints across the workspace
lint:
    cargo clippy --all-targets --all-features -- -D warnings

# Run Clippy lints across example guest crates
lint-examples:
    for d in {{guest_crates}}; do (cd "$d" && cargo clippy --target wasm32-wasip2 -- -D warnings); done

# Format and lint workspace
tidy: fmt lint

# Format and lint example guests
tidy-examples: fmt-examples lint-examples

# ── Test ──────────────────────────────────────────────────────────────────────

# Run all tests
test:
    WRT_TEST_DB_URL={{db_url_test}} \
    WRT_TEST_S3_ENDPOINT={{s3_endpoint}} \
    WRT_TEST_S3_ACCESS_KEY={{s3_access_key}} \
    WRT_TEST_S3_SECRET_KEY={{s3_secret_key}} \
    cargo test --timings

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

# Start node B proxy (listens :9003, control :9004, proxy_address = "http://127.0.0.1:9003")
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

# Reset example DB — drops module schemas, manager schema, and migration history
dev-reset-db:
    @echo "==> Resetting example database..."
    psql "{{db_url_example}}" -c " \
        DO \$\$DECLARE r RECORD; \
        BEGIN \
            FOR r IN SELECT schema_name FROM information_schema.schemata \
                     WHERE schema_name LIKE 'wr__%' \
            LOOP \
                EXECUTE 'DROP SCHEMA \"' || r.schema_name || '\" CASCADE'; \
                RAISE NOTICE 'dropped schema %', r.schema_name; \
            END LOOP; \
            DROP SCHEMA IF EXISTS wr_system CASCADE; \
            DROP TABLE IF EXISTS refinery_schema_history CASCADE; \
            FOR r IN SELECT tablename FROM pg_tables \
                     WHERE schemaname = 'public' AND tablename LIKE 'wr_%' \
            LOOP \
                EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE'; \
                RAISE NOTICE 'dropped table %', r.tablename; \
            END LOOP; \
        END\$\$; \
    "
    @echo "Done."

# Clear all objects from the codegen S3 bucket
dev-reset-blobstore bucket="codegen":
    AWS_ACCESS_KEY_ID={{s3_access_key}} AWS_SECRET_ACCESS_KEY={{s3_secret_key}} \
        aws --endpoint-url {{s3_endpoint}} s3 rm s3://{{bucket}} --recursive
    @echo "Cleared s3://{{bucket}}"

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
    (cd wr-tests/guests/db-guest && cargo build --target wasm32-wasip2)
    (cd wr-tests/guests/tracing-guest && cargo build --target wasm32-wasip2)
    (cd wr-tests/guests/blobstore-guest && cargo build --target wasm32-wasip2)
    (cd wr-tests/guests/http-guest && cargo build --target wasm32-wasip2)
    (cd wr-tests/guests/llm-guest && cargo build --target wasm32-wasip2)

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

# Run hot-path benchmarks (WASM→proxy→WASM). Override iterations/concurrency via env vars.
bench: build-test-guests
    WRT_TEST_DB_URL={{db_url_test}} \
    BENCH_ITERATIONS=5000 \
    BENCH_WARMUP=30 \
    BENCH_CONCURRENCY=20 \
    cargo test -p wr-tests --test bench_test --release -- --nocapture

# ── Ecommerce Example ─────────────────────────────────────────────────────────

# Compile ecommerce protobuf schemas to FileDescriptorSet binaries (.binpb)
build-ecommerce-schemas:
    protoc --descriptor_set_out=examples/ecommerce/schemas/inventory.binpb \
           --include_imports \
           examples/ecommerce/schemas/inventory.proto
    protoc --descriptor_set_out=examples/ecommerce/schemas/client.binpb \
           --include_imports \
           examples/ecommerce/schemas/client.proto

# Build WASM components and schemas for the ecommerce example
build-ecommerce: build-ecommerce-schemas
    (cd examples/ecommerce/inventory && cargo build --target wasm32-wasip2)
    (cd examples/ecommerce/client && cargo build --target wasm32-wasip2)

# Run the full ecommerce example (requires Postgres — see `just dev-up`)
ecommerce: build-ecommerce build
    WRT_SECRET_ENCRYPTION_KEY="0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef" \
    DB_URL={{db_url_example}} bash examples/ecommerce/run.sh

# Run the ecommerce example inline (single invocation, exits on failure)
ecommerce-inline: build-ecommerce build
    WRT_SECRET_ENCRYPTION_KEY="0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef" \
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
    (cd examples/stockmarket/exchange && cargo build --target wasm32-wasip2)
    (cd examples/stockmarket/ledger && cargo build --target wasm32-wasip2)
    (cd examples/stockmarket/simulator && cargo build --target wasm32-wasip2)

# Run the full stockmarket example (requires Postgres + RustFS S3 — see `just dev-up`)
# Pass exchanges=N to run N exchange engines in parallel (default: 1)
stockmarket exchanges="1": build-stockmarket build
    WRT_SECRET_ENCRYPTION_KEY="0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef" \
    DB_URL={{db_url_example}} bash examples/stockmarket/run.sh --exchanges {{exchanges}}

# Run the stockmarket example inline (single invocation, exits on failure)
stockmarket-inline exchanges="1": build-stockmarket build
    WRT_SECRET_ENCRYPTION_KEY="0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef" \
    DB_URL={{db_url_example}} bash examples/stockmarket/run.sh --inline --exchanges {{exchanges}}

# ── Codegen Example ───────────────────────────────────────────────────────────

# Compile codegen protobuf schemas to FileDescriptorSet binaries (.binpb)
build-codegen-schemas:
    protoc --descriptor_set_out=examples/codegen/schemas/collector.binpb \
           --include_imports \
           examples/codegen/schemas/collector.proto
    protoc --descriptor_set_out=examples/codegen/schemas/agent.binpb \
           --include_imports \
           examples/codegen/schemas/agent.proto
    protoc --descriptor_set_out=examples/codegen/schemas/coordinator.binpb \
           --include_imports \
           examples/codegen/schemas/coordinator.proto
    protoc --descriptor_set_out=examples/codegen/schemas/worker.binpb \
           --include_imports \
           --proto_path=examples/codegen/schemas \
           examples/codegen/schemas/worker.proto

# Build WASM components and schemas for the codegen example
build-codegen: build-codegen-schemas
    (cd examples/codegen/collector && cargo build --target wasm32-wasip2)
    (cd examples/codegen/agent && cargo build --target wasm32-wasip2)
    (cd examples/codegen/coordinator && cargo build --target wasm32-wasip2)
    (cd examples/codegen/worker && cargo build --target wasm32-wasip2)

# Run the full codegen example (requires Postgres + RustFS S3 — see `just dev-up`)
codegen: build-codegen build
    WRT_SECRET_ENCRYPTION_KEY="0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef" \
    DB_URL={{db_url_example}} bash examples/codegen/run.sh

# Run the codegen example inline (single invocation, exits on failure)
codegen-inline: build-codegen build
    WRT_SECRET_ENCRYPTION_KEY="0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef" \
    DB_URL={{db_url_example}} bash examples/codegen/run.sh --inline

# ── Dev Workflow ──────────────────────────────────────────────────────────────

# Run the CLI, passing all arguments through
[positional-arguments]
cli *args:
    cargo run --bin wr-cli -- "$@"

# Run bacon (continuous compilation on file save)
# Jobs: check, clippy, test, build, build-ecommerce, build-codegen, build-stockmarket
watch job="build":
    bacon {{job}}

# ── Housekeeping ──────────────────────────────────────────────────────────────

# Remove build artifacts
clean:
    cargo clean
