# Testing

Maintainers should select checks by change class in the [validation matrix](agents/wruntime-maintainer/validation.md). This page documents command behavior and prerequisites.

Common recipes:

```bash
just dev-up                  # start Postgres, Grafana/LGTM, and RustFS S3
just test                    # all tests with test DB/S3 env vars set
just test-integration        # wr-tests crate only
just test-one <test_name>    # single test by name
just test-wasm               # build WASM guests, then run host binding tests
just validate-ecommerce      # ecommerce inline run, failing on WARN/WARNING output
just validate-all            # full format/lint/WASM/test/E2E suite
just dev-down                # stop dev infrastructure
```

`just test`, `just test-integration`, `just test-one`, and `just test-wasm`
set the `WRT_TEST_DB_URL` and `WRT_TEST_S3_*` variables expected by
integration tests. Run `just dev-up` first when using those full recipes.

Direct `cargo test -p wr-tests` runs are allowed for quick local checks.
DB-backed tests use `WRT_TEST_DB_URL` and skip through the shared helper policy
when it is absent. S3-backed tests use `WRT_TEST_S3_ENDPOINT`,
`WRT_TEST_S3_ACCESS_KEY`, and `WRT_TEST_S3_SECRET_KEY`; direct S3-backed cargo
tests require those variables because the current blobstore helper expects
them. Required WASM artifacts must be built before direct WASM host binding
test runs. The LLM guest protocol uses protobuf enums for stop reasons, stream
events, and error kinds, while the DB guest protocol uses `oneof` parameter and
column values rather than JSON strings. Positive-path tests can use `RpcPath`
and `GuestHarness::dispatch_typed`; raw request helpers remain available for
malformed-input coverage.

WASM host binding tests require:

- `rustup target add wasm32-wasip2`
- `protoc`
- `wasm-tools`
- Postgres and RustFS from `just dev-up`

Example inline scripts require the built workspace binaries, the same dev
infrastructure, and Python 3 for small JSON/config rendering helpers. They
create per-run temporary config directories and call
`wr-cli dev --state-dir <run-dir>/dev-state`, so cleanup only observes that
run's PID state. The codegen example uses `wr-cli invoke --json` and Python
stdlib JSON parsing; no `jq` dependency is required.

`just validate-all` is a thin alias for `dev/validate-all.sh`. The script
orchestrates existing Just recipes for formatting, compile checks, lints, WASM
guest builds, Rust tests, and fixed-port E2E examples. E2E examples run
sequentially because they share ports and example resources. Logs and
`summary.txt` are written under `target/validate-all/<timestamp>/`; terminal
failure output is capped for agent-friendly context use. Codegen E2E runs only
when `ANTHROPIC_API_KEY` is set by default; use `--codegen-e2e` to require it
or `--no-codegen-e2e` to always skip it.

## Dev infrastructure

Docker Compose provides Postgres, Grafana/LGTM, and RustFS S3:

```bash
just dev-up                  # start all dev services
just dev-down                # stop all dev services
just dev-logs                # tail logs from all services
just dev-logs postgres       # tail logs from a single service
just dev-ps                  # show running container status
just dev-reset-db            # drop module schemas, manager tables, migrations
```
