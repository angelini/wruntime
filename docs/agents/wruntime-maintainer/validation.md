# Validation Matrix

Start focused, then run the broader requirement for the change class.

| Change class | Focused validation |
|---|---|
| Docs only | `git diff --check`, `just fmt-check`, manual link/navigation review |
| Workspace Rust | `just check` plus the owning crate or named test |
| Proxy routing/version/circuit breaker | relevant `proxy_test`, `version_test`, `concurrent_routing_test`, `cross_node_test`, and `circuit_breaker_test` targets |
| Manager lifecycle/readiness/clustering | relevant `manager_test`, `health_test`, and `multi_manager_test` targets |
| Worker/scheduler/schedules | relevant `worker_test`, `scheduler_test`, and `schedules_test` targets |
| WIT, SDK, build generator, or host binding | `just test-wasm-one <target>`, then `just test-wasm` |
| Guest example | `just build-<example>`, guest format/lint, then its inline recipe |
| Migration | migration tests plus tests for the owning manager or engine/module subsystem |
| Deployment generator | relevant CLI/config/bundle tests and deterministic output review |
| Broad pre-merge | `just validate-all` |

## Environment and command policy

- Run `just dev-up` before recipes that require Postgres, RustFS S3, or LGTM. `just test`, `just test-integration`, `just test-one`, and `just test-wasm` set the repository test environment variables but do not replace the services.
- Direct `cargo test` is useful for fast pure tests. DB-backed tests skip under the shared helper policy when `WRT_TEST_DB_URL` is absent; direct S3-backed tests require `WRT_TEST_S3_*` variables.
- WASM tests require `wasm32-wasip2`, `protoc`, `wasm-tools`, and built guest artifacts. Prefer Just recipes because they build fixtures and set environment variables.
- Fixed-port E2E examples share ports and resources. Run them serially, not concurrently.
- `just validate-ecommerce` is the warning-enforcing ecommerce command; any `WARN` or `WARNING` line fails it.
- `just validate-all` runs formatting, compile checks, lints, WASM builds/tests, Rust tests, and serial E2E examples. Codegen E2E is optional when `ANTHROPIC_API_KEY` is absent; pass `--codegen-e2e` to require it or `--no-codegen-e2e` to skip it.

For command details and prerequisites, use [`docs/testing.md`](../../testing.md). When a check cannot run, report the exact command, reason, and remaining risk.
