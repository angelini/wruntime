# Validation Matrix

Start focused, then run the broader requirement for the change class.

| Change class | Focused validation |
| --- | --- |
| Docs only | `git diff --check`, `just fmt-check`, manual link/navigation review |
| Workspace Rust | `just check` plus the owning crate or named test |
| Proxy routing/version/circuit breaker | relevant `proxy_test`, `version_test`, `concurrent_routing_test`, `cross_node_test`, and `circuit_breaker_test` targets |
| Manager lifecycle/readiness/clustering | relevant `manager_test`, `health_test`, and `multi_manager_test` targets |
| Worker/scheduler/schedules | relevant `worker_test`, `scheduler_test`, and `schedules_test` targets |
| WIT, SDK, build generator, or host binding | `just test-wasm-one <target>`, then `just test-wasm` |
| Guest example | `just build-<example>`, guest format/lint, then its inline recipe |
| Migration | migration tests plus tests for the owning manager or engine/module subsystem |
| Engine job queue/database topology | `job_migration_test`, startup manifest unit tests, worker/namespace/provisioning tests; manual ignored `db_simplification_metrics` only for evidence claims |
| Local foreground runner or process lifecycle | `cargo test -p wr-common lifecycle`; `cargo test -p wr-cli`; `cargo test -p wr-proxy`; `just test-lifecycle-runners`; relevant lifecycle/proxy/version integration targets; `just test-wasm`; serial `just multi-node-inline`, `just validate-ecommerce`, and `just stockmarket-inline`; static socket/lock/state and leftover-child audit |
| Deployment generator, lifecycle, or backend stop | relevant CLI/config/bundle/node-stop tests and deterministic output review; `just deployment-e2e-python-test`; `just validate-all --deployment-e2e` on the protected runner |
| Broad pre-merge | `just validate-all --deployment-e2e` on the protected runner, or an explicit reported `--no-deployment-e2e` skip when the change does not affect deployment |

## Environment and command policy

- Run `just dev-up` before recipes that require Postgres, RustFS S3, or LGTM. `just test`, `just test-integration`, `just test-one`, and `just test-wasm` set the repository test environment variables but do not replace the services.
- In the Pi sandbox (`DOTGEN_PI_SANDBOX=1`), run `just validate-all --no-deployment-e2e --skip-dev-up --no-codegen-e2e`. Docker is unavailable there, but the existing development services are exposed. Only `ANTHROPIC_API_KEY` is unavailable for the local examples, so skip codegen explicitly while still running the multi-node, ecommerce, and stockmarket E2E examples. Do not pass `--no-e2e`.
- Direct `cargo test` is useful for fast pure tests. DB-backed tests skip under the shared helper policy when `WRT_TEST_DB_URL` is absent; direct S3-backed tests require `WRT_TEST_S3_*` variables.
- WASM tests require `wasm32-wasip2`, `protoc`, `wasm-tools`, and built guest artifacts. Prefer Just recipes because they build fixtures and set environment variables.
- Fixed-port E2E examples share ports and resources. Their runners enforce a non-blocking per-user host lock; run them serially, including across worktrees.
- `just validate-ecommerce` is the warning-enforcing ecommerce command; any `WARN` or `WARNING` line fails it.
- `just validate-all` requires exactly one deployment lifecycle choice. `--deployment-e2e` requires both live systemd and Docker passes; `--no-deployment-e2e` records separate explicit skips. The live stage runs before fixed-port local examples. Codegen E2E is optional when `ANTHROPIC_API_KEY` is absent; pass `--codegen-e2e` to require it or `--no-codegen-e2e` to skip it.
- Changes to deployment generation, bundle integrity, remote lifecycle behavior, provider reset logic, deployment fixtures, or lifecycle assertions require `just validate-all --deployment-e2e` on the protected Proxmox runner. A local explicit skip is not completion evidence for those changes.
- Foreground-runner checks must leave no owned service or scenario descendant and must create no state socket, lock, or persistent state. Exit proof belongs to the runner's retained `Child` handles locally and to systemd/Docker inspection for deployed services; lifecycle status alone is not process-exit proof. Remaining sleeps/polls in touched runners must be identified as bounded application, infrastructure, or provider waits; service readiness and topology routing convergence may not use them.
- A lifecycle migration touching the runner, service task ownership, examples, or deployment stop contract requires the focused runner and integration suites, `just test-wasm`, serial multi-node/ecommerce/stockmarket examples, and `just validate-all --deployment-e2e` on the protected runner. Missing codegen credentials or protected-runner access remains explicitly unmet evidence, not a substitute skip that proves completion.

For command details and prerequisites, use [`docs/testing.md`](../../testing.md). When a check cannot run, report the exact command, reason, and remaining risk.
