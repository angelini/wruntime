# AGENTS.md

Guidance for repository maintenance and guest-module work.

## Commands

`just` is the task runner. Run `just` with no arguments to list recipes.

```bash
# Workspace
just build
just check
just test
just test-integration
just test-one <name>
just test-lifecycle-runners
just test-tenant-isolation-e2e
just test-worktree-dev-fixture
just tidy                    # format + clippy -D warnings

# WASM and examples
just build-wasm-guests
just clean-wasm-cache
just test-wasm
just build-ecommerce
just build-stockmarket
just build-codegen
just validate-ecommerce     # ecommerce E2E; fails on WARN/WARNING
just validate-changed        # default completion selector for current worktree changes
just validate-changed --explain # preview conservative selection without running commands
just test-validate-changed   # hermetic selector regression suite
just validate-all --no-deployment-e2e # explicit local skip
just validate-all --no-deployment-e2e --skip-dev-up --no-codegen-e2e # Pi sandbox
just validate-all --deployment-e2e    # protected runner, live Systemd node + manager qualification

# Development infrastructure and services
just certs
just dev-up
just dev-down
just manager
just proxy
just engine
```

Continuous compilation is available through `just watch [check|clippy|test|build-ecommerce|build-codegen|build-stockmarket]`.

Before completion, preview the conservative validation selection with `just validate-changed --explain`, then run `just validate-changed` with any flags required by the reported profile; pass `--base <ref>` when committed branch changes must also be included. Do not invoke `validate-all` directly unless the selector chooses a broad fallback or the validation matrix explicitly requires it for the change class. A focused selector result is the completion validation for stable homogeneous changes, including documentation-only changes, but does not replace additional change-class checks from the matrix.

In the Pi sandbox (`DOTGEN_PI_SANDBOX=1`), when the selector or validation matrix requires broad validation, run it through `just validate-changed --no-deployment-e2e --skip-dev-up --no-codegen-e2e` (or use the equivalent direct `validate-all` command only when explicitly required). Docker is unavailable, so host `just dev-up` must first prepare this worktree's deterministic `wruntime-dev-<id>` Compose project. Per-worktree state lives under `<absolute-git-dir>/wruntime-dev-state`; its published record supplies the worktree-specific PostgreSQL and RustFS ports while preserving provenance, manifest, migration, artifact, image, and PKI bindings. Linked worktrees have distinct projects, volumes, ports, state, and lifecycle locks, so DB tests and local E2Es may run concurrently across worktrees. Sandbox consumers never invoke Docker or perform fixture setup. A missing or internally invalid fixture fails before database access and requires host `just dev-up` in that same worktree. Source changes do not invalidate an already published worktree fixture. When rerun for provisioning or migration changes, `dev-up` converges the retained database through the normal migration tooling and never deletes its volumes automatically; reset stuck local data manually. `--skip-dev-up` avoids Docker startup, and `--no-codegen-e2e` skips only codegen because `ANTHROPIC_API_KEY` is not exposed. When broad validation runs, the multi-node, ecommerce, and stockmarket E2E examples must still run; do not pass `--no-e2e` or omit the rest of `validate-all`. Only protected deployment E2E remains globally serialized.

## Agent modes

[`docs/agents/README.md`](docs/agents/README.md) is a neutral dispatcher with exactly two modes:

- [Guest module author](docs/agents/guest-module-author/README.md) — consumes existing SDK/WIT contracts to build WASM guests.
- [Wruntime maintainer](docs/agents/wruntime-maintainer/README.md) — changes runtime, SDK, WIT, protobuf, CLI, tests, deployment, or repository contracts.

A task requiring changes to root `wit/`, `wr-sdk`, or `wr-build` is maintainer work even when a guest example is the downstream consumer. Follow the [maintainer workflow](docs/agents/wruntime-maintainer/README.md), [invariants](docs/agents/wruntime-maintainer/invariants.md), and [validation matrix](docs/agents/wruntime-maintainer/validation.md).

Exact guest APIs are owned by `wr-sdk/src/*.rs`, `wr-build/src/lib.rs`, and `wit/*.wit`. The [guest API guide](docs/agents/guest-module-author/api_guide.md) owns preferred usage and semantic guidance.

## Verification

After runtime refactoring, run `just tidy` and `just validate-ecommerce`. Treat any ecommerce warning as a bug. Use the change-sensitive requirements in the [validation matrix](docs/agents/wruntime-maintainer/validation.md).

Changes to host bindings (`wr-engine/src/db/`, `wr-engine/src/blobstore.rs`, `wr-engine/src/llm.rs`, `wr-engine/src/tracing.rs`), root WIT, `wr-sdk`, `wr-build`, test guests, or split `wr-tests/tests/wasm_*_host_test.rs` targets also require `just test-wasm`.

Keep documentation synchronized according to [documentation ownership](docs/agents/wruntime-maintainer/documentation_ownership.md). Guest-visible SDK/WIT/build semantics require review of the guest API guide; exact signatures remain in source.

**Prerequisites:** `rustc`, `cargo`, `just`, `protoc`, and `taplo`. WASM work also requires `wasm32-wasip2` and `wasm-tools`. Cross-compilation requires `zig`, `cargo-zigbuild`, `rustup`, and the selected target; host fixture preparation fails without mutation and prints the exact `rustup target add <target>` action when it is missing. Live deployment E2E also requires `uv`.

Integration helpers live in `wr-tests/tests/helpers/mod.rs`. Direct DB-backed tests use `WRT_TEST_DB_URL` and skip under the shared policy when it is absent. Just test recipes verify this worktree's fixture and export its published DB/S3 endpoints; run host `just dev-up` from the same worktree first.

## Architecture summary

Wruntime is a Cargo workspace implementing a distributed WASI Preview 2 runtime:

| Service | Default listeners | Role |
| --- | --- | --- |
| `wr-manager` | 9000 unified mTLS gRPC | Registry, routing, policy, infrastructure, lifecycle, jobs, schemas, schedules, secrets, and PostgreSQL lease heartbeats |
| `wr-proxy` | 9001 loopback HTTP, 9002 loopback control, 9443 mTLS peer | Streaming header-based routing and circuit breaking |
| `wr-engine` | 9100 loopback HTTP + configured manager-only job-admin mTLS | WASM component execution and host capabilities |
| `wr-cli node agent` | no listener | Node-bound fenced Systemd lifecycle executor |

Modules use `(namespace, name, version)` identity and call `http://namespace.module/{package}.{Service}/{Method}`. The engine intercepts outbound HTTP and supplies internal routing metadata; the proxy resolves a healthy local/peer destination and streams the body; the destination engine dispatches to a module instance.

Engine startup registers unhealthy routes, reconciles password-free manager identities with node-local tenant expectations, verifies the offline provision/migration receipt through bounded native-certificate logins, applies embedded job-queue migrations on the separate platform database, builds namespace pools, starts engine-level recovery when needed, resolves secrets, validates capabilities, loads components, sends an immediate readiness heartbeat, then starts periodic heartbeats. Manager, engine queue, and offline module migrations follow separate policies.

Destructive operator lifecycle uses fingerprint-mapped manager mTLS roles, durable operations and append-only events, and a pull-based node-bound agent with fenced lease epochs. The agent executes only typed per-slot Systemd effects; source/proxy headers are never authorization. Committed and staged revisions may overlap with explicit per-slot route authority.

Managers expose one mTLS gRPC listener. Distinct server/client roots, URI-SAN principals, leaf-fingerprint revocation, and immutable default-deny per-RPC policy authorize human and workload calls; the manager uses a separate clientAuth workload leaf for engine job administration. Peer-proxy traffic remains a separate mTLS protocol boundary, and manager liveness uses the shared PostgreSQL lease with server-side freshness. Loopback engine/proxy traffic is plain HTTP only on documented listeners. Source routing metadata is not authorization. Guest DB pools use namespace roles without access to `wr__jobs` or `wr_system`; module schemas remain admin-owned.

Host interfaces are canonical under `wit/` and implemented asynchronously in `wr-engine`; guest calls remain synchronous from the guest perspective. Do not use `block_in_place` or `block_on` in host implementations.

For maintenance details, use [architecture](docs/architecture.md), [configuration](docs/configuration.md), and the [maintainer guide](docs/agents/wruntime-maintainer/README.md).
