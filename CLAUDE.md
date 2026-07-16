# CLAUDE.md

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
just tidy                    # format + clippy -D warnings

# WASM and examples
just test-wasm
just build-ecommerce
just build-stockmarket
just build-codegen
just validate-ecommerce     # ecommerce E2E; fails on WARN/WARNING
just validate-all

# Development infrastructure and services
just certs
just dev-up
just dev-down
just manager
just proxy
just engine
```

Continuous compilation is available through `just watch [check|clippy|test|build-ecommerce|build-codegen|build-stockmarket]`.

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

**Prerequisites:** `rustc`, `cargo`, `just`, `protoc`, and `taplo`. WASM work also requires `wasm32-wasip2` and `wasm-tools`. Cross-compilation requires `zig` and `cargo-zigbuild`.

Integration helpers live in `wr-tests/tests/helpers/mod.rs`. Direct DB-backed tests use `WRT_TEST_DB_URL=postgres://postgres@localhost:5433/wruntime_test` and skip under the shared policy when it is absent. Just test recipes set required DB/S3 variables; run `just dev-up` first.

## Architecture summary

Wruntime is a Cargo workspace implementing a distributed WASI Preview 2 runtime:

| Service | Default listeners | Role |
|---|---|---|
| `wr-manager` | 9000 mTLS gRPC, 9010 gossip | Registry, routing, schemas, schedules, secrets, heartbeats |
| `wr-proxy` | 9001 loopback HTTP, 9002 loopback control, 9443 mTLS peer | Streaming header-based routing and circuit breaking |
| `wr-engine` | 9100 loopback HTTP | WASM component execution and host capabilities |

Modules use `(namespace, name, version)` identity and call `http://namespace.module/{package}.{Service}/{Method}`. The engine intercepts outbound HTTP and supplies internal routing metadata; the proxy resolves a healthy local/peer destination and streams the body; the destination engine dispatches to a module instance.

Engine startup registers unhealthy routes, provisions namespace resources, runs module migrations, resolves secrets, validates capabilities, loads components, sends an immediate readiness heartbeat, then starts periodic heartbeats. Manager migrations and module migrations follow separate policies.

Manager gRPC and peer-proxy cross-node traffic use mTLS; manager liveness uses chitchat UDP gossip on its separately configured listener. Loopback engine/proxy traffic is plain HTTP only on documented listeners. Source routing metadata is not authorization. Guest DB pools use namespace roles without access to `wr_system`.

Host interfaces are canonical under `wit/` and implemented asynchronously in `wr-engine`; guest calls remain synchronous from the guest perspective. Do not use `block_in_place` or `block_on` in host implementations.

For maintenance details, use [architecture](docs/architecture.md), [configuration](docs/configuration.md), and the [maintainer guide](docs/agents/wruntime-maintainer/README.md).
