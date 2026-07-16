# Repository Map

Use this task map to start from the contract owner, then follow callers and tests. “Minimum validation” is additive to the broader matrix in [validation.md](validation.md).

| Area | Contract owner and source entry points | Focused tests | Existing docs | Minimum validation | Downstream docs |
|---|---|---|---|---|---|
| Manager registry/readiness | `wr-manager/src/{main,service,db,state}.rs` | `manager_test.rs`, `health_test.rs`, `namespace_test.rs` | architecture, configuration, gRPC | `just check`; relevant `just test-one` filters | architecture, configuration, gRPC |
| Manager clustering | `wr-manager/src/cluster.rs`, manager discovery/state code | `multi_manager_test.rs`, `cross_node_test.rs` | architecture, configuration | clustering tests | architecture, configuration, deployment |
| Proxy routing | `wr-proxy/src/layers/{routing,forward,ingress,egress}.rs`, `routing.rs`, `indexed_routing.rs`, `circuit_breaker.rs` | `proxy_test.rs`, `version_test.rs`, `concurrent_routing_test.rs`, `circuit_breaker_test.rs`, `ingress_test.rs`, `egress_test.rs`, `cross_node_test.rs` | architecture, configuration, schemas | routing/version/circuit tests | architecture, configuration, schemas |
| Engine runtime | `wr-engine/src/{main,engine,runtime,registry,server,state,pool}.rs` | `health_test.rs`, `wasm_*_host_test.rs`, engine-backed integration tests | architecture, configuration, host bindings | `just check`; relevant integration test | architecture, configuration, host bindings |
| Host capabilities | `wr-engine/src/db/`, `blobstore.rs`, `llm.rs`, `tracing.rs`, `state.rs`; canonical root `wit/` | split `wasm_db`, `wasm_blobstore`, `wasm_llm`, `wasm_tracing` host tests | host bindings, configuration | focused `just test-wasm-one`, then `just test-wasm` | host bindings, guest API/constraints |
| Workers and schedules | `wr-engine/src/worker.rs`, `wr-manager/src/scheduler.rs`, `wr-sdk/src/jobs.rs`, control-plane proto | `worker_test.rs`, `scheduler_test.rs`, `schedules_test.rs` | architecture, configuration, gRPC | worker/scheduler/schedule tests | architecture, configuration, gRPC, guest codegen/API |
| Shared contracts | `wr-common/src/`, `proto/wruntime.proto`, shared headers/identity/TLS types | `namespace_test.rs`, `config_test.rs`, routing and cross-node tests | architecture, gRPC, configuration | `just check`; affected integration tests | architecture, gRPC, configuration |
| SDK and codegen | `wr-sdk/src/*.rs`, `wr-build/src/lib.rs`, `wr-sdk/wit/` | crate unit tests, split WASM host tests, executable examples | SDK, host bindings, guest guides | `just test-wasm`; relevant example | guest API, codegen, template, SDK |
| CLI and deployment | `wr-cli/src/cmd/`, especially `deploy_config.rs`, `bundle.rs`, `managers.rs`, `services.rs`, `dev.rs` | CLI unit/integration and config tests near owning commands | deployment, configuration, gRPC | owning tests; deterministic output review | deployment, configuration, README |
| Manager migrations | `wr-manager/migrations/`, `wr-manager/src/migrate.rs` | `migration_test.rs`, manager startup tests | architecture, configuration | migration and manager tests | configuration, architecture |
| Module migrations | `wr-engine/src/migration.rs`, guest `migrations/`, engine module config | `migration_test.rs`, affected example E2E | configuration, guest template/constraints | migration tests; affected inline example | configuration, guest template/constraints |
| Test harness | `wr-tests/tests/helpers/mod.rs` and helper submodules; `wr-tests/guests/` protocol fixtures | the affected integration and split WASM target | testing | build fixture, then affected test | testing, maintainer validation |
| Executable examples | `examples/ecommerce/`, `examples/stockmarket/`, `examples/codegen/`, `examples/multi-node/` | build recipes and inline E2E scripts | README, configuration, guest examples | relevant build plus inline recipe | README, guest examples, configuration |

Paths are repository-relative. Test files are under `wr-tests/tests/`; use `just test-one <filter>` for named tests and direct `cargo test -p wr-tests --test <target>` when selecting an integration-test binary.
