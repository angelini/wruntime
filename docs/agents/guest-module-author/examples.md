# Worked Examples

Use executable examples to study integrated patterns, then apply the documented production constraints. Paths link to current source; read each guest's manifest, build script, WIT world, source, schema, config, and migrations together.

## Service and client patterns

| Use case | Source | Supporting files |
|---|---|---|
| DB-backed protobuf service with transactions/tracing | [ecommerce inventory](../../../examples/ecommerce/inventory/src/lib.rs) | [build.rs](../../../examples/ecommerce/inventory/build.rs), [schema](../../../examples/ecommerce/schemas/inventory.proto), [migration](../../../examples/ecommerce/inventory/migrations/V1__create_tables.sql), [engine config](../../../examples/ecommerce/engine-inventory-1.toml) |
| HTTP-triggered generated client | [ecommerce client](../../../examples/ecommerce/client/src/lib.rs) | [build.rs](../../../examples/ecommerce/client/build.rs), [world](../../../examples/ecommerce/client/wit/world.wit), [engine config](../../../examples/ecommerce/engine-client.toml) |
| Service that calls another service | [stockmarket exchange](../../../examples/stockmarket/exchange/src/lib.rs) | [build.rs](../../../examples/stockmarket/exchange/build.rs), [schemas](../../../examples/stockmarket/schemas/), [migration](../../../examples/stockmarket/exchange/migrations/V1__create_tables.sql) |
| Client/simulator orchestration | [stockmarket simulator](../../../examples/stockmarket/simulator/src/lib.rs) | [build.rs](../../../examples/stockmarket/simulator/build.rs), [engine config](../../../examples/stockmarket/engine-simulator.toml) |
| Service router plus manual JSON ingress | [codegen coordinator](../../../examples/codegen/coordinator/src/lib.rs) | [nested generator](../../../examples/codegen/coordinator/build.rs), [migration](../../../examples/codegen/coordinator/migrations/V1__create_tables.sql) |

Run scripts: [ecommerce](../../../examples/ecommerce/run.sh), [stockmarket](../../../examples/stockmarket/run.sh), and [codegen](../../../examples/codegen/run.sh).

## Workers and capabilities

| Use case | Source | Key configuration/schema |
|---|---|---|
| Worker implementation and generated dispatch structure; add transactional idempotency before production | [codegen worker](../../../examples/codegen/worker/src/lib.rs) | [worker schema](../../../examples/codegen/schemas/worker.proto), [`mode = "worker"` config](../../../examples/codegen/engine.toml) |
| Generated worker client and nested generator composition | [codegen coordinator](../../../examples/codegen/coordinator/src/lib.rs) | [coordinator build.rs](../../../examples/codegen/coordinator/build.rs) |
| LLM, DB, blobstore, filesystem, typed tracing | [codegen agent](../../../examples/codegen/agent/src/lib.rs) | [world](../../../examples/codegen/agent/wit/world.wit), [migrations](../../../examples/codegen/agent/migrations/), [engine config](../../../examples/codegen/engine.toml) |
| Outbound HTTP, blobstore, filesystem | [codegen collector](../../../examples/codegen/collector/src/lib.rs) | [world](../../../examples/codegen/collector/wit/world.wit), [engine config](../../../examples/codegen/engine.toml) |
| Multi-node placement/config only | [multi-node configs](../../../examples/multi-node/) | [public deployment guide](../../deployment.md) |

## Protocol fixtures, not scaffolds

[`wr-tests/guests/`](../../../wr-tests/guests/) contains narrow host-binding protocol and negative-test fixtures for DB, blobstore, HTTP, tracing, and LLM. Use them to understand edge cases and test wire contracts, not as production module templates. Start new modules from [module_template.md](module_template.md).
