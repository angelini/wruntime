# Design Narrative

> **Non-authoritative:** This document explains design intent with stable pseudocode. Rust source and tests own runtime behavior; protobuf/WIT own wire and host contracts. See [documentation ownership](agents/wruntime-maintainer/documentation_ownership.md).

## Transparent module networking

A guest calls a logical module authority rather than an engine address:

```text
http://ecommerce.inventory/ecommerce.InventoryService/GetStock
```

Conceptually, the engine intercepts outbound WASI HTTP, adds trusted routing and trace metadata, and sends the request to its loopback proxy. The proxy selects a healthy local engine or peer proxy and streams the request. The destination engine dispatches by module identity.

The stable implementation entry points are:

- engine interception/runtime: [`wr-engine/src/runtime.rs`](../wr-engine/src/runtime.rs) and [`wr-engine/src/state.rs`](../wr-engine/src/state.rs);
- proxy branch selection: [`wr-proxy/src/layers/routing.rs`](../wr-proxy/src/layers/routing.rs);
- indexed version lookup: [`wr-proxy/src/indexed_routing.rs`](../wr-proxy/src/indexed_routing.rs);
- forwarding/circuit behavior: [`wr-proxy/src/layers/forward.rs`](../wr-proxy/src/layers/forward.rs) and [`wr-proxy/src/circuit_breaker.rs`](../wr-proxy/src/circuit_breaker.rs).

No fixed routing refresh interval is part of this narrative. Discovery and synchronization behavior is owned by current source/configuration.

## Routing branches and trust

The proxy keeps four boundaries explicit:

1. public ingress maps an external route to an internal destination;
2. internal module traffic chooses a local engine or peer proxy;
3. external egress is checked against policy;
4. cross-node forwarding uses mTLS.

Public ingress strips reserved `x-wr-*` headers before setting trusted values. Source metadata supports routing and observability; it is not authorization. See [architecture](architecture.md) for the public contract and [runtime invariants](agents/wruntime-maintainer/invariants.md) for change review.

Version selection distinguishes exact, semver-range, and unpinned requests. Circuit breakers influence candidate choice without changing the identity/version contract. Tests in `wr-tests/tests/{version,circuit_breaker,proxy,cross_node}_test.rs` are the executable specification.

## Registration and readiness

Engine startup follows this conceptual sequence:

```text
register engine/modules (routes initially unhealthy)
  → provision namespace DB roles/schemas and worker storage
  → run module migrations
  → build guest pools
  → resolve requested secrets to module environment values
  → validate component imports against capability flags
  → load components
  → send immediate readiness heartbeat
  → run periodic heartbeats
```

This ordering prevents traffic from reaching a module before migrations, secrets, capability checks, and component load complete. Current lifecycle code is in [`wr-engine/src/main.rs`](../wr-engine/src/main.rs); public behavior is described in [configuration](configuration.md).

## Host capabilities

Root [`wit/`](../wit/) is the canonical ABI:

- [`db.wit`](../wit/db.wit) — queries, transactions, and row cursors;
- [`blobstore.wit`](../wit/blobstore.wit) — bounded S3-compatible operations;
- [`tracing.wit`](../wit/tracing.wit) — typed span attributes and events;
- [`llm.wit`](../wit/llm.wit) — completions and typed stream events.

Host implementations live in [`wr-engine/src/db/`](../wr-engine/src/db/), [`blobstore.rs`](../wr-engine/src/blobstore.rs), [`tracing.rs`](../wr-engine/src/tracing.rs), and [`llm.rs`](../wr-engine/src/llm.rs). They are asynchronous internally; calls remain synchronous from the guest's perspective.

A guest still generates component metadata for its own local world. `wr_sdk::bindings` supplies compatible convenience types but does not replace that world. Capability imports must match the guest's module flags or startup fails.

### Database isolation

The manager provisions administrative state and namespace roles. Engines run module migrations under schema-scoped policy and build guest pools with namespace credentials. Guest roles cannot read `wr_system`. Transactions roll back on drop unless committed; cursor/resource drops release host resources.

### Secrets and LLM credentials

Module secret references are resolved at registration/startup and passed only as environment values. Manager list APIs expose metadata, not plaintext. LLM provider credentials stay on the host and are not guest environment values.

### LLM streams

The semantic stream is:

```text
zero or more text deltas → one usage event → one stop event → end
```

Tool use is non-streaming only. Exact event types belong to [`wit/llm.wit`](../wit/llm.wit); preferred use belongs to the [guest API guide](agents/guest-module-author/api_guide.md#llm).

## Workers and schedules

Workers are HTTP service implementations consumed through an engine-managed durable queue. The stable flow is:

```text
submit canonical job type
  → persist pending job
  → worker claims under lease/fence
  → execute handler
  → fenced complete or retry/dead transition
```

A non-empty ad-hoc version matches exactly. An empty ad-hoc version is name-only and may be claimed by any matching worker version. Manager schedules are version-pinned. Leases and retries make delivery at least once, so handlers must be idempotent.

Current sources are [`wr-engine/src/worker.rs`](../wr-engine/src/worker.rs), [`wr-manager/src/scheduler.rs`](../wr-manager/src/scheduler.rs), [`wr-sdk/src/jobs.rs`](../wr-sdk/src/jobs.rs), and the worker generator in [`wr-build/src/lib.rs`](../wr-build/src/lib.rs). Avoid copying a `claim_job` signature into narrative documentation; source owns it.

## Guest lifecycle and generated routing

`ServiceGuest` provides `init`, `handle`, and `health_check`. `init` runs once before the first request. The SDK intercepts `GET /__health` and maps health to 200/503. Exact lifecycle/export behavior is in [`wr-sdk/src/lib.rs`](../wr-sdk/src/lib.rs).

`WrServiceGenerator` emits a service trait, `_router`, and `_handle`. Ordinary and worker clients are separate generators and can be composed. See the [guest template](agents/guest-module-author/module_template.md), [codegen guide](agents/guest-module-author/codegen.md), and live examples rather than treating snippets here as scaffolds.

## Executable examples

- [Ecommerce](../examples/ecommerce/) demonstrates a DB-backed service, generated client, migrations, load balancing, and tracing.
- [Stockmarket](../examples/stockmarket/) demonstrates multiple services/clients, persistence, and configurable exchange replicas.
- [Codegen](../examples/codegen/) demonstrates a durable worker, generated worker client, LLM, DB, blobstore, outbound HTTP, and ephemeral filesystem.
- [Multi-node](../examples/multi-node/) demonstrates placement and peer-proxy configuration.

The codegen [worker](../examples/codegen/worker/src/lib.rs), [coordinator](../examples/codegen/coordinator/src/lib.rs), [agent](../examples/codegen/agent/src/lib.rs), and [collector](../examples/codegen/collector/src/lib.rs) are the live source for those capability combinations.

## Integration harness

Integration tests spin up services in-process with helpers under [`wr-tests/tests/helpers/mod.rs`](../wr-tests/tests/helpers/mod.rs). Split WASM tests cover each host capability, while `wr-tests/guests/` contains protocol fixtures rather than production scaffolds.

See [testing](testing.md) and the [maintainer validation matrix](agents/wruntime-maintainer/validation.md) for command and prerequisite selection.
