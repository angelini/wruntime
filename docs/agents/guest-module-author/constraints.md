# Constraints and Gotchas

Read these rules before writing a guest. Exact APIs live in Rust/WIT source; see the [API guide](api_guide.md).

## Build and component rules

1. Use `[lib] crate-type = ["cdylib"]` and target `wasm32-wasip2`.
2. Add a bare `[workspace]` to standalone example/guest manifests so they do not inherit the repository workspace accidentally.
3. Keep `prost` and `prost-build` on the same tested minor version. Current guest manifests use `0.14`; current `wit-bindgen`/runtime pins are in the [module template](./module_template.md).
4. Every proto service needs a non-empty package. Prost writes `{package}.rs` to `OUT_DIR`.
5. Never edit `OUT_DIR` Rust. Regenerate checked-in `.binpb` descriptors with `--include_imports` after proto changes.
6. Every guest needs a local `wit_bindgen::generate!` block for its world/component metadata. Keep `wit/deps` synchronized with `wr-sdk/wit/deps`.

## Dispatch and capability rules

- Canonical RPC and worker job paths are `/{package}.{Service}/{Method}`. Generated clients use `namespace.module` as the authority.
- Use `WrServiceGenerator` for implemented services, `WrClientGenerator` for ordinary clients, and `WrWorkerClientGenerator` for `*WorkerService` clients. Runner/client generators do not create a guest entry point by themselves.
- Importing DB, blobstore, or LLM without the matching module opt-in fails startup validation before readiness. It is not a delayed guest panic. The host still enforces capability scope and limits on every call.
- Blobstore requires a non-empty engine bucket allowlist. LLM requires supported engine provider configuration. Filesystem access exists only with `fs = "tempdir"` and is ephemeral.
- Secret references use `[module.env] KEY = { secret = true }`. Guests receive the resolved environment value only; missing secrets fail registration/startup.

## Guest execution model

- Host calls are synchronous from the guest perspective. This does not imply that host implementations are synchronous.
- `ServiceGuest::init` runs once before first request. Health requests are intercepted by the SDK; override `health_check()` only for meaningful guest health.
- Guests are single-threaded and cannot spawn processes or open raw `std::net` sockets. Use generated clients or WASI/SDK HTTP. `std::fs` requires the tempdir capability.
- Outbound HTTP is intercepted for module routing. External hosts must match the proxy's `[egress].allowed_domains` policy. Request bodies and capability resources remain host-limited.

## Data and resource semantics

- Put schema changes in versioned module migrations. Do not use `CREATE TABLE IF NOT EXISTS` in guest handlers.
- Database inputs are converted strictly. Prefer typed SDK wrappers and row extraction.
- Dropping an uncommitted transaction rolls it back. Dropping a cursor cancels it and releases its connection.
- Spans, DB resources, and LLM streams consume per-request host resource slots until dropped. Drop them promptly.
- Blobstore object/list limits and HTTP body limits are enforced by the host.
- LLM stream order is text deltas, one usage, one stop, then `None`. Tool-enabled streaming is rejected before an upstream request; use non-streaming completion for tools.

## Worker rules

- Worker handlers must be idempotent. Delivery is at least once because leases can expire and failed jobs can retry.
- Non-empty ad-hoc worker versions are claimed exactly. Empty versions are name-only and may be claimed by any matching namespace/name version. Manager schedules remain version-pinned.
- `timeout_secs` controls stale-running recovery; worker dispatch also uses the configured worker job timeout. Zero option values select engine defaults; negative values are rejected.
- Generated result helpers return `None` while pending/running, decode successful results, and surface dead or malformed results as errors.

## Frequent failures

| Symptom | Likely cause | Fix |
|---|---|---|
| No usable `.wasm` | Missing `cdylib` or wrong target | Apply the template manifest and build for `wasm32-wasip2` |
| Component import/package error | Missing local bindings block or `wit/deps` | Generate the local world and link SDK WIT dependencies |
| Engine rejects module before ready | Capability import/config mismatch or unresolved secret | Align WIT imports, module flags, engine provider config, and namespace secrets |
| No generated router/handler | Used client generator for an implemented service | Add `WrServiceGenerator` |
| No ordinary client for worker service | `WrClientGenerator` skips `*WorkerService` | Add `WrWorkerClientGenerator` |
| Wrong include filename | Used proto filename instead of package | Include `$OUT_DIR/{proto_package}.rs` |
| Descriptor rejected/missing imports | Stale `.binpb` or omitted imports | Regenerate with `--include_imports` |
| Retried job duplicates side effects | Worker is not idempotent | Deduplicate transactionally by stable job/business key |
| Blob access denied/too large | Bucket outside allowlist or engine limit exceeded | Use an allowed bucket and bounded objects/lists |
| LLM streaming rejects request | Tools were configured | Use `complete()` instead of streaming |

Use [configuration](../../configuration.md) for full engine tables and [examples](./examples.md) for executable patterns.
