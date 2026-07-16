# Decision Matrix

Choose the smallest pattern and capability set that satisfies the guest.

## Module pattern

| Need | Pattern | Generator | Guest implementation |
|---|---|---|---|
| Handle typed protobuf HTTP requests | Service handler | `WrServiceGenerator` | `ServiceGuest` plus generated service trait; dispatch with generated `_handle` |
| Trigger work over HTTP and call another module | Runner/client | Combined service + client generators | Generated trigger handler calling generated clients |
| Serve one API and call other modules | Combined service/client | Nested `WrCombinedGenerator` | Generated service trait plus normal clients |
| Consume engine-queued jobs | Worker implementation | `WrServiceGenerator` | Service named `*WorkerService`; configure `mode = "worker"`; make every handler idempotent |
| Submit jobs to a worker | Generated worker client | `WrWorkerClientGenerator` | Construct `*WorkerServiceClient` with `namespace.module` and optional version |
| Call HTTP without protobuf generation | Direct typed HTTP | None | Prefer `wr_sdk::http::TypedHttpRequest`; use raw WASI HTTP only for unsupported cases |

A runner is still an HTTP handler when something invokes it. There is no separate runner export macro.

## Generator composition

- Use `WrServiceGenerator` for services implemented by this guest.
- Use `WrClientGenerator` for ordinary services this guest calls. It intentionally skips `*WorkerService`.
- Use `WrWorkerClientGenerator` for worker job submission/status/result helpers.
- Nest `WrCombinedGenerator` when more than two outputs are needed. See [codegen](codegen.md#combined-generation).

## Capabilities

| Need | Module/config opt-in | Guest import or interface | Preferred API |
|---|---|---|---|
| PostgreSQL | `database = true`; usually `migrations_path` | `wruntime:db/database@0.4.0` | `wr_sdk::db` and prelude types |
| Blobstore | `blobstore = true`; engine `[blobstore]` with non-empty allowlist | `wruntime:blobstore/store@0.1.0` | typed `wr_sdk::blobstore` values plus store binding |
| Tracing | available to loaded guests | `wruntime:tracing/span@0.2.0` | `span!`, `root_span!`, `tracing::*` |
| LLM | `llm = true`; engine `[llm]` | `wruntime:llm/inference@0.1.0` | `CompletionBuilder` |
| Ephemeral filesystem | `fs = "tempdir"` | standard WASI filesystem imports | `std::fs` inside the mounted sandbox |
| Outbound HTTP | proxy `[egress].allowed_domains` permits external hosts | standard WASI HTTP imports | generated client or `wr_sdk::http` |
| Secret-backed environment | `[module.env] NAME = { secret = true }` after storing namespace secret | standard WASI environment | `std::env::var("NAME")` |
| Worker queue | `mode = "worker"`, `database = true`, concurrency/timeout settings | generated HTTP service handler | generated worker client for submitter |

Importing DB, blobstore, or LLM without the matching opt-in fails startup validation. See [host bindings](../../host-bindings.md) and [configuration](../../configuration.md).

## Worker version and delivery choice

| Submission | Version behavior |
|---|---|
| `WorkerServiceClient::new("namespace.worker", "1.2.3")` | Exact version only |
| `WorkerServiceClient::new("namespace.worker", "")` | Ad-hoc name-only job; any matching version may claim it |
| Manager schedule | Always version-pinned |

Worker and scheduled delivery is at least once. A lease may expire and a job may be retried, so use stable idempotency keys or transactional deduplication for side effects.
