# Documentation Ownership

Each public guide has one purpose. Link to the owning guide instead of copying
its detailed contract into another page. Rust source and tests remain the
authority for implementation behavior; protobuf and WIT remain the wire and ABI
authorities.

## Public guides

| Document | Owns | Does not own |
| --- | --- | --- |
| [`README.md`](../../../README.md) | Product overview, capabilities, quick start, examples, and documentation navigation. | Detailed configuration, operation, or protocol reference. |
| [`docs/architecture.md`](../../architecture.md) | Components, topology, request flow, trust boundaries, readiness model, database lifecycle, and concise failure behavior. | Development-fixture mechanics, exact persistence schemas, or operator command inventories. |
| [`docs/configuration.md`](../../configuration.md) | Current keys, defaults, constraints, and minimal valid examples for manager, proxy, engine, modules, and CLI environment. | Deployment procedures or duplicated API field reference. |
| [`docs/deployment.md`](../../deployment.md) | Task-oriented operator workflows, status, rollback, recovery, and troubleshooting. | Test qualification narratives or internal persistence algorithms. |
| [`docs/grpc-api.md`](../../grpc-api.md) | Service purpose, authorization boundaries, and non-obvious API semantics. | Mechanical message/field duplication; exact wire fields remain in the proto. |
| [`docs/host-bindings.md`](../../host-bindings.md) | Runtime-provided guest capabilities, behavior, limits, and errors. | Module scaffolding or exact generated signatures. |
| [`docs/sdk.md`](../../sdk.md) and [`guest-module-author/`](../guest-module-author/) | SDK discovery, module construction, preferred guest usage, and guest constraints. | Host implementation details or operator workflows. |
| [`docs/testing.md`](../../testing.md) | Commands, prerequisites, fixture consumption, and validation selection. | Repeated lists of what individual tests prove. |
| [`wruntime-maintainer/`](./) | Exhaustive invariants, repository map, generated-contract fanout, documentation ownership, and change-class validation. | Public tutorials or duplicated operator reference. |

Other exact authorities:

| Information | Authority |
| --- | --- |
| Control-plane wire contract | [`proto/wruntime.proto`](../../../proto/wruntime.proto) |
| Guest host ABI | root [`wit/*.wit`](../../../wit/) |
| Test recipe behavior | [`Justfile`](../../../Justfile) and [`dev/validate-all.sh`](../../../dev/validate-all.sh) |
| Guest scaffold and dependency pins | [`guest-module-author/module_template.md`](../guest-module-author/module_template.md), checked against manifests |
| Guest API semantics | [`guest-module-author/api_guide.md`](../guest-module-author/api_guide.md); exact signatures stay in Rust/WIT source |

## Change-to-documentation matrix

| Change | Update or review |
| --- | --- |
| Root WIT ABI or host implementation | Host bindings; guest API guide when preferred usage changes; generated-contract guidance; affected examples. |
| `wr-sdk` helper or `wr-build` generator | Guest API/codegen guides, template, examples, and generated-contract guidance. |
| `proto/wruntime.proto` | gRPC semantics, affected architecture/configuration/deployment pages, CLI behavior, and generated-contract guidance. |
| Operator lifecycle, authorization, or agent backend | Architecture guarantees, deployment workflow/recovery, configuration, testing, maintainer invariants/matrix, and protected assertions. |
| Manager/proxy/engine configuration | Configuration, maintained examples, deployment templates, and architecture when the boundary changes. |
| Manager migration | Repository map, migration tests, and concise architecture/configuration policy if externally visible. |
| Tenant provisioning or module migration | Architecture lifecycle, configuration keys, deployment workflow, guest constraints, and affected examples. |
| Deployment generation or backend effects | Deployment, configuration, sample deploy config, CLI help/tests, maintainer validation, and protected qualification. |
| Executable example | Example config/scripts, guest examples index, README when advertised behavior changes, and matching validation guidance. |
| Validation recipe or prerequisite | Justfile/scripts, testing guide, and maintainer validation matrix. |

Keep repository-wide agent guidance in root `AGENTS.md`. Keep `CLAUDE.md` as a
pointer rather than a second copy.
