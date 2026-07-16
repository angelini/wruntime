# Documentation Ownership

Use one authority for each information class. Derived documentation explains intent and preferred usage; it must not replace source contracts.

| Information | Authority |
|---|---|
| Runtime behavior | Rust source and tests |
| Control-plane wire contract | [`proto/wruntime.proto`](../../../proto/wruntime.proto) |
| Host ABI | root [`wit/*.wit`](../../../wit/) |
| Public architecture | [`docs/architecture.md`](../../architecture.md) |
| Public configuration | [`docs/configuration.md`](../../configuration.md) |
| Test command behavior | [`Justfile`](../../../Justfile), [`dev/validate-all.sh`](../../../dev/validate-all.sh), and [`docs/testing.md`](../../testing.md) |
| Guest scaffold and dependency pins | guest [`module_template.md`](../guest-module-author/module_template.md), checked against actual manifests |
| Guest API discovery and semantics | guest [`api_guide.md`](../guest-module-author/api_guide.md); exact signatures remain owned by Rust/WIT source |
| Maintainer change guidance | files in this directory |
| Design narrative | [`docs/demo.md`](../../demo.md), explicitly non-authoritative |

## Change-to-documentation matrix

| Change | Update or review |
|---|---|
| Root WIT ABI or host implementation | `docs/host-bindings.md`, guest `api_guide.md` when preferred usage or semantics change, `generated_contracts.md`, and relevant constraints/examples |
| `wr-sdk` public helper or `wr-build` generator | guest `api_guide.md`, `codegen.md`, template/examples when usage changes, and generated-contract guidance |
| `proto/wruntime.proto` | `docs/grpc-api.md`, architecture/configuration where behavior changes, CLI docs, tests, and generated-contract guidance |
| Engine/manager/proxy configuration | `docs/configuration.md`, example configs, architecture when flow changes, and relevant guest capability guidance |
| Manager migration | migration policy in configuration/architecture as applicable, repository map, and migration tests |
| Module migration behavior | `docs/configuration.md`, guest template/constraints, and migration tests/examples |
| Deployment generation or templates | `docs/deployment.md`, sample deploy config, CLI help/tests, and parity/determinism invariants |
| Executable example | example configs/scripts, guest examples index, README if the advertised workflow changes, and matching validation guidance |
| Architecture/request flow | `docs/architecture.md`, concise root README/CLAUDE summary, invariants, and any affected public guide |
| Validation recipe or prerequisites | `Justfile`, `dev/validate-all.sh`, `docs/testing.md`, and maintainer `validation.md` |

Keep `CLAUDE.md` concise. It points maintainers here rather than becoming a second copy of repository guidance.
