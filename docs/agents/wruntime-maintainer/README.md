# Wruntime Maintainer

> **Audience:** Contributors changing runtime behavior, repository contracts, SDK/build tooling, tests, deployment, or guest-visible interfaces.

This mode covers `wr-manager`, `wr-proxy`, `wr-engine`, `wr-common`, `wr-sdk`, `wr-build`, `wr-cli`, `wr-tests`, root `proto/` and `wit/`, manager migrations, deployment generation, and repository infrastructure. Guest examples and guest docs remain downstream maintainer work when a contract changes; there is no hybrid mode.

## Workflow

1. Identify the contract being changed and its owner in [documentation ownership](documentation_ownership.md).
2. Use the [repository map](repository_map.md) to locate sources, focused tests, and related docs.
3. Review the relevant [invariants](invariants.md).
4. Follow [generated-contract fanout](generated_contracts.md) for protobuf, WIT, SDK, or guest schema changes.
5. Before completion, preview the conservative selection with `just validate-changed --explain`, then run `just validate-changed` with any flags required by the reported profile and any additional focused checks required by the [validation matrix](validation.md).
6. Update the document that owns the changed public or maintainer contract.
7. Run additional authoritative validation only when the change class explicitly requires it.

Do not duplicate operator material here. Use the public guides for [architecture](../../architecture.md), [configuration](../../configuration.md), [deployment](../../deployment.md), [gRPC](../../grpc-api.md), and [testing](../../testing.md).
