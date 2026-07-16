# Guest Module Author

> **Audience:** Authors of WASI Preview 2 guest modules that consume existing wruntime SDK and WIT contracts.

Use this mode for guest `Cargo.toml`, `build.rs`, `src/lib.rs`, local `.proto` or `world.wit`, module-owned migrations, and the guest's `[[module]]` configuration. Do not alter host/runtime contracts merely to make a guest work. A required change to root `wit/`, `wr-sdk`, or `wr-build` switches the task to [wruntime maintainer mode](../wruntime-maintainer/README.md).

## Workflow

1. Choose a service, runner/client, combined, or worker pattern in the [decision matrix](decision_matrix.md).
2. Start with the composable [module template](module_template.md).
3. Add only the capabilities the module needs.
4. Use the [API guide](api_guide.md) for discovery, then inspect Rust or WIT source for exact signatures.
5. Check the [constraints](constraints.md).
6. Follow a production [example](examples.md); test guests are protocol fixtures only.
7. Build, format, lint, and run focused validation from the [template](module_template.md#build-and-validation).

Public references:

- [Configuration](../../configuration.md) — engine and module settings
- [Host bindings](../../host-bindings.md) — capability concepts and configuration
- [Schemas](../../schemas.md) — protobuf descriptors and routing paths
