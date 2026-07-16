# Agent Guide

Choose exactly one mode before changing the repository.

- [Guest module author](guest-module-author/README.md)
- [Wruntime maintainer](wruntime-maintainer/README.md)

| Task | Mode |
|---|---|
| Build a WASM guest against existing SDK and WIT APIs | Guest module author |
| Change a guest's source, schema, migrations, or `[[module]]` entry | Guest module author |
| Change the runtime, SDK, WIT, control-plane proto, CLI, tests, deployment, or repository contracts | Wruntime maintainer |
| Change a guest-visible contract and update examples or guest docs downstream | Wruntime maintainer |

A task that requires changes to root `wit/`, `wr-sdk`, or `wr-build` crosses the boundary and is maintainer work. There is no third or hybrid mode.
