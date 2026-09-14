# Testing

This page documents commands and prerequisites. Maintainers must also follow the
change-class requirements in the
[validation matrix](agents/wruntime-maintainer/validation.md).

## Common commands

```bash
just build
just check
just test
just test-integration
just test-one <name>
just test-wasm
just test-wasm-one db
just build-wasm-guests
just test-tenant-isolation-e2e
just test-lifecycle-runners
just validate-ecommerce
just docs-check
just validate-changed
just validate-changed --explain
just validate-all --no-deployment-e2e
just validate-all --deployment-e2e
```

Use `just` with no arguments for the complete recipe list. `just test-one`
accepts either an integration-test filename without `.rs` or a Cargo test-name
filter. Test recipes build required WASM fixtures before invoking Cargo.

`just docs-check` validates repository-local Markdown destinations and anchors
and parses every fenced `toml` block. Runtime config tests additionally parse the
marked manager, proxy, and engine examples with their owning Serde types, and
CLI parser tests cover the documented operator command shapes.

## Development services

PostgreSQL, RustFS, and local observability run in a Docker Compose fixture:

```bash
just dev-up
just dev-ps
just dev-logs
just dev-down
```

Run `just dev-up` from each worktree before tests that require PostgreSQL or
RustFS. Each linked worktree receives an isolated Compose project, persistent
port allocation, and state under `<absolute-git-dir>/wruntime-dev-state`.
`owner.json` and `fixture/ready.json` bind that state to the worktree, endpoints,
PKI, provisioning inputs, migrations, and provisioner artifact. Consumers fail
before database access if the fixture is absent or incompatible; rerun host
`just dev-up` in that worktree to replace it.

Inside the Pi sandbox, Docker setup is intentionally unavailable. Tests consume
the compatible host-prepared fixture exposed to the worktree. Do not attempt to
provision or repair host infrastructure from the sandbox.

`just dev-reset-db` destroys and recreates this worktree's manager and engine
job persistence. Use it only when intentionally changing the clean embedded
migration baseline. Tenant namespace state follows the separate offline
provision/migrate workflow.

## Prerequisites

Workspace tests require Rust, Cargo, `just`, and `protoc`. WASM host tests also
require:

```bash
rustup target add wasm32-wasip2
```

Install `wasm-tools` and start the development services before DB/S3-backed
WASM tests. Direct `cargo test` does not build guest artifacts or export fixture
environment variables; prefer the Just recipes unless the required artifacts
and `WRT_TEST_DB_URL`/`WRT_TEST_S3_*` variables are already present.

DB-backed tests skip under the shared helper policy when `WRT_TEST_DB_URL` is
absent. Such a skip is not completion evidence for database, migration, or
durable-operation changes.

Examples require built workspace binaries, their WASM components, the same
development fixture, and Python 3 for small rendering/assertion helpers. Codegen
E2E additionally requires `ANTHROPIC_API_KEY`; its run scripts use Python's JSON
support and do not require `jq`.

## Focused validation

`just validate-changed [--base REF] [--explain]` inspects committed-since-base,
staged, unstaged, deleted, renamed, and untracked paths. It selects one
conservative profile:

- documentation: whitespace, workspace formatting, and `just docs-check`;
- workspace Rust: formatting, check, lint, and tests;
- WIT/SDK/engine/guest-host changes: workspace and guest checks plus WASM builds;
- mixed, unknown, validation-policy, or deployment-sensitive changes: the
  corresponding broad `validate-all` path.

`--explain` prints the selected profile and commands without running them. The
selector is feedback, not pre-merge evidence; it does not replace additional
checks from the maintainer validation matrix.

## Broad validation

Local broad validation requires an explicit deployment choice:

```bash
just validate-all --no-deployment-e2e
```

In the Pi sandbox, run exactly:

```bash
just validate-all --no-deployment-e2e --skip-dev-up --no-codegen-e2e
```

This skips Docker startup, protected remote deployment, and codegen only. It
must still run multi-node, ecommerce, and stockmarket E2E; do not pass
`--no-e2e`.

`validate-all` runs formatting, checks, lints, WASM builds, Rust tests, and the
enabled E2E examples. Example stages are serial because they reset shared state
within one worktree. Logs and `summary.txt` are written below
`target/validate-all/<timestamp>-<pid>/`.

## Change-specific gates

Use the validation matrix for exact selection. Common additional requirements
are:

| Change | Additional gate |
| --- | --- |
| Host bindings, root WIT, SDK, build generator, or test guests | Focused `just test-wasm-one <target>`, then `just test-wasm`. |
| Tenant provisioning, migration, or deployment | `just test-tenant-isolation-e2e`, provisioning/migration/namespace tests, `just test-wasm`, and affected examples. |
| Runtime lifecycle or foreground runner | `just test-lifecycle-runners`, lifecycle/proxy/version tests, `just test-wasm`, and all local examples. |
| Deployment generation, node-agent effects, or manager rollout | Protected `just validate-all --deployment-e2e`. |
| Ecommerce runtime behavior | `just validate-ecommerce`; any `WARN` or `WARNING` fails. |

A local `--no-deployment-e2e` result records an environmental skip. It cannot
qualify deployment generation or remote lifecycle behavior.

## Protected deployment runner

Protected deployment uses three disposable Systemd hosts and a locked `uv`
project under `dev/deployment-e2e/`. Before a live run:

```bash
just deployment-e2e-python-test
just deployment-e2e-preflight
just validate-all --deployment-e2e
```

The runner requires `flock`, `uv`, `cargo-zigbuild`, SSH, `psql`, and the
repository-documented `PVE_*`, `WRT_DEPLOY_E2E_*`, and
`WRT_SECRET_ENCRYPTION_KEY` environment values. Target definitions live in
`dev/deployment-e2e.toml`; Python dependencies and versions live in the nested
`pyproject.toml`, `.python-version`, and `uv.lock`.

The live gate starts and ends with target reset and exercises manager and node
Systemd lifecycle, database-host provisioning, deployment/rollback, ambiguous
recovery, and manager failed-closed recovery. Do not run it without all
protected inputs. The Python-only tests validate harness contracts but do not
replace live evidence.

## Local examples and foreground lifecycle

Repository example recipes build guest artifacts before invoking one foreground
`wr-cli dev run` owner. Useful commands are:

```bash
just multi-node-inline
just validate-ecommerce
just stockmarket-inline
just codegen-inline
```

The foreground runner uses typed lifecycle identity and routing convergence,
then owns scenario and service teardown. Focused lifecycle tests must leave no
owned descendant or persistent supervisor state.

## WASM build cache

All repository guests share `target/wasm-guests`. `just build-wasm-guests`
builds them sequentially. `just clean-wasm-cache` removes only that shared
cache; `cargo clean` removes the entire root target tree. Deleting a staged guest
artifact does not require clearing the shared cache—the next build recreates it.
