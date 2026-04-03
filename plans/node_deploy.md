# Plan: `wr-cli node` — Remote Node Deployment

## Context

Today all deployment is local (`dev up`, `dev deploy`). To add a node to a cluster, a developer must manually copy binaries, configs, and WASM artifacts to a VM and start processes by hand. This feature adds CLI commands to package, deploy, and manage remote nodes as a first-class workflow.

The cluster join mechanism already exists — a proxy discovers managers via the shared Postgres `wr_managers` table, and engines register with their local proxy. So the problem is purely: get the right files to the right place with the right configs, and start the processes.

## New CLI Commands

### `wr-cli node init <output-dir>`

Generates a node config directory from an existing engine config template.

```
wr-cli node init ./node-c \
    --host 10.0.1.50 \
    --db-url "postgres://postgres@10.0.1.1:5432/wruntime" \
    --engine-config examples/ecommerce/engine-inventory-1.toml \
    --proxy-port 9001          # default: 9001
    --control-port 9002        # default: 9002
    --engine-port 9100         # default: 9100
    --guest-db-url <url>       # optional
```

**Produces:**
- `<output-dir>/proxy.toml` — listen/control addresses using `--host`, DB URL for manager discovery
- `<output-dir>/engine.toml` — copies `[[module]]` sections from template, rewrites `listen_address`, `node.proxy_address`, `node.control_address`, DB URLs. Artifact paths rewritten to bundle-relative layout (`modules/<name>.wasm`, `schemas/<name>.binpb`, `migrations/<name>/`)

### `wr-cli node bundle`

Builds WASM + schemas, cross-compiles host binaries, and packages everything into a deployable tarball.

```
wr-cli node bundle \
    --proxy-config ./node-c/proxy.toml \
    --engine-config ./node-c/engine.toml  # repeatable
    --target x86_64-unknown-linux-gnu     # cargo target triple
    --output node-c.tar.gz
    --skip-build                          # optional: skip WASM/schema compilation
```

**Steps:**
1. Compile `.proto` → `.binpb` for each module with `schema_path` (via `protoc`)
2. Build WASM modules (`cargo component build --release --target wasm32-wasip2`)
3. Build host binaries (`cargo build --release --target <target> -p wr-proxy -p wr-engine`)
4. Collect artifacts into tarball

**Bundle layout:**
```
wr-node/
  bin/
    wr-proxy
    wr-engine
  config/
    proxy.toml
    engine.toml           # (or engine-1.toml, engine-2.toml if multiple)
  modules/
    inventory.wasm
    client.wasm
  schemas/
    inventory.binpb
  migrations/
    inventory/
      V1__create_tables.sql
  manifest.json           # metadata: modules, checksums, target triple
```

Engine configs inside the tarball have paths rewritten to be relative to `wr-node/` (e.g., `wasm_path = "modules/inventory.wasm"`).

### `wr-cli node deploy`

Pushes a bundle to a remote host, installs systemd units, and starts services.

```
wr-cli node deploy node-c.tar.gz deploy@10.0.1.50 \
    --remote-dir /opt/wruntime        # default: /opt/wruntime
    --ssh-key ~/.ssh/id_ed25519       # optional
    --ssh-port 22                     # default: 22
```

**Steps:**
1. `scp` tarball to remote `/tmp/`
2. `ssh`: unpack to `<remote-dir>/`
3. `ssh`: generate systemd unit files:
   - `wr-proxy.service` — runs `wr-node/bin/wr-proxy wr-node/config/proxy.toml`, `WorkingDirectory=<remote-dir>`, `Restart=on-failure`
   - `wr-engine-<name>.service` — one per engine config, `After=wr-proxy.service`
4. `ssh`: install units to `/etc/systemd/system/`, `systemctl daemon-reload`, `systemctl enable --now` each service
5. Poll manager gRPC (`ListEngines`) until new engine appears or timeout (60s)

### `wr-cli node status`

Inspect a bundle without deploying.

```
wr-cli node status node-c.tar.gz
```

Reads `manifest.json` and prints: target triple, included modules (namespace/name/version), binary checksums, config files.

## Workflow Example

```bash
# 1. Generate configs for new node
wr-cli node init ./node-c \
    --host 10.0.1.50 \
    --db-url "postgres://postgres@10.0.1.1:5432/wruntime" \
    --engine-config examples/ecommerce/engine-inventory-1.toml

# 2. (Optional) Edit generated configs
vim ./node-c/engine.toml

# 3. Build + package (cross-compile for Linux)
wr-cli node bundle \
    --proxy-config ./node-c/proxy.toml \
    --engine-config ./node-c/engine.toml \
    --target x86_64-unknown-linux-gnu \
    --output node-c.tar.gz

# 4. Deploy to VM
wr-cli node deploy node-c.tar.gz deploy@10.0.1.50

# 5. Verify
wr-cli engines list
wr-cli services list
```

## Implementation

### Step 1: Add dependencies to `wr-cli/Cargo.toml`

- `flate2` — gzip compression
- `tar` — tarball creation/reading
- `sha2` — SHA256 checksums for manifest

### Step 2: Create `wr-cli/src/cmd/node.rs`

New command module with four subcommands: `init`, `bundle`, `deploy`, `status`.

**`NodeArgs` / `NodeCommand` enum** — clap derive structs mirroring the CLI above.

**`init` implementation:**
- Parse template engine config (reuse `wr_engine::config::EngineConfig` deserialization)
- Generate `proxy.toml` via string template (simple — proxy config is ~15 lines)
- Clone engine config, rewrite addresses and artifact paths to bundle-relative layout
- Serialize back to TOML via `toml::to_string_pretty` (add `toml` as a dep if not already present)

**`bundle` implementation:**
- Reuse schema compilation logic from `cmd/dev.rs` (the `protoc` invocation) — extract to shared helper
- Reuse WASM build logic from `cmd/dev.rs` (`cargo component build`) — extract to shared helper
- Run `cargo build --release --target <triple> -p wr-proxy -p wr-engine` for host binaries
- Walk engine configs, collect all referenced artifacts
- Build tarball with `tar` + `flate2` crates
- Rewrite paths in configs before adding to tarball
- Generate and include `manifest.json`

**`deploy` implementation:**
- Shell out to `scp` via `std::process::Command`
- Shell out to `ssh` for: unpack, generate systemd units, install + start
- Systemd unit template is a const string with placeholder substitution
- Poll manager gRPC (`ListEngines`) until new engine appears or timeout (60s)

**`status` implementation:**
- Open tarball, extract `manifest.json`, pretty-print

### Step 3: Extract shared build helpers from `cmd/dev.rs`

Move proto compilation and WASM build logic into `cmd/build_helpers.rs` so both `dev deploy` and `node bundle` can use them.

Functions to extract:
- `compile_schemas(config, modules_filter)` — runs `protoc` for each module
- `build_wasm_modules(config, modules_filter)` — runs `cargo component build` for each module

### Step 4: Register in `main.rs`

- Add `pub mod node;` to `cmd/mod.rs`
- Add `Node(cmd::node::NodeArgs)` variant to `Commands` enum
- Note: `node init`, `node bundle`, and `node status` don't need `--manager`, but `node deploy` does for verification polling. Make `--manager` optional at the CLI level (change from required `String` to `Option<String>`), and have commands that need it error if missing.

### Files to modify

| File | Change |
|------|--------|
| `wr-cli/Cargo.toml` | Add `flate2`, `tar`, `sha2` deps |
| `wr-cli/src/main.rs` | Add `Node` command variant, make `--manager` optional |
| `wr-cli/src/cmd/mod.rs` | Add `pub mod node;`, `pub mod build_helpers;` |
| `wr-cli/src/cmd/node.rs` | **New** — all four subcommands |
| `wr-cli/src/cmd/build_helpers.rs` | **New** — extracted build logic |
| `wr-cli/src/cmd/dev.rs` | Refactor to use `build_helpers` |

### Files to read (reference during implementation)

| File | Why |
|------|-----|
| `wr-cli/src/cmd/dev.rs` | Build/deploy logic to reuse |
| `wr-engine/src/config.rs` | Engine config struct for parsing |
| `wr-proxy/src/config.rs` | Proxy config struct for generation |
| `examples/multi-node/` | Reference configs for multi-node layout |
| `examples/ecommerce/run.sh` | Current deployment workflow |

## Verification

1. **`node init`** — Generate configs, verify TOML is valid by parsing with the existing config structs
2. **`node bundle`** — Create tarball, extract it, verify all referenced paths exist and configs parse
3. **`node status`** — Run against generated bundle, verify output matches contents
4. **`node deploy`** — Test against a local VM or Docker container with SSH enabled:
   - Verify tarball is copied and unpacked
   - Verify systemd units are installed and services start
   - Verify `wr-cli engines list` shows the new engine
5. **Integration** — Run `just tidy` to verify formatting and lints pass
