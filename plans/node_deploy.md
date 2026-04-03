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

Builds WASM + schemas, cross-compiles host binaries, and packages everything into a deployable bundle. The `--format` flag controls the output format.

```
wr-cli node bundle \
    --proxy-config ./node-c/proxy.toml \
    --engine-config ./node-c/engine.toml  # repeatable
    --target x86_64-unknown-linux-gnu     # cargo target triple
    --format systemd                      # output format (default: systemd)
    --remote-dir /opt/wruntime            # install path for systemd units (default: /opt/wruntime)
    --output node-c.tar.gz
    --skip-build                          # optional: skip WASM/schema compilation
```

**Output formats:**

| Format | Status | Description |
|--------|--------|-------------|
| `systemd` | Planned | Tarball with pre-built systemd unit files for bare-metal / VM deployment |
| `docker` | Future | Dockerfile + context for container image builds |
| `k8s` | Future | Kubernetes manifests (Deployment, Service, ConfigMap) |

The `--format` flag is an enum that will be extended as new formats are added. Format-specific flags (e.g., `--remote-dir` for systemd) are validated per format.

**Steps (systemd format):**
1. Compile `.proto` → `.binpb` for each module with `schema_path` (via `protoc`)
2. Build WASM modules (`cargo component build --release --target wasm32-wasip2`)
3. Build host binaries (`cargo build --release --target <target> -p wr-proxy -p wr-engine`)
4. Generate systemd unit files from config (see below)
5. Collect artifacts into tarball

**Bundle layout (systemd):**
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
  systemd/
    wr-proxy.service                # WorkingDirectory=<remote-dir>, Restart=on-failure
    wr-engine-inventory.service     # one per engine config, After=wr-proxy.service
  manifest.json           # metadata: modules, checksums, target triple, format
```

**Systemd unit generation** — units are built at bundle time using `--remote-dir` to set `WorkingDirectory` and binary/config paths. Each unit file is a rendered template:
- `wr-proxy.service` — `ExecStart=<remote-dir>/wr-node/bin/wr-proxy <remote-dir>/wr-node/config/proxy.toml`, `Restart=on-failure`
- `wr-engine-<name>.service` — one per engine config, `After=wr-proxy.service`, same pattern

This means the bundle is fully self-contained — deploy does not need to generate any files on the remote host.

Engine configs inside the tarball have paths rewritten to be relative to `wr-node/` (e.g., `wasm_path = "modules/inventory.wasm"`).

### `wr-cli node deploy`

Pushes a bundle to a remote host, installs pre-built systemd units, and starts services. The deploy command reads `manifest.json` to determine the bundle format and runs the appropriate installation steps.

```
wr-cli node deploy node-c.tar.gz deploy@10.0.1.50 \
    --ssh-key ~/.ssh/id_ed25519       # optional
    --ssh-port 22                     # default: 22
```

Note: `--remote-dir` is no longer a deploy flag — it is baked into the bundle at `bundle` time (systemd unit paths are pre-rendered).

**Steps (systemd format):**
1. `scp` tarball to remote `/tmp/`
2. `ssh`: unpack to install directory (read from `manifest.json`)
3. `ssh`: copy pre-built unit files from `wr-node/systemd/` to `/etc/systemd/system/`
4. `ssh`: `systemctl daemon-reload`, `systemctl enable --now` each service
5. Poll manager gRPC (`ListEngines`) until new engine appears or timeout (60s)

### `wr-cli node status`

Inspect a bundle without deploying.

```
wr-cli node status node-c.tar.gz
```

Reads `manifest.json` and prints: bundle format, target triple, remote install directory, included modules (namespace/name/version), binary checksums, config files, and (for systemd format) included unit files.

## Workflow Example

```bash
# 1. Generate configs for new node
wr-cli node init ./node-c \
    --host 10.0.1.50 \
    --db-url "postgres://postgres@10.0.1.1:5432/wruntime" \
    --engine-config examples/ecommerce/engine-inventory-1.toml

# 2. (Optional) Edit generated configs
vim ./node-c/engine.toml

# 3. Build + package (cross-compile for Linux, systemd format)
wr-cli node bundle \
    --proxy-config ./node-c/proxy.toml \
    --engine-config ./node-c/engine.toml \
    --target x86_64-unknown-linux-gnu \
    --format systemd \
    --remote-dir /opt/wruntime \
    --output node-c.tar.gz

# 4. Deploy to VM (reads format + remote-dir from manifest)
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
- Accept `--format` flag (`BundleFormat` enum: `Systemd`, future `Docker`, `K8s`)
- Reuse schema compilation logic from `cmd/dev.rs` (the `protoc` invocation) — extract to shared helper
- Reuse WASM build logic from `cmd/dev.rs` (`cargo component build`) — extract to shared helper
- Run `cargo build --release --target <triple> -p wr-proxy -p wr-engine` for host binaries
- Walk engine configs, collect all referenced artifacts
- **Systemd format:** render unit file templates using `--remote-dir` to produce absolute paths in `ExecStart`/`WorkingDirectory`; include rendered `.service` files in `systemd/` directory
- Rewrite paths in configs before adding to tarball
- Build tarball with `tar` + `flate2` crates
- Generate and include `manifest.json` (includes `format`, `remote_dir`, checksums, target triple)

**`deploy` implementation:**
- Read `manifest.json` from tarball to determine format and install directory
- Shell out to `scp` via `std::process::Command`
- Shell out to `ssh` for: unpack, copy pre-built units from `systemd/` to `/etc/systemd/system/`, daemon-reload + enable
- No file generation on the remote host — all artifacts are pre-built in the bundle
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
