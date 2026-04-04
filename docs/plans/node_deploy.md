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

Builds WASM + schemas, cross-compiles host binaries, and packages everything into a universal deployable bundle. The bundle contains artifacts for all deployment formats (systemd, docker) — the format is chosen at deploy time, not bundle time.

```
wr-cli node bundle \
    --proxy-config ./node-c/proxy.toml \
    --engine-config ./node-c/engine.toml  # repeatable
    --target x86_64-unknown-linux-gnu     # cargo target triple
    --workdir /opt/wruntime               # base directory for installed files (default: /opt/wruntime)
    --image-prefix myregistry.io/wruntime # optional: image name prefix for Dockerfiles (default: wr)
    --output node-c.tar.gz
    --skip-build                          # optional: skip WASM/schema compilation
```

**Build steps:**
1. Compile `.proto` → `.binpb` for each module with `schema_path` (via `protoc`)
2. Build WASM modules (`cargo component build --release --target wasm32-wasip2`)
3. Build host binaries (`cargo build --release --target <target> -p wr-proxy -p wr-engine`)
4. Generate all deployment artifacts: systemd unit files, Dockerfiles, `docker-compose.yml`, `.dockerignore`
5. Collect everything into a single tarball

**Universal bundle layout:**
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
    wr-proxy.service                # WorkingDirectory=<workdir>, Restart=on-failure
    wr-engine-inventory.service     # one per engine config, After=wr-proxy.service
  docker/
    Dockerfile.proxy
    Dockerfile.engine-inventory     # one per engine config
    docker-compose.yml
    .dockerignore
  manifest.json           # metadata: modules, checksums, target triple, workdir, image_prefix
```

The bundle is fully self-contained — deploy does not need to generate any files on the remote host. The format-specific subdirectories (`systemd/`, `docker/`) add only a few KB of text templates.

**Systemd unit generation** — units use `--workdir` to set `WorkingDirectory` and binary/config paths. Each unit file is a rendered template:
- `wr-proxy.service` — `ExecStart=<workdir>/wr-node/bin/wr-proxy <workdir>/wr-node/config/proxy.toml`, `Restart=on-failure`
- `wr-engine-<name>.service` — one per engine config, `After=wr-proxy.service`, same pattern

**Docker artifacts** — Dockerfiles use `--workdir` as the container `WORKDIR`. Containers use Google's [distroless](https://github.com/GoogleContainerTools/distroless) `cc-debian12` as the runtime base image — no shell, no package manager, no init system. Process supervision is handled by the container runtime or orchestrator. Each service gets its own Dockerfile producing a separate image. The generated `docker-compose.yml` wires up the topology with `depends_on` ordering and `restart: on-failure`.

**Generated Dockerfile (example — `Dockerfile.engine-inventory` with `--workdir /opt/wruntime`):**
```dockerfile
FROM gcr.io/distroless/cc-debian12
WORKDIR /opt/wruntime
COPY bin/wr-engine bin/wr-engine
COPY config/engine-inventory.toml config/engine.toml
COPY modules/ modules/
COPY schemas/ schemas/
COPY migrations/ migrations/
ENTRYPOINT ["bin/wr-engine", "config/engine.toml"]
```

**Generated `docker-compose.yml` (example):**
```yaml
services:
  proxy:
    build:
      context: .
      dockerfile: docker/Dockerfile.proxy
    ports:
      - "9001:9001"
      - "9002:9002"
    restart: on-failure

  engine-inventory:
    build:
      context: .
      dockerfile: docker/Dockerfile.engine-inventory
    ports:
      - "9100:9100"
    depends_on:
      - proxy
    restart: on-failure
```

Engine configs inside the bundle have paths rewritten to be relative to `wr-node/` (e.g., `wasm_path = "modules/inventory.wasm"`). Dockerfiles reference paths relative to the bundle root (`wr-node/`) as the build context.

**Why not multi-stage Dockerfile builds?** Host binaries are cross-compiled locally via `cargo build --release --target <triple>`. This keeps the build pipeline identical across formats, avoids embedding Rust source in the Docker context, produces smaller images (no builder layer cache), and means `docker build` is fast (just COPY, no compilation). The tradeoff is that the user must have the cross-compilation toolchain installed locally.

**Why a universal bundle?** All expensive artifacts (binaries, WASM modules, configs, schemas, migrations) are shared across deployment formats. The format-specific files (systemd units, Dockerfiles) are small text templates that add negligible size. A single `bundle` command produces one artifact that can be deployed to any target — the format choice is deferred to `deploy` time.

### `wr-cli node deploy`

Pushes a universal bundle to a remote host and installs using the specified format. The `--format` flag determines which deployment artifacts from the bundle are used.

```
wr-cli node deploy node-c.tar.gz deploy@10.0.1.50 \
    --format systemd                  # deployment format: systemd or docker
    --ssh-key ~/.ssh/id_ed25519       # optional
    --ssh-port 22                     # default: 22
```

Note: `--workdir` is not a deploy flag — it is baked into the bundle at `bundle` time (systemd unit paths and Docker `WORKDIR` are pre-rendered).

| Format | Status | Description |
|--------|--------|-------------|
| `systemd` | Planned | Install systemd units for bare-metal / VM deployment |
| `docker` | Planned | Run `docker compose up` with pre-built images |
| `k8s` | Future | Apply Kubernetes manifests |

**Steps (systemd format):**
1. `scp` tarball to remote `/tmp/`
2. `ssh`: unpack to install directory (read from `manifest.json`)
3. `ssh`: copy pre-built unit files from `wr-node/systemd/` to `/etc/systemd/system/`
4. `ssh`: `systemctl daemon-reload`, `systemctl enable --now` each service
5. Poll manager gRPC (`ListEngines`) until new engine appears or timeout (60s)

**Steps (docker format):**
1. `scp` tarball to remote host, unpack
2. `ssh`: `docker compose -f wr-node/docker/docker-compose.yml up -d` from the bundle root
3. Poll manager gRPC (`ListEngines`) until new engine appears or timeout (60s)

Alternatively, for docker format, users may skip `node deploy` entirely and run `docker compose up` themselves (locally or in CI). The bundle is a self-contained build context.

### `wr-cli node status`

Inspect a bundle without deploying.

```
wr-cli node status node-c.tar.gz
```

Reads `manifest.json` and prints: target triple, workdir, image prefix, included modules (namespace/name/version), checksums, config files, and deployment artifacts (systemd unit files, Dockerfiles).

## Workflow Examples

### Bundle once, deploy anywhere

```bash
# 1. Generate configs for new node
wr-cli node init ./node-c \
    --host 10.0.1.50 \
    --db-url "postgres://postgres@10.0.1.1:5432/wruntime" \
    --engine-config examples/ecommerce/engine-inventory-1.toml

# 2. (Optional) Edit generated configs
vim ./node-c/engine.toml

# 3. Build universal bundle (cross-compile for Linux)
wr-cli node bundle \
    --proxy-config ./node-c/proxy.toml \
    --engine-config ./node-c/engine.toml \
    --target x86_64-unknown-linux-gnu \
    --workdir /opt/wruntime \
    --output node-c.tar.gz

# 4a. Deploy to VM with systemd
wr-cli node deploy node-c.tar.gz deploy@10.0.1.50 --format systemd

# 4b. Or deploy with Docker
wr-cli node deploy node-c.tar.gz deploy@10.0.1.50 --format docker

# 4c. Or run Docker locally / in CI (extract bundle first)
tar xzf node-c.tar.gz && cd wr-node && docker compose -f docker/docker-compose.yml up -d

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
- Walk engine configs, collect all referenced artifacts
- Rewrite paths in configs to bundle-relative layout
- Cross-compile host binaries (`cargo build --release --target <triple> -p wr-proxy -p wr-engine`)
- Generate all deployment artifacts: systemd unit files (using `--workdir`), Dockerfiles (`WORKDIR <workdir>`, COPY pre-built binaries into `gcr.io/distroless/cc-debian12`), `docker-compose.yml`, `.dockerignore`
- Package everything into a single tarball with `tar` + `flate2`
- Generate and include `manifest.json` (includes target triple, workdir, image_prefix, checksums)

**`deploy` implementation:**
- Accept `--format` flag (`DeployFormat` enum: `Systemd`, `Docker`, future `K8s`)
- Read `manifest.json` from bundle for metadata (workdir, etc.)
- **Systemd:** `scp` tarball → `ssh` unpack to workdir → copy units from `systemd/` to `/etc/systemd/system/` → daemon-reload + enable
- **Docker:** `scp` tarball → `ssh` unpack → `docker compose -f docker/docker-compose.yml up -d`
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
2. **`node bundle`** — Create tarball, extract it, verify: all referenced paths exist, configs parse, systemd unit files contain correct paths, Dockerfiles reference correct binaries/configs, `docker-compose.yml` is valid
3. **`node status`** — Run against generated bundle, verify output matches contents
4. **`node deploy --format systemd`** — Test against a local VM or Docker container with SSH enabled:
   - Verify tarball is copied and unpacked
   - Verify systemd units are installed and services start
   - Verify `wr-cli engines list` shows the new engine
5. **`node deploy --format docker`** — Test with `docker compose up -d` locally:
   - Verify images build successfully on distroless base
   - Verify containers start and stay healthy
   - Verify `wr-cli engines list` shows the new engine
6. **Integration** — Run `just tidy` to verify formatting and lints pass
