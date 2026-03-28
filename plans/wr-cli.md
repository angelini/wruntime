# Plan: `wr-cli` — wruntime Management CLI

## Overview

A new `wr-cli` Cargo workspace member that connects to `wr-manager` over gRPC and provides a human-friendly interface for operating a wruntime deployment. All data comes from the existing `ManagerService` RPCs in `wr-common`.

---

## Command Interface

```
wr-cli [--manager <addr>] <subcommand>

Global flags:
  --manager  Manager gRPC address (default: http://127.0.0.1:9000)
             Can also be set via WR_MANAGER env var

Subcommands:
  engines  list | get <id> | remove <id>
  services list | get <namespace>.<module>
  metrics  summary
```

### `engines`

| Command | RPC | Description |
|---|---|---|
| `wr-cli engines list` | `ListEngines` | Table of engine ID, address, module count, last heartbeat |
| `wr-cli engines get <id>` | `ListEngines` (filtered) | Detail view: modules with health status per namespace/version |
| `wr-cli engines remove <id>` | `DeregisterEngine` | Deregisters engine; manager marks its routing rules unhealthy |

### `services`

"Services" are the logical namespace.module pairs that appear in the routing table, not physical engine processes.

| Command | RPC | Description |
|---|---|---|
| `wr-cli services list` | `GetRoutingTable` | Unique (namespace, module) pairs with healthy/unhealthy counts |
| `wr-cli services get <ns>.<module>` | `GetRoutingTable` (filtered) | All routing rules for that service: engine, version, health |

### `metrics`

| Command | RPC | Description |
|---|---|---|
| `wr-cli metrics summary` | `GetMetricsSummary` | Aggregate table: source → destination, request count, avg/p99 latency ms, error rate |

---

## Crate Layout

```
wr-cli/
  Cargo.toml
  src/
    main.rs          # clap App, global flags, subcommand dispatch
    client.rs        # shared ManagerServiceClient construction
    cmd/
      mod.rs
      engines.rs     # list, get, remove
      services.rs    # list, get
      metrics.rs     # summary
    display.rs       # tabled table rendering helpers
```

---

## Key Dependencies

| Crate | Purpose |
|---|---|
| `clap` (features: `derive`) | Argument parsing with nested subcommands |
| `tonic` | gRPC client (reuses version from workspace) |
| `tokio` (features: `rt-multi-thread`, `macros`) | Async runtime |
| `tabled` | Pretty-print tables to stdout |
| `wr-common` | Reuses generated `ManagerServiceClient` and all proto types |

---

## Implementation Steps

### 1. Scaffold crate

- Add `wr-cli` to workspace `members` in root `Cargo.toml`.
- Create `wr-cli/Cargo.toml` with the dependencies above.
- Create `wr-cli/src/main.rs` with a `clap` `Parser` struct:
  ```rust
  #[derive(Parser)]
  struct Cli {
      #[arg(long, env = "WR_MANAGER", default_value = "http://127.0.0.1:9000")]
      manager: String,
      #[command(subcommand)]
      command: Commands,
  }

  #[derive(Subcommand)]
  enum Commands {
      Engines(EnginesArgs),
      Services(ServicesArgs),
      Metrics(MetricsArgs),
  }
  ```

### 2. `client.rs` — shared gRPC connection

```rust
pub async fn connect(addr: &str) -> Result<ManagerServiceClient<Channel>> {
    ManagerServiceClient::connect(addr.to_string()).await
}
```

### 3. `cmd/engines.rs`

- **list**: call `list_engines({})`, render table with columns: `ID | Address | Modules | Last Heartbeat`.
  - Last heartbeat: the manager does not return a timestamp in `ListEnginesResponse` today — note this and display module count only; a follow-up proto change could add it.
- **get `<id>`**: call `list_engines`, filter client-side, show per-module rows: `Namespace | Module | Version | Healthy`.
- **remove `<id>`**: call `deregister_engine({ engine_id })`, confirm success and print the engine ID removed.

### 4. `cmd/services.rs`

- **list**: call `get_routing_table({})`, collect unique `(source_namespace, destination_module)` pairs from `rules`, group and display: `Service | Total Rules | Healthy | Unhealthy`.
- **get `<ns>.<module>`**: same RPC, filter rules where `destination_module == module` and `destination_namespace == ns`, display per-rule: `Rule ID | Engine | Version | Healthy`.

### 5. `cmd/metrics.rs`

- **summary**: call `get_metrics_summary({})`, aggregate `metrics` slice in memory:
  - Group by `(source, destination)`.
  - Compute: count, mean `duration_ms`, p99 `duration_ms` (sort + index), error count.
  - Display table: `Source | Destination | Requests | Avg ms | P99 ms | Errors`.

### 6. `display.rs`

Thin wrappers around `tabled::Table` so each command can call `print_table(rows)` without repeating formatting boilerplate.

---

## Proto Gaps & Notes

- `ListEnginesResponse` contains `engines: Vec<EngineRegistration>` which includes `engine_id`, `address`, and `modules`. It does **not** include last-heartbeat timestamps — the manager tracks these internally in `ManagerState::heartbeats` but does not expose them. The `get` sub-command can show health derived from `RoutingRule::healthy` instead.
- `GetMetricsSummaryResponse` returns raw `RequestMetrics` entries (no pre-aggregation). P99 is computed client-side over the returned buffer (up to 10,000 entries).
- There is no RPC to add a new engine from the CLI — engines self-register on startup. `engines remove` is the only mutation a CLI operator would need.
- There is no `services add/remove` — the routing table is managed by engines registering modules. A future `routing-rules` command group could expose `UpsertRoutingRule` / `DeleteRoutingRule` if manual overrides are needed.
