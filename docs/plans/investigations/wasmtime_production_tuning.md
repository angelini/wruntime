# Wasmtime Production Tuning for High-Scale Usage

Investigation into production recommendations for running wasmtime at scale, based on official docs, API references, and community findings.

## 1. Pre-Compilation

Compile components ahead of time (CI/deploy) and deserialize at startup — removes Cranelift from the critical path (~1.2s → ~5μs).

```rust
// Build time:
let precompiled = engine.precompile_component(&wasm_bytes)?;
std::fs::write("module.cwasm", &precompiled)?;

// Runtime:
let component = unsafe { Component::deserialize_file(&engine, "module.cwasm")? };
```

Use `engine.precompile_compatibility_hash()` to verify compile/runtime `Config` match. Artifacts are NOT portable across CPU architectures or wasmtime versions.

Ref: [Pre-Compiling Wasm](https://docs.wasmtime.dev/examples-pre-compiling-wasm.html)

## 2. Epoch-Based Interruption (Critical Gap)

Without epoch or fuel, a `loop {}` in WASM **pins the tokio worker thread forever** — `tokio::time::timeout` won't fire because the future never yields.

```rust
// Config
wt_config.epoch_interruption(true);

// Per-store
store.set_epoch_deadline(1);
store.epoch_deadline_async_yield_and_update(1);

// Background ticker
tokio::spawn(async move {
    let mut interval = tokio::time::interval(Duration::from_millis(10));
    loop {
        interval.tick().await;
        engine.increment_epoch();
    }
});
```

~10% overhead vs ~2x for fuel. Use epoch unless you need deterministic metering.

Ref: [Interrupting Execution](https://docs.wasmtime.dev/examples-interrupting-wasm.html)

## 3. Pooling Allocator Tuning

```rust
let mut pool = PoolingAllocationConfig::new();

// Component model components may contain 2-5 core modules internally,
// so memories/tables must be higher than component instances
pool.total_memories(total_instances * 3);
pool.total_tables(total_instances * 3);

// Warm slot retention (0 = aggressive RSS reclaim, 100 = fast re-instantiation)
pool.max_unused_warm_slots(100);

// Keep memory pages resident to avoid page faults on next instantiation
pool.linear_memory_keep_resident(0);  // 0 = decommit everything (saves RSS)

// Linux x86_64 only: memory protection keys for denser layout
if PoolingAllocationConfig::are_memory_protection_keys_available() {
    pool.memory_protection_keys(MpkEnabled::Enable);
}
```

Ref: [PoolingAllocationConfig API](https://docs.rs/wasmtime/latest/wasmtime/struct.PoolingAllocationConfig.html)

## 4. Per-Store Resource Limits

Defense-in-depth on top of the pooling allocator — prevents a single module from exhausting the pool:

```rust
let limits = StoreLimitsBuilder::new()
    .memory_size(10 * 1024 * 1024)  // 10 MiB per linear memory
    .table_elements(10_000)
    .instances(10)
    .tables(10)
    .memories(10)
    .trap_on_grow_failure(true)
    .build();

store.limiter(|data| &mut data.limits);
```

Ref: [StoreLimitsBuilder API](https://docs.rs/wasmtime/latest/wasmtime/struct.StoreLimitsBuilder.html)

## 5. Memory Config

```rust
// Virtual memory per linear memory (default 4 GiB on 64-bit)
// Larger = more bounds-check elimination
wt_config.memory_reservation(4 * (1 << 30));

// Guard pages (default 32 MiB) — combined with reservation eliminates bounds checks
wt_config.memory_guard_size(32 * (1 << 20));

// Copy-on-write init — CRITICAL for fast instantiation, do NOT disable
wt_config.memory_init_cow(true);
```

On Linux, monitor `vm.max_map_count` (default 65530) — increase to 262144+ for many instances.

Ref: [Config API](https://docs.rs/wasmtime/latest/wasmtime/struct.Config.html)

## 6. Thread/Concurrency

```rust
// Call once per OS thread before first WASM execution (~200μs savings)
Engine::tls_eager_initialize();
```

The canonical pattern (which wruntime already follows):

```
Engine (1 per process, Send+Sync, clone is Arc bump)
  → Component (1 per module, Send+Sync)
    → ProxyPre (1 per module, amortizes import resolution)
      → Store (per-request, !Sync)
```

Ref: [Engine API](https://docs.rs/wasmtime/latest/wasmtime/struct.Engine.html)

## 7. Anti-Patterns Summary

| Pitfall | Fix |
|---------|-----|
| Compiling at startup | Pre-compile + `deserialize_file` |
| No epoch/fuel | Infinite loops pin tokio threads |
| `total_memories` = `total_instances` | Multiply by core modules per component (~3x) |
| Debug-mode guests | Always `--release` + `wasm-tools strip` |
| No `StoreLimits` | Runaway module exhausts pool |
| Disabling `memory_init_cow` | 400x slower instantiation |
| Ignoring `vm.max_map_count` | Pool creation fails on Linux |

## Priority Gaps for wruntime

1. **Epoch interruption** — highest priority, CPU-bound loops will deadlock tokio
2. **StoreLimits** — no per-store memory growth caps currently
3. **Pre-compilation** — components recompile every restart
4. **`total_memories`/`total_tables` multiplier** — likely too low for composed components
5. **`tls_eager_initialize`** — easy latency win
6. **MPK** — free memory density on Linux x86_64
