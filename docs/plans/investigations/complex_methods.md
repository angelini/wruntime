# Investigation: Complex / Long Methods

## Context

Follow-up to the complexity reduction investigation. Focuses exclusively on long or complex methods that could be broken into smaller, testable sub-functions.

---

## Findings

### 1. `bundle()` — 277 lines

**File:** `wr-cli/src/cmd/node.rs:232-508`

Largest function in the codebase. Handles 5 distinct phases in sequence:
1. Config validation + parsing (232-246)
2. Optional build step (254-272)
3. Tar assembly — binaries, configs, modules, migrations (274-395)
4. Systemd + Docker artifact generation (406-466)
5. Manifest + summary output (468-507)

**Suggested splits:**
- `add_binaries_to_tar()` — lines 287-298
- `add_engine_artifacts()` — lines 315-395 (the nested loop over engine configs + modules)
- `add_deployment_artifacts()` — lines 406-466 (systemd + docker generation)
- Keep manifest + summary inline (small)

**Impact:** The tar assembly loop (lines 315-395) is the most complex part — nested loop with `seen_modules` dedup, 3 artifact types (wasm, schema, migrations), config rewriting. Extracting it would make the function readable as a linear pipeline.

---

### 2. `spawn_module()` — 147 lines

**File:** `wr-engine/src/engine.rs:234-380`

Sets up a WASM module with all its host bindings. Three distinct phases:
1. Linker setup — 5 repetitive `add_to_linker` calls (252-270)
2. Service resolution — conditional db/blobstore/llm (273-295)
3. Context + handler construction + optional worker pool (297-376)

**Suggested splits:**
- `configure_linker()` — lines 252-270, returns `Linker<ModuleState>`
- `resolve_module_services()` — lines 273-295, returns `(Option<Arc<Pool>>, Option<Arc<str>>, Option<Arc<BlobstoreRuntime>>, Option<Arc<str>>, Option<Arc<LlmRuntime>>)` or a struct
- Leave the rest inline — it's the straightforward assembly of those pieces

**Impact:** Medium. The linker setup is boilerplate-heavy but not complex. The service resolution has conditional logic that would be easier to test in isolation.

---

### 3. `worker_loop()` — 111 lines

**File:** `wr-engine/src/worker.rs:373-483`

Event loop that claims and dispatches jobs. Has 3 nesting levels: outer poll loop → inner drain loop → response match.

**Suggested splits:**
- `dispatch_job()` — lines 418-480 (build request, send to module, handle response). This is a self-contained unit: takes a claimed job, dispatches it, updates status.
- Keep the claim loop and poll/notify logic in `worker_loop()`

**Impact:** High. `dispatch_job()` would be ~60 lines with a clear contract (job in, status update out), and the outer loop becomes trivial. Also eliminates the `#[allow(clippy::too_many_arguments)]` since `dispatch_job` could take a struct.

---

### 4. `up()` — 106 lines

**File:** `wr-cli/src/cmd/dev.rs:142-247`

Starts manager + proxy processes, checking if they're already running. The manager and proxy startup blocks (lines 172-202 and 204-231) are nearly identical: resolve binary, spawn, wait for port, add PID entry.

**Suggested splits:**
- `start_or_reuse_service()` — extracts the common pattern of checking alive, spawning, waiting, creating PID entry. Would take role name, binary name, config path, existing entries.

**Impact:** Medium. Removes ~40 lines of near-duplication between manager/proxy startup.

---

### 5. `register_engine()` — 87 lines

**File:** `wr-manager/src/service.rs:36-122`

Three phases: validation (40-63), DB persist (68), secret resolution (71-115).

**Suggested split:**
- `resolve_secrets()` — lines 71-115. Takes the secret requests and returns `Vec<NamespaceSecrets>`. This is a self-contained operation: fetch encrypted → check missing → decrypt → group by namespace.

**Impact:** Medium. The secret resolution block is the most complex part (HashSet intersection for missing check, decrypt loop, HashMap grouping). Extracting it makes `register_engine` a clean 3-step pipeline.

---

### 6. `provision_schemas()` — 78 lines

**File:** `wr-engine/src/engine.rs:117-194`

**Suggested split:**
- `extract_guest_role()` — lines 127-141. Manual URL parsing to extract username. Short (15 lines) but worth extracting because it's independently testable and the current chain of `split("://").nth(1)?.split('@').next()?.split(':').next()?` is easy to get wrong.

**Impact:** Low but high testability gain. The URL parsing has no tests currently.

---

### 7. `metrics::summary()` — 84 lines

**File:** `wr-cli/src/cmd/metrics.rs:133-216`

Linear flow: query → parse → group → display. Already clean enough — the grouping/aggregation (lines 172-212) is straightforward map-reduce. No strong case for extraction.

**Impact:** Low. Skip.

---

### 8. `update_rule_health_from_heartbeats()` — 51 lines

**File:** `wr-manager/src/db.rs:174-224`

Two symmetric queries (stale + recovered) followed by a conditional version bump. Already clean — the two queries differ only in the WHERE clause direction. Extracting would add indirection without reducing complexity.

**Impact:** Low. Skip.

---

## Recommended Actions (Prioritized)

### High Impact

1. **Split `bundle()` in `wr-cli/src/cmd/node.rs`** — extract `add_engine_artifacts()` for the nested engine/module tar loop, `add_deployment_artifacts()` for systemd/docker
2. **Extract `dispatch_job()` from `worker_loop()` in `wr-engine/src/worker.rs`** — clean separation of job dispatch from the poll/claim loop

### Medium Impact

3. **Extract `resolve_secrets()` from `register_engine()` in `wr-manager/src/service.rs`** — isolate the decrypt/group logic
4. **Extract `configure_linker()` and `resolve_module_services()` from `spawn_module()` in `wr-engine/src/engine.rs`** — reduce method size, improve readability
5. **Extract `start_or_reuse_service()` from `up()` in `wr-cli/src/cmd/dev.rs`** — deduplicate manager/proxy startup

### Low Impact (skip)

6. ~~`extract_guest_role()`~~ — small gain, low risk but not urgent
7. ~~`metrics::summary()`~~ — already clean linear flow
8. ~~`update_rule_health_from_heartbeats()`~~ — symmetric queries, clean as-is

---

## Verification

After each refactoring step:
```bash
just tidy
just test
```
