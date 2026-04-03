# Plan: Remove Run Mode

## Context

The `ModuleMode::Run` guest type provides a single persistent WASM instance that exports a `run` function. No examples actually use it — all examples use Service or Worker mode. With the Worker mode (job queue + retries + concurrency), Run mode is redundant. Removing it simplifies the codebase and the two remaining modes (Service, Worker) cleanly cover all use cases.

---

## Files to modify (code)

1. **`wr-engine/src/config.rs`** — Remove `Run` variant from `ModuleMode` enum (line 126). Update comment on `mode` field (line 189) to say "service (default) or worker".

2. **`wr-engine/src/engine.rs`** — Remove `ModuleMode::Run => { ... }` match arm (lines 252-289). The remaining `Service | Worker` arm becomes the only path.

3. **`wr-sdk/src/lib.rs`** — Remove `RunGuest` trait (lines 61-65) and `export_run!` macro (lines 138-158).

4. **`wr-sdk/wit/world.wit`** — Remove comment referencing "client-style modules (which export `run`)" (line 4).

5. **`wr-tests/tests/worker_test.rs`** — Remove `test_worker_mode_run_parsing` test (lines 95-112).

## Files to modify (docs)

6. **`README.md`** — Remove runner module example (lines ~117-128).
7. **`docs/sdk.md`** — Remove RunGuest references (lines 7, 154-167, 180, 182).
8. **`docs/agents/api_reference.md`** — Remove RunGuest trait and export_run (lines 77-79, 102).
9. **`docs/agents/module_template.md`** — Remove runner template (lines 122, 216-226).
10. **`docs/agents/decision_matrix.md`** — Remove runner row (line 10).
11. **`CLAUDE.md`** — Update any stale references.
12. **`plans/worker_mode.md`** — Remove Run references (lines 101-111, 270).

## Verification

- `just tidy` — no compile errors, no clippy warnings
- `just test` — all tests pass
